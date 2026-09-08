package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.api.StatefulRedisConnection;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.store.BucketState;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.StoreResult;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/**
 * Verification of {@code seed.lua} against a real Redis: recovery seeding
 * adopts the local remaining when the store holds nothing, merges
 * conservatively (never increasing the remaining the store already holds),
 * and addresses the same keys the chain evaluation uses.
 */
class SeedScriptTest extends RedisContainerSupport {

    /** No refill during a test: one token per hour. */
    private static final Limit LIMIT = new Limit(10, 1, Duration.ofHours(1));

    private static BucketState bucket(String key, Algorithm algorithm, long remaining) {
        return new BucketState(key, LIMIT, algorithm, remaining);
    }

    @Test
    void seedingAdoptsLocalRemainingWhenStoreHoldsNothing() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("seed", "tenant", "absent-tb");
            store.seed(key, List.of(bucket(key, Algorithm.TOKEN_BUCKET, 3))).toCompletableFuture().join();
            StoreResult result = store.tryAcquire(key, LIMIT, Algorithm.TOKEN_BUCKET, 1);
            assertTrue(result.acquired());
            assertEquals(2, result.remaining(), "seeded with 3 of 10 remaining, one more acquired");
        }
    }

    @Test
    void tokenBucketMergeNeverIncreasesRemaining() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("seed", "tenant", "merge-tb");
            for (int i = 0; i < 4; i++) {
                assertTrue(store.tryAcquire(key, LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired());
            }
            // store holds 6; a local snapshot claiming 8 must not raise it
            store.seed(key, List.of(bucket(key, Algorithm.TOKEN_BUCKET, 8))).toCompletableFuture().join();
            StoreResult result = store.tryAcquire(key, LIMIT, Algorithm.TOKEN_BUCKET, 1);
            assertEquals(5, result.remaining(), "the lower stored remaining won the merge");
            // a local snapshot claiming less does lower it
            store.seed(key, List.of(bucket(key, Algorithm.TOKEN_BUCKET, 2))).toCompletableFuture().join();
            StoreResult lowered = store.tryAcquire(key, LIMIT, Algorithm.TOKEN_BUCKET, 1);
            assertEquals(1, lowered.remaining(), "the lower local remaining won the merge");
        }
    }

    @Test
    void gcraSeedingAdoptsAndMergesConservatively() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String fresh = storageKey("seed", "tenant", "absent-gcra");
            store.seed(fresh, List.of(bucket(fresh, Algorithm.GCRA, 3))).toCompletableFuture().join();
            StoreResult adopted = store.tryAcquire(fresh, LIMIT, Algorithm.GCRA, 1);
            assertTrue(adopted.acquired());
            assertEquals(2, adopted.remaining());

            String merged = storageKey("seed", "tenant", "merge-gcra");
            for (int i = 0; i < 4; i++) {
                assertTrue(store.tryAcquire(merged, LIMIT, Algorithm.GCRA, 1).acquired());
            }
            // store holds 6; local claims 8 -> stays 6
            store.seed(merged, List.of(bucket(merged, Algorithm.GCRA, 8))).toCompletableFuture().join();
            assertEquals(5, store.tryAcquire(merged, LIMIT, Algorithm.GCRA, 1).remaining());
            // local claims 1 -> drops to 1
            store.seed(merged, List.of(bucket(merged, Algorithm.GCRA, 1))).toCompletableFuture().join();
            assertEquals(0, store.tryAcquire(merged, LIMIT, Algorithm.GCRA, 1).remaining());
        }
    }

    @Test
    void seededStateIsWhatChainEvaluationReads() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String parentKey = storageKey("seedg", "global", "g");
            String leafKey = storageKey("seedu", "user", "u");
            List<LevelRequest> chain = List.of(
                    new LevelRequest(parentKey, LIMIT, Algorithm.TOKEN_BUCKET, 1),
                    new LevelRequest(leafKey, LIMIT, Algorithm.TOKEN_BUCKET, 1));
            // seed the parent as fully consumed under this chain's leaf identity
            store.seed(leafKey, List.of(bucket(parentKey, Algorithm.TOKEN_BUCKET, 0)))
                    .toCompletableFuture().join();
            ChainResult rejected = store.tryAcquireAll(chain).toCompletableFuture().join();
            assertFalse(rejected.acquired());
            assertEquals(0, rejected.firedLevelIndex(), "the seeded parent rejects the chain");
            assertEquals(0, rejected.remaining());
        }
    }

    @Test
    void seedingMultipleBucketsInOneFlush() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String leafKey = storageKey("seedu", "user", "multi");
            String parentKey = storageKey("seedt", "tenant", "multi");
            store.seed(leafKey, List.of(
                    bucket(parentKey, Algorithm.TOKEN_BUCKET, 7),
                    bucket(leafKey, Algorithm.TOKEN_BUCKET, 4))).toCompletableFuture().join();
            ChainResult result = store.tryAcquireAll(List.of(
                    new LevelRequest(parentKey, LIMIT, Algorithm.TOKEN_BUCKET, 1),
                    new LevelRequest(leafKey, LIMIT, Algorithm.TOKEN_BUCKET, 1)))
                    .toCompletableFuture().join();
            assertTrue(result.acquired());
            assertEquals(3, result.remaining(), "min(7-1, 4-1)");
        }
    }

    @Test
    void emptySeedCompletesWithoutStoreCalls() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            store.seed("any:global:g", List.of()).toCompletableFuture().join();
        }
    }

    @Test
    void seedingFailureSurfacesAsExceptionalCompletion() {
        StatefulRedisConnection<String, String> connection = client().connect();
        RedisRateLimitStore store = newStore(connection);
        connection.close();
        String key = storageKey("seed", "tenant", "closed");
        CompletionStage<Void> seeding =
                store.seed(key, List.of(bucket(key, Algorithm.TOKEN_BUCKET, 1)));
        assertThrows(CompletionException.class,
                () -> seeding.toCompletableFuture().join());
    }
}
