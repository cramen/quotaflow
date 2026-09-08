package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.api.StatefulRedisConnection;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Verification of {@code chain.lua} against a real Redis: whole-chain
 * all-or-nothing deduction, fired-level reporting, minimum remaining, and
 * mixed-algorithm chains.
 */
class ChainScriptTest extends RedisContainerSupport {

    private static final RedisKeyScheme KEYS = RedisKeyScheme.defaults();

    private static List<LevelRequest> chain(String run, long globalCap, long tenantCap, long userCap) {
        Limit global = new Limit(globalCap, 1, Duration.ofSeconds(10));
        Limit tenant = new Limit(tenantCap, 1, Duration.ofSeconds(10));
        Limit user = new Limit(userCap, 1, Duration.ofSeconds(10));
        String suffix = "-" + run;
        return List.of(
                new LevelRequest("g:global:global" + suffix, global, Algorithm.TOKEN_BUCKET, 1),
                new LevelRequest("t:tenant:acme" + suffix, tenant, Algorithm.TOKEN_BUCKET, 1),
                new LevelRequest("u:user:alice" + suffix, user, Algorithm.TOKEN_BUCKET, 1));
    }

    @Test
    void allowDeductsEveryLevelAndReportsChainMinimum() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            ChainResult result = store.tryAcquireAll(chain("allow", 10, 5, 2))
                    .toCompletableFuture().join();
            assertTrue(result.acquired());
            assertEquals(2, result.firedLevelIndex(), "leaf reports on allow");
            assertEquals(1, result.remaining(), "min(9, 4, 1)");
            assertEquals(0, result.retryAfterMillis());
        }
    }

    @Test
    void childRejectionConsumesNothingAtAnyParentLevel() {
        String run = UUID.randomUUID().toString();
        List<LevelRequest> chain = chain(run, 10, 5, 2);
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            assertTrue(store.tryAcquireAll(chain).toCompletableFuture().join().acquired());
            assertTrue(store.tryAcquireAll(chain).toCompletableFuture().join().acquired());

            List<String> redisKeys = KEYS.chainKeys(chain.stream().map(LevelRequest::storageKey).toList());
            String globalBefore = connection.sync().get(redisKeys.get(0));
            String tenantBefore = connection.sync().get(redisKeys.get(1));

            // user level (capacity 2) is exhausted -> rejection at the leaf
            ChainResult rejected = store.tryAcquireAll(chain).toCompletableFuture().join();
            assertFalse(rejected.acquired());
            assertEquals(2, rejected.firedLevelIndex());
            assertEquals(0, rejected.remaining());
            assertTrue(rejected.retryAfterMillis() > 0);

            // all-or-nothing: parent state is byte-identical, not merely close
            assertEquals(globalBefore, connection.sync().get(redisKeys.get(0)));
            assertEquals(tenantBefore, connection.sync().get(redisKeys.get(1)));
        }
    }

    @Test
    void parentRejectionLeavesDeeperLevelsUntouched() {
        String run = UUID.randomUUID().toString();
        List<LevelRequest> chain = chain(run, 1, 100, 100);
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            assertTrue(store.tryAcquireAll(chain).toCompletableFuture().join().acquired());
            List<String> redisKeys = KEYS.chainKeys(chain.stream().map(LevelRequest::storageKey).toList());
            String tenantBefore = connection.sync().get(redisKeys.get(1));
            String userBefore = connection.sync().get(redisKeys.get(2));

            ChainResult rejected = store.tryAcquireAll(chain).toCompletableFuture().join();
            assertFalse(rejected.acquired());
            assertEquals(0, rejected.firedLevelIndex(), "global level fires");
            assertEquals(0, rejected.remaining());

            // all-or-nothing: deeper levels keep byte-identical state
            assertEquals(tenantBefore, connection.sync().get(redisKeys.get(1)));
            assertEquals(userBefore, connection.sync().get(redisKeys.get(2)));
        }
    }

    @Test
    void mixedAlgorithmChainEvaluatesAtomically() {
        String run = UUID.randomUUID().toString();
        String suffix = "-" + run;
        List<LevelRequest> chain = List.of(
                new LevelRequest("g:global:global" + suffix,
                        new Limit(1, 1, Duration.ofSeconds(10)), Algorithm.GCRA, 1),
                new LevelRequest("u:user:alice" + suffix,
                        new Limit(3, 1, Duration.ofSeconds(10)), Algorithm.TOKEN_BUCKET, 2));
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            ChainResult first = store.tryAcquireAll(chain).toCompletableFuture().join();
            assertTrue(first.acquired());
            assertEquals(1, first.firedLevelIndex());
            assertEquals(0, first.remaining(), "min(gcra 0, tb 1)");
            // gcra parent (capacity 1) is exhausted; tb leaf must not consume its remaining token
            ChainResult rejected = store.tryAcquireAll(chain).toCompletableFuture().join();
            assertFalse(rejected.acquired());
            assertEquals(0, rejected.firedLevelIndex());
            String leafKey = KEYS.chainKeys(chain.stream().map(LevelRequest::storageKey).toList()).get(1);
            assertTrue(connection.sync().get(leafKey).startsWith("1:"),
                    "leaf bucket must still hold its one remaining token");
        }
    }

    @Test
    void retryAfterComesFromTheFiredLevelSchedule() {
        String run = UUID.randomUUID().toString();
        String suffix = "-" + run;
        List<LevelRequest> chain = List.of(
                new LevelRequest("g:global:global" + suffix,
                        new Limit(100, 1, Duration.ofSeconds(10)), Algorithm.TOKEN_BUCKET, 1),
                new LevelRequest("t:tenant:acme" + suffix,
                        new Limit(1, 1, Duration.ofSeconds(2)), Algorithm.TOKEN_BUCKET, 1));
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            assertTrue(store.tryAcquireAll(chain).toCompletableFuture().join().acquired());
            ChainResult rejected = store.tryAcquireAll(chain).toCompletableFuture().join();
            assertFalse(rejected.acquired());
            assertEquals(1, rejected.firedLevelIndex());
            assertTrue(rejected.retryAfterMillis() >= 1900 && rejected.retryAfterMillis() <= 2100,
                    "retryAfter was " + rejected.retryAfterMillis());
        }
    }
}
