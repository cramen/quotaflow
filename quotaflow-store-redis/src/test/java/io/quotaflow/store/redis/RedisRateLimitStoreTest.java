package io.quotaflow.store.redis;

import static io.quotaflow.testing.TestIdentities.key;
import io.quotaflow.core.store.BucketIdentity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.api.StatefulRedisConnection;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.StoreResult;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Store-level behavior: EVALSHA with transparent EVAL recovery after
 * SCRIPT FLUSH, sync/async agreement, and SPI argument validation.
 */
class RedisRateLimitStoreTest extends RedisContainerSupport {

    @Test
    void scriptFlushIsRecoveredTransparently() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "flush");
            Limit limit = new Limit(2, 1, Duration.ofSeconds(1));
            StoreResult first = store.tryAcquire(key(key), limit, Algorithm.TOKEN_BUCKET, 1);
            assertTrue(first.acquired());
            assertEquals(1, first.remaining());

            connection.sync().scriptFlush();

            // cold script cache: EVALSHA misses with NOSCRIPT, the store resubmits via EVAL
            StoreResult second = store.tryAcquire(key(key), limit, Algorithm.TOKEN_BUCKET, 1);
            assertTrue(second.acquired());
            assertEquals(0, second.remaining());
            StoreResult third = store.tryAcquire(key(key), limit, Algorithm.TOKEN_BUCKET, 1);
            assertFalse(third.acquired());
        }
    }

    @Test
    void scriptFlushBetweenChainCallsIsRecovered() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            List<LevelRequest> chain = List.of(
                    new LevelRequest(key("t:tenant:" + "acme-" + System.nanoTime()),
                            new Limit(1, 1, Duration.ofSeconds(10)), Algorithm.TOKEN_BUCKET, 1),
                    new LevelRequest(key("u:user:" + "alice-" + System.nanoTime()),
                            new Limit(5, 1, Duration.ofSeconds(10)), Algorithm.GCRA, 1));
            assertTrue(store.tryAcquireAll(chain).toCompletableFuture().join().acquired());
            connection.sync().scriptFlush();
            ChainResult rejected = store.tryAcquireAll(chain).toCompletableFuture().join();
            assertFalse(rejected.acquired());
            assertEquals(0, rejected.firedLevelIndex());
        }
    }

    @Test
    void asyncAndSyncPathsAgree() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "async");
            Limit limit = new Limit(1, 1, Duration.ofSeconds(1));
            StoreResult sync = store.tryAcquire(key(key), limit, Algorithm.GCRA, 1);
            assertTrue(sync.acquired());
            StoreResult async = store.tryAcquireAsync(key(key), limit, Algorithm.GCRA, 1)
                    .toCompletableFuture().join();
            assertFalse(async.acquired());
            assertTrue(async.retryAfterMillis() > 0);
        }
    }

    @Test
    void invalidArgumentsRejected() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            Limit limit = new Limit(1, 1, Duration.ofSeconds(1));
            assertThrows(IllegalArgumentException.class,
                    () -> store.tryAcquire(key("k"), limit, Algorithm.TOKEN_BUCKET, 0));
            assertThrows(NullPointerException.class,
                    () -> store.tryAcquire(null, limit, Algorithm.TOKEN_BUCKET, 1));
            assertThrows(NullPointerException.class,
                    () -> store.tryAcquire(key("k"), null, Algorithm.TOKEN_BUCKET, 1));
            assertThrows(NullPointerException.class,
                    () -> store.tryAcquire(key("k"), limit, null, 1));
            assertThrows(IllegalArgumentException.class, () -> store.tryAcquireAll(List.of()));
            assertThrows(NullPointerException.class, () -> store.tryAcquireAll(null));
        }
    }

    @Test
    void factoryOpensDedicatedConnectionClosedWithStore() {
        RedisRateLimitStore store = RedisRateLimitStore.create(client(), RedisStoreConfig.defaults());
        var identity = key(storageKey("p", "user", "factory"));
        try (var adminConnection = client().connect()) {
            io.quotaflow.testing.RecoveryStoreFixture.prepareFor(store, adminConnection,
                    List.of(io.quotaflow.core.store.PolicyBinding.of(identity, Algorithm.TOKEN_BUCKET)));
        }
        StoreResult result = store.tryAcquire(identity,
                new Limit(1, 1, Duration.ofSeconds(1)), Algorithm.TOKEN_BUCKET, 1);
        assertTrue(result.acquired());
        store.close();
    }

    @Test
    void constructorRejectsNulls() {
        try (StatefulRedisConnection<String, String> connection = client().connect()) {
            assertThrows(NullPointerException.class,
                    () -> new RedisRateLimitStore((StatefulRedisConnection<String, String>) null, RedisStoreConfig.defaults()));
            assertThrows(NullPointerException.class, () -> new RedisRateLimitStore(connection, null));
            assertThrows(NullPointerException.class,
                    () -> new RedisRateLimitStore(connection, RedisStoreConfig.defaults(), null));
        }
    }
}
