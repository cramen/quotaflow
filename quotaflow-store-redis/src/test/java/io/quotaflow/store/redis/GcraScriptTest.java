package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.api.StatefulRedisConnection;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.store.StoreResult;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Script-level verification of {@code gcra.lua} against a real Redis: burst of
 * exactly capacity, spacing of admissions at the emission interval, weighted
 * rejection leaving no state, TTL equal to drain time, idle self-eviction.
 */
class GcraScriptTest extends RedisContainerSupport {

    @Test
    void freshCellAdmitsExactlyCapacityThenRejects() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "alice");
            Limit limit = new Limit(5, 1, Duration.ofSeconds(1));
            for (int i = 0; i < 5; i++) {
                StoreResult result = store.tryAcquire(key, limit, Algorithm.GCRA, 1);
                assertTrue(result.acquired(), "request " + i + " should be allowed");
                assertEquals(4 - i, result.remaining());
            }
            StoreResult rejected = store.tryAcquire(key, limit, Algorithm.GCRA, 1);
            assertFalse(rejected.acquired());
            assertEquals(0, rejected.remaining());
            // the next slot opens one emission interval (1 s) out
            assertTrue(rejected.retryAfterMillis() >= 900 && rejected.retryAfterMillis() <= 1100,
                    "retryAfter was " + rejected.retryAfterMillis());
        }
    }

    @Test
    void admissionsSpaceOutAtTheEmissionInterval() throws InterruptedException {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "bob");
            Limit limit = new Limit(1, 2, Duration.ofSeconds(1)); // one slot every 500 ms
            assertTrue(store.tryAcquire(key, limit, Algorithm.GCRA, 1).acquired());
            assertFalse(store.tryAcquire(key, limit, Algorithm.GCRA, 1).acquired());
            Thread.sleep(600);
            assertTrue(store.tryAcquire(key, limit, Algorithm.GCRA, 1).acquired());
        }
    }

    @Test
    void weightBeyondCapacityLeavesNoState() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "carol");
            Limit limit = new Limit(3, 1, Duration.ofSeconds(1));
            StoreResult rejected = store.tryAcquire(key, limit, Algorithm.GCRA, 4);
            assertFalse(rejected.acquired());
            String redisKey = RedisKeyScheme.defaults().singleKey(key);
            assertNull(connection.sync().get(redisKey), "a never-admitted key must hold no state");
        }
    }

    @Test
    void weightedConsumptionDrainsTat() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "dave");
            Limit limit = new Limit(10, 1, Duration.ofSeconds(1));
            StoreResult first = store.tryAcquire(key, limit, Algorithm.GCRA, 6);
            assertTrue(first.acquired());
            assertEquals(4, first.remaining());
            assertFalse(store.tryAcquire(key, limit, Algorithm.GCRA, 5).acquired());
            assertTrue(store.tryAcquire(key, limit, Algorithm.GCRA, 4).acquired());
        }
    }

    @Test
    void keyCarriesTtlEqualToDrainTime() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "erin");
            Limit limit = new Limit(5, 1, Duration.ofSeconds(1));
            store.tryAcquire(key, limit, Algorithm.GCRA, 1);
            String redisKey = RedisKeyScheme.defaults().singleKey(key);
            long pttl = connection.sync().pttl(redisKey);
            // TAT sits one emission interval (1 s) in the future
            assertTrue(pttl > 500 && pttl <= 1100, "pttl was " + pttl);
        }
    }

    @Test
    void idleStateSelfEvicts() throws InterruptedException {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "frank");
            Limit limit = new Limit(3, 10, Duration.ofSeconds(1)); // 100 ms per token, drain 300 ms
            store.tryAcquire(key, limit, Algorithm.GCRA, 1);
            String redisKey = RedisKeyScheme.defaults().singleKey(key);
            Thread.sleep(600);
            assertNull(connection.sync().get(redisKey), "drained state should have self-evicted");
        }
    }
}
