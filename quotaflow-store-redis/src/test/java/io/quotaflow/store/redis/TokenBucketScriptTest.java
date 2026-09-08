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
 * Script-level verification of {@code token_bucket.lua} against a real Redis:
 * refill math from server time, boundary behavior at exact capacity, weighted
 * consumption, TTL equal to full-refill time, and idle-key self-eviction.
 */
class TokenBucketScriptTest extends RedisContainerSupport {

    @Test
    void freshBucketAdmitsExactlyCapacityThenRejects() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "alice");
            Limit limit = new Limit(5, 1, Duration.ofSeconds(1));
            for (int i = 0; i < 5; i++) {
                StoreResult result = store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1);
                assertTrue(result.acquired(), "request " + i + " should be allowed");
                assertEquals(4 - i, result.remaining());
            }
            StoreResult rejected = store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1);
            assertFalse(rejected.acquired());
            assertEquals(0, rejected.remaining());
            // one token takes one second to regenerate
            assertTrue(rejected.retryAfterMillis() >= 900 && rejected.retryAfterMillis() <= 1100,
                    "retryAfter was " + rejected.retryAfterMillis());
        }
    }

    @Test
    void bucketRefillsFromServerTime() throws InterruptedException {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "bob");
            Limit limit = new Limit(1, 2, Duration.ofSeconds(1)); // one token every 500 ms
            assertTrue(store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1).acquired());
            assertFalse(store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1).acquired());
            Thread.sleep(600);
            assertTrue(store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1).acquired());
        }
    }

    @Test
    void weightedConsumptionRejectsWhenInsufficient() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "carol");
            Limit limit = new Limit(10, 1, Duration.ofSeconds(1));
            StoreResult first = store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 6);
            assertTrue(first.acquired());
            assertEquals(4, first.remaining());
            assertFalse(store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 5).acquired());
            StoreResult fits = store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 4);
            assertTrue(fits.acquired());
            assertEquals(0, fits.remaining());
        }
    }

    @Test
    void keyCarriesTtlEqualToFullRefillTime() {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "dave");
            Limit limit = new Limit(5, 1, Duration.ofSeconds(1));
            store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1);
            String redisKey = RedisKeyScheme.defaults().singleKey(key);
            long pttl = connection.sync().pttl(redisKey);
            // one token missing at 1 token/s -> about 1000 ms until full
            assertTrue(pttl > 400 && pttl <= 1100, "pttl was " + pttl);
        }
    }

    @Test
    void idleStateSelfEvictsAndNextRequestStartsFromFullBucket() throws InterruptedException {
        try (StatefulRedisConnection<String, String> connection = client().connect();
                RedisRateLimitStore store = newStore(connection)) {
            String key = storageKey("p", "user", "erin");
            Limit limit = new Limit(2, 5, Duration.ofSeconds(1)); // full refill of 2 tokens: 400 ms
            store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1);
            String redisKey = RedisKeyScheme.defaults().singleKey(key);
            Thread.sleep(900);
            assertNull(connection.sync().get(redisKey), "idle state should have self-evicted");
            StoreResult result = store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1);
            assertTrue(result.acquired());
            assertEquals(1, result.remaining(), "bucket must restart full");
        }
    }
}
