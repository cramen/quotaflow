package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.RedisClient;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.store.StoreResult;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Weighted acquisition against a real server: N-token requests consume exactly
 * N tokens and are rejected, consuming nothing, when fewer than N remain.
 */
class WeightedAcquisitionTckTest extends TckContainers {

    @Test
    void weightedAcquisitionOnRedis() throws Exception {
        weighted(redisUri(), Algorithm.TOKEN_BUCKET);
        weighted(redisUri(), Algorithm.GCRA);
    }

    @Test
    void weightedAcquisitionOnValkey() throws Exception {
        weighted(valkeyUri(), Algorithm.TOKEN_BUCKET);
        weighted(valkeyUri(), Algorithm.GCRA);
    }

    private void weighted(String uri, Algorithm algorithm) throws Exception {
        String key = uniqueKey("weighted:global:" + algorithm.name().toLowerCase());
        Limit limit = new Limit(10, 1, Duration.ofSeconds(1));
        RedisClient client = newClient(uri);
        try (RedisRateLimitStore store = RedisRateLimitStore.create(client, RedisStoreConfig.defaults())) {
            StoreResult first = store.tryAcquire(key, limit, algorithm, 6);
            assertTrue(first.acquired(), "weight 6 against capacity 10");
            assertEquals(4, first.remaining());

            StoreResult tooMuch = store.tryAcquire(key, limit, algorithm, 5);
            assertFalse(tooMuch.acquired(), "weight 5 exceeds the remaining 4");
            assertTrue(tooMuch.retryAfterMillis() > 0, "rejection carries a retry-after");

            StoreResult fits = store.tryAcquire(key, limit, algorithm, 4);
            assertTrue(fits.acquired(), "weight 4 exactly fits");
            assertEquals(0, fits.remaining());

            assertFalse(store.tryAcquire(key, limit, algorithm, 1).acquired(), "bucket is drained");

            Thread.sleep(1100);
            assertTrue(store.tryAcquire(key, limit, algorithm, 1).acquired(),
                    "one token refills after one emission interval");
        }
    }
}
