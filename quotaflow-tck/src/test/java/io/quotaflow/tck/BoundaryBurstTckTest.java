package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.RedisClient;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Boundary burst: traffic concentrated around a period boundary must not show
 * the fixed-window 2x amplification pattern. Token bucket and GCRA allow at
 * most the capacity (plus refill accrued during the burst) in a burst that
 * hugs the moment the bucket is full again.
 */
class BoundaryBurstTckTest extends TckContainers {

    private static final long CAPACITY = 10;
    // 10 tokens per second: full refill in exactly one "period" of 1 s
    private static final Limit LIMIT = new Limit(CAPACITY, 10, Duration.ofSeconds(1));
    // slack for refill accrued while the burst itself is in flight
    private static final long SLACK = 2;

    @Test
    void tokenBucketHasNoBoundaryBurstOnRedis() throws Exception {
        assertNoBoundaryBurst(redisUri(), Algorithm.TOKEN_BUCKET);
    }

    @Test
    void tokenBucketHasNoBoundaryBurstOnValkey() throws Exception {
        assertNoBoundaryBurst(valkeyUri(), Algorithm.TOKEN_BUCKET);
    }

    @Test
    void gcraHasNoBoundaryBurstOnRedis() throws Exception {
        assertNoBoundaryBurst(redisUri(), Algorithm.GCRA);
    }

    @Test
    void gcraHasNoBoundaryBurstOnValkey() throws Exception {
        assertNoBoundaryBurst(valkeyUri(), Algorithm.GCRA);
    }

    private void assertNoBoundaryBurst(String uri, Algorithm algorithm) throws Exception {
        String key = uniqueKey("burst:global:" + algorithm.name().toLowerCase());
        RedisClient client = newClient(uri);
        try (RedisRateLimitStore store = RedisRateLimitStore.create(client, RedisStoreConfig.defaults())) {
            // phase 1: drain the bucket
            long drained = burst(store, key, algorithm, (int) (3 * CAPACITY));
            assertEquals(CAPACITY, drained, "a fresh bucket admits exactly its capacity");

            // wait one full period: the bucket is full again at the "boundary"
            Thread.sleep(1050);

            // phase 2: concentrated burst right at the boundary — a fixed window
            // would allow a full second window here (2x capacity in total)
            long atBoundary = burst(store, key, algorithm, (int) (3 * CAPACITY));
            assertTrue(atBoundary >= CAPACITY, "a refilled bucket admits its capacity, got " + atBoundary);
            assertTrue(atBoundary <= CAPACITY + SLACK,
                    "boundary burst amplified to " + atBoundary + " with capacity " + CAPACITY
                            + " (" + algorithm + ')');

            // phase 3: immediately after, the budget is gone
            long after = burst(store, key, algorithm, (int) (3 * CAPACITY));
            assertTrue(after <= SLACK,
                    "allowed " + after + " right after the boundary burst (" + algorithm + ')');
        }
    }

    private static long burst(RedisRateLimitStore store, String key, Algorithm algorithm, int attempts) {
        long allowed = 0;
        for (int i = 0; i < attempts; i++) {
            if (store.tryAcquire(key, LIMIT, algorithm, 1).acquired()) {
                allowed++;
            }
        }
        return allowed;
    }
}
