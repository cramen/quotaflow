package io.quotaflow.tck;

import static io.quotaflow.testing.TestIdentities.key;
import io.quotaflow.core.store.BucketIdentity;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Boundary burst: traffic concentrated around a period boundary must not show
 * the fixed-window 2x amplification pattern. Token bucket and GCRA allow at
 * most the capacity (plus refill accrued during the burst) in a burst that
 * hugs the moment the bucket is full again.
 *
 * <p>Timing is derived exclusively from the server's TIME command, never the
 * test machine's wall clock: every tolerance in this test is refill arithmetic
 * over a measured server-time span (plus one token for microsecond rounding),
 * so a scheduling stall on a loaded CI runner stretches the measured span
 * instead of breaking an assertion.
 */
class BoundaryBurstTckTest extends TckContainers {

    private static final long CAPACITY = 10;
    // 10 tokens per second: full refill in exactly one "period" of 1 s
    private static final Limit LIMIT = new Limit(CAPACITY, 10, Duration.ofSeconds(1));
    private static final long INTERVAL_MICROS = 100_000; // one token per 100 ms
    private static final long PERIOD_MICROS = 1_000_000;

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
        try (StatefulRedisConnection<String, String> serverTime = client.connect();
                RedisRateLimitStore store = io.quotaflow.testing.RecoveryStoreFixture.create(client, RedisStoreConfig.defaults())) {
            // phase 1: drain the bucket; a fresh bucket admits its capacity plus
            // at most the refill over the measured drain span
            long drainStart = serverTimeMicros(serverTime);
            long drained = burst(store, key, algorithm, (int) (3 * CAPACITY));
            long drainedAt = serverTimeMicros(serverTime);
            assertTrue(drained >= CAPACITY, "a fresh bucket admits its capacity, got " + drained);
            assertTrue(drained <= CAPACITY + refillBetween(drainStart, drainedAt),
                    "fresh bucket admitted " + drained + " beyond capacity + refill (" + algorithm + ')');

            // wait until the server clock says the bucket is full again: one
            // full period plus one emission interval (the GCRA tolerance band
            // after the last admissible request) after the drain ended
            awaitServerTime(serverTime, drainedAt + PERIOD_MICROS + INTERVAL_MICROS);

            // phase 2: concentrated burst right at the boundary — a fixed window
            // would allow a full second window here (2x capacity in total)
            long boundaryStart = serverTimeMicros(serverTime);
            long atBoundary = burst(store, key, algorithm, (int) (3 * CAPACITY));
            long boundaryEnd = serverTimeMicros(serverTime);
            assertTrue(atBoundary >= CAPACITY, "a refilled bucket admits its capacity, got " + atBoundary);
            assertTrue(atBoundary <= CAPACITY + refillBetween(boundaryStart, boundaryEnd),
                    "boundary burst amplified to " + atBoundary + " with capacity " + CAPACITY
                            + " (" + algorithm + ')');

            // phase 3: immediately after, only refill over the measured span
            // remains; phases 2+3 combined must stay within capacity + refill
            long after = burst(store, key, algorithm, (int) (3 * CAPACITY));
            long afterEnd = serverTimeMicros(serverTime);
            assertTrue(atBoundary + after <= CAPACITY + refillBetween(boundaryStart, afterEnd),
                    "allowed " + (atBoundary + after) + " across the boundary window with capacity "
                            + CAPACITY + " (" + algorithm + ')');
        }
    }

    /**
     * Tokens refill arithmetic can produce over the measured server-time span,
     * plus one to absorb microsecond rounding of the TIME reply.
     */
    private static long refillBetween(long startMicros, long endMicros) {
        return Math.max(0, endMicros - startMicros) / INTERVAL_MICROS + 1;
    }

    private static void awaitServerTime(
            StatefulRedisConnection<String, String> serverTime, long targetMicros) throws Exception {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (serverTimeMicros(serverTime) < targetMicros) {
            if (System.nanoTime() > deadlineNanos) {
                fail("server time did not reach " + targetMicros + " within 15 s");
            }
            Thread.sleep(25);
        }
    }

    private static long serverTimeMicros(StatefulRedisConnection<String, String> connection) {
        List<String> time = connection.sync().time();
        return Long.parseLong(time.get(0)) * 1_000_000 + Long.parseLong(time.get(1));
    }

    private static long burst(RedisRateLimitStore store, String key, Algorithm algorithm, int attempts) {
        long allowed = 0;
        for (int i = 0; i < attempts; i++) {
            if (store.tryAcquire(key(key), LIMIT, algorithm, 1).acquired()) {
                allowed++;
            }
        }
        return allowed;
    }
}
