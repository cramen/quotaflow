package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class LocalRateLimitStoreTest {

    private static final long MILLI = 1_000_000L;

    private final AtomicLong nanos = new AtomicLong();
    private final LongSupplier clock = nanos::get;
    private final LocalRateLimitStore store = new LocalRateLimitStore(clock);

    /** Capacity 5, one token per millisecond. */
    private static final Limit LIMIT = new Limit(5, 1, Duration.ofMillis(1));

    @Test
    void tokenBucketAdmitsExactlyCapacityThenRejects() {
        for (long expectedRemaining = 4; expectedRemaining >= 0; expectedRemaining--) {
            StoreResult result = store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 1);
            assertTrue(result.acquired());
            assertEquals(expectedRemaining, result.remaining());
        }
        StoreResult rejected = store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 1);
        assertFalse(rejected.acquired());
        assertEquals(0, rejected.remaining());
        assertEquals(1, rejected.retryAfterMillis());
    }

    @Test
    void tokenBucketRefillsDeterministically() {
        drain("k", 5);
        nanos.addAndGet(3 * MILLI);
        StoreResult result = store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 3);
        assertTrue(result.acquired());
        assertEquals(0, result.remaining());
        assertFalse(store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired());
    }

    @Test
    void tokenBucketRefillIsCappedAtCapacity() {
        drain("k", 5);
        nanos.addAndGet(60_000 * MILLI);
        StoreResult result = store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 1);
        assertTrue(result.acquired());
        assertEquals(4, result.remaining());
    }

    @Test
    void rejectionConsumesNoTokens() {
        drain("k", 5);
        assertFalse(store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 2).acquired());
        nanos.addAndGet(MILLI);
        StoreResult result = store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 1);
        assertTrue(result.acquired());
        assertEquals(0, result.remaining());
    }

    @Test
    void retryAfterScalesWithMissingWeight() {
        drain("k", 5);
        StoreResult rejected = store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 3);
        assertEquals(3, rejected.retryAfterMillis());
    }

    @Test
    void weightAboveCapacityIsNeverAdmitted() {
        StoreResult rejected = store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 6);
        assertFalse(rejected.acquired());
        assertTrue(rejected.retryAfterMillis() > 0);
        nanos.addAndGet(60_000 * MILLI);
        assertFalse(store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 6).acquired());
    }

    @Test
    void weightedAcquisitionConsumesWeight() {
        StoreResult result = store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 4);
        assertTrue(result.acquired());
        assertEquals(1, result.remaining());
    }

    @Test
    void bucketsAreIndependentPerKey() {
        drain("a", 5);
        assertTrue(store.tryAcquire("b", LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired());
        assertEquals(2, store.cellCount());
    }

    @Test
    void gcraAdmitsExactlyCapacityThenRejects() {
        for (long expectedRemaining = 4; expectedRemaining >= 0; expectedRemaining--) {
            StoreResult result = store.tryAcquire("k", LIMIT, Algorithm.GCRA, 1);
            assertTrue(result.acquired());
            assertEquals(expectedRemaining, result.remaining());
        }
        StoreResult rejected = store.tryAcquire("k", LIMIT, Algorithm.GCRA, 1);
        assertFalse(rejected.acquired());
        assertEquals(0, rejected.remaining());
        assertEquals(1, rejected.retryAfterMillis());
    }

    @Test
    void gcraRefillsDeterministically() {
        drain("k", 5, Algorithm.GCRA);
        nanos.addAndGet(3 * MILLI);
        StoreResult result = store.tryAcquire("k", LIMIT, Algorithm.GCRA, 3);
        assertTrue(result.acquired());
        assertEquals(0, result.remaining());
    }

    @Test
    void gcraWeightAboveCapacityIsNeverAdmitted() {
        assertFalse(store.tryAcquire("k", LIMIT, Algorithm.GCRA, 6).acquired());
        nanos.addAndGet(60_000 * MILLI);
        assertFalse(store.tryAcquire("k", LIMIT, Algorithm.GCRA, 6).acquired());
    }

    @Test
    void algorithmsDoNotShareStateForSameKey() {
        drain("k", 5, Algorithm.TOKEN_BUCKET);
        StoreResult result = store.tryAcquire("k", LIMIT, Algorithm.GCRA, 1);
        assertTrue(result.acquired());
        assertEquals(4, result.remaining());
    }

    @Test
    void asyncAcquisitionMatchesSync() {
        StoreResult result = store.tryAcquireAsync("k", LIMIT, Algorithm.TOKEN_BUCKET, 2)
                .toCompletableFuture().join();
        assertTrue(result.acquired());
        assertEquals(3, result.remaining());
    }

    @Test
    void rejectsInvalidInput() {
        assertThrows(IllegalArgumentException.class,
                () -> store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 0));
        assertThrows(NullPointerException.class,
                () -> store.tryAcquire(null, LIMIT, Algorithm.TOKEN_BUCKET, 1));
        assertThrows(NullPointerException.class,
                () -> store.tryAcquire("k", null, Algorithm.TOKEN_BUCKET, 1));
        assertThrows(NullPointerException.class,
                () -> store.tryAcquire("k", LIMIT, null, 1));
    }

    @Test
    void storeResultFactoriesAndEquality() {
        assertEquals(new StoreResult(true, 3, 0), StoreResult.acquired(3));
        assertEquals(new StoreResult(false, 0, 7), StoreResult.rejected(0, 7));
        assertNotEquals(StoreResult.acquired(3), StoreResult.rejected(3, 1));
        assertEquals(StoreResult.acquired(3).hashCode(), new StoreResult(true, 3, 0).hashCode());
        assertTrue(StoreResult.acquired(3).toString().contains("true"));
    }

    private void drain(String key, long tokens) {
        drain(key, tokens, Algorithm.TOKEN_BUCKET);
    }

    private void drain(String key, long tokens, Algorithm algorithm) {
        for (long i = 0; i < tokens; i++) {
            assertTrue(store.tryAcquire(key, LIMIT, algorithm, 1).acquired());
        }
    }
}
