package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Batch chain evaluation on the local store: all-or-nothing deduction,
 * result mapping identical to the distributed chain script, and snapshots of
 * active buckets for state replay.
 */
class LocalRateLimitStoreBatchTest {

    private static final long MILLI = 1_000_000L;

    /** Capacity 5, one token per millisecond. */
    private static final Limit LIMIT = new Limit(5, 1, Duration.ofMillis(1));

    private final AtomicLong nanos = new AtomicLong();
    private final LocalRateLimitStore store = new LocalRateLimitStore(nanos::get);

    private static LevelRequest level(String key, Limit limit, Algorithm algorithm, long weight) {
        return new LevelRequest(key, limit, algorithm, weight);
    }

    private static ChainResult join(LocalRateLimitStore store, List<LevelRequest> chain) {
        return store.tryAcquireAll(chain).toCompletableFuture().join();
    }

    @Test
    void allowedChainDeductsEveryLevelAndReportsLeafAndMinimumRemaining() {
        Limit parentLimit = new Limit(10, 1, Duration.ofMillis(1));
        ChainResult result = join(store, List.of(
                level("p:global:g", parentLimit, Algorithm.TOKEN_BUCKET, 1),
                level("c:tenant:t", LIMIT, Algorithm.TOKEN_BUCKET, 1)));
        assertTrue(result.acquired());
        assertEquals(1, result.firedLevelIndex());
        // parent 9, child 4 -> minimum is the child level
        assertEquals(4, result.remaining());
        assertEquals(0, result.retryAfterMillis());
        // the chain already deducted one token at the parent: one more acquisition leaves 8 of 10
        StoreResult parent = store.tryAcquire("p:global:g", parentLimit, Algorithm.TOKEN_BUCKET, 1);
        assertEquals(8, parent.remaining());
    }

    @Test
    void childRejectionDeductsNothingAtParents() {
        Limit parentLimit = new Limit(10, 1, Duration.ofMillis(1));
        // drain the child bucket completely
        for (int i = 0; i < 5; i++) {
            assertTrue(store.tryAcquire("c:tenant:t", LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired());
        }
        ChainResult result = join(store, List.of(
                level("p:global:g", parentLimit, Algorithm.TOKEN_BUCKET, 1),
                level("c:tenant:t", LIMIT, Algorithm.TOKEN_BUCKET, 1)));
        assertFalse(result.acquired());
        assertEquals(1, result.firedLevelIndex());
        assertEquals(0, result.remaining());
        assertEquals(1, result.retryAfterMillis());
        // the parent must be untouched: a direct acquisition sees a full bucket minus one
        StoreResult parent = store.tryAcquire("p:global:g", parentLimit, Algorithm.TOKEN_BUCKET, 1);
        assertTrue(parent.acquired());
        assertEquals(9, parent.remaining());
    }

    @Test
    void childRejectionRollsBackParentExactlyForGcra() {
        Limit parentLimit = new Limit(10, 1, Duration.ofMillis(1));
        for (int i = 0; i < 5; i++) {
            assertTrue(store.tryAcquire("c:tenant:t", LIMIT, Algorithm.GCRA, 1).acquired());
        }
        ChainResult result = join(store, List.of(
                level("p:global:g", parentLimit, Algorithm.GCRA, 3),
                level("c:tenant:t", LIMIT, Algorithm.GCRA, 1)));
        assertFalse(result.acquired());
        assertEquals(1, result.firedLevelIndex());
        // the GCRA parent keeps its full burst after the rollback
        StoreResult parent = store.tryAcquire("p:global:g", parentLimit, Algorithm.GCRA, 10);
        assertTrue(parent.acquired());
        assertEquals(0, parent.remaining());
    }

    @Test
    void mixedAlgorithmChainsEvaluateAtomically() {
        ChainResult result = join(store, List.of(
                level("p:global:g", LIMIT, Algorithm.TOKEN_BUCKET, 2),
                level("c:tenant:t", LIMIT, Algorithm.GCRA, 1)));
        assertTrue(result.acquired());
        assertEquals(3, result.remaining());
    }

    @Test
    void weightedChainConsumptionUsesTheWeightAtEveryLevel() {
        ChainResult result = join(store, List.of(
                level("p:global:g", LIMIT, Algorithm.TOKEN_BUCKET, 4),
                level("c:tenant:t", LIMIT, Algorithm.TOKEN_BUCKET, 4)));
        assertTrue(result.acquired());
        assertEquals(1, result.remaining());
        ChainResult rejected = join(store, List.of(
                level("p:global:g", LIMIT, Algorithm.TOKEN_BUCKET, 2),
                level("c:tenant:t", LIMIT, Algorithm.TOKEN_BUCKET, 2)));
        assertFalse(rejected.acquired());
        assertEquals(0, rejected.firedLevelIndex());
    }

    @Test
    void chainValidation() {
        assertThrows(NullPointerException.class, () -> store.tryAcquireAll(null));
        assertThrows(IllegalArgumentException.class, () -> store.tryAcquireAll(List.of()));
    }

    @Test
    void concurrentChainsSharingAParentAccountExactly() throws Exception {
        int parentCapacity = 200;
        Limit parentLimit = new Limit(parentCapacity, 1, Duration.ofHours(1));
        Limit leafLimit = new Limit(parentCapacity, 1, Duration.ofHours(1));
        int threads = 8;
        int attemptsPerThread = 100;
        AtomicInteger allowed = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                String leafKey = "leaf:user:u" + t;
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int i = 0; i < attemptsPerThread; i++) {
                        ChainResult result = join(store, List.of(
                                level("root:global:g", parentLimit, Algorithm.TOKEN_BUCKET, 1),
                                level(leafKey, leafLimit, Algorithm.TOKEN_BUCKET, 1)));
                        if (result.acquired()) {
                            allowed.incrementAndGet();
                        }
                    }
                });
            }
            assertTrue(ready.await(30, TimeUnit.SECONDS), "workers failed to start");
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "race did not finish in time");
        } finally {
            pool.shutdownNow();
        }
        assertEquals(parentCapacity, allowed.get(),
                "rejected chains must leave no deductions behind, so the parent drains exactly");
    }

    @Test
    void snapshotIsEmptyWhileBucketsAreFull() {
        assertTrue(store.snapshot().isEmpty());
        store.tryAcquire("k", new Limit(5, 5, Duration.ofMillis(1)), Algorithm.TOKEN_BUCKET, 1);
        // capacity 5 refills 5 per millisecond: not yet full right after one deduction
        assertEquals(1, store.snapshot().size());
        nanos.addAndGet(MILLI);
        assertTrue(store.snapshot().isEmpty(), "a refilled bucket carries no information");
        assertEquals(0, store.cellCount(), "the snapshot sweep evicts information-free cells");
    }

    @Test
    void snapshotReportsActiveTokenBucketsWithRemaining() {
        store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 3);
        List<BucketState> snapshot = store.snapshot();
        assertEquals(1, snapshot.size());
        BucketState bucket = snapshot.get(0);
        assertEquals("k", bucket.storageKey());
        assertEquals(LIMIT, bucket.limit());
        assertEquals(Algorithm.TOKEN_BUCKET, bucket.algorithm());
        assertEquals(2, bucket.remaining());
    }

    @Test
    void snapshotReportsActiveGcraCellsAndSweepsDrainedOnes() {
        store.tryAcquire("k", LIMIT, Algorithm.GCRA, 3);
        List<BucketState> snapshot = store.snapshot();
        assertEquals(1, snapshot.size());
        BucketState bucket = snapshot.get(0);
        assertEquals(Algorithm.GCRA, bucket.algorithm());
        assertEquals(2, bucket.remaining());
        nanos.addAndGet(60_000 * MILLI);
        assertTrue(store.snapshot().isEmpty(), "a drained TAT carries no information");
        assertEquals(0, store.cellCount());
    }

    @Test
    void snapshotReflectsRefillPartially() {
        store.tryAcquire("k", LIMIT, Algorithm.TOKEN_BUCKET, 5);
        nanos.addAndGet(2 * MILLI);
        List<BucketState> snapshot = store.snapshot();
        assertEquals(1, snapshot.size());
        assertEquals(2, snapshot.get(0).remaining());
    }

    @Test
    void rollbackToleratesCellsReplacedByAnotherAlgorithm() {
        // pathological chain: the same key with both algorithms; the second
        // level replaces the cell, so the first level's rollback finds a cell
        // of the other kind and must leave it alone
        Limit one = new Limit(1, 1, Duration.ofHours(1));
        store.tryAcquire("c:tenant:t", one, Algorithm.TOKEN_BUCKET, 1); // drain the third level
        ChainResult result = join(store, List.of(
                level("k:p:x", LIMIT, Algorithm.TOKEN_BUCKET, 1),
                level("k:p:x", LIMIT, Algorithm.GCRA, 1),
                level("c:tenant:t", one, Algorithm.TOKEN_BUCKET, 1)));
        assertFalse(result.acquired());
        assertEquals(2, result.firedLevelIndex());

        store.tryAcquire("c:tenant:t", one, Algorithm.GCRA, 1); // drain again, other order
        ChainResult reversed = join(store, List.of(
                level("k:p:y", LIMIT, Algorithm.GCRA, 1),
                level("k:p:y", LIMIT, Algorithm.TOKEN_BUCKET, 1),
                level("c:tenant:t", one, Algorithm.GCRA, 1)));
        assertFalse(reversed.acquired());
        assertEquals(2, reversed.firedLevelIndex());
    }

    @Test
    void bucketStateValidation() {
        assertThrows(NullPointerException.class,
                () -> new BucketState(null, LIMIT, Algorithm.GCRA, 0));
        assertThrows(NullPointerException.class,
                () -> new BucketState("k", null, Algorithm.GCRA, 0));
        assertThrows(NullPointerException.class,
                () -> new BucketState("k", LIMIT, null, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new BucketState("k", LIMIT, Algorithm.GCRA, -1));
    }
}
