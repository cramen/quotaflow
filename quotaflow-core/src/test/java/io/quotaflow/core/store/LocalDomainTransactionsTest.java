package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class LocalDomainTransactionsTest {
    private static final QuotaDomain DOMAIN = new QuotaDomain("atomic", "root");
    private static final Limit ONE = new Limit(1, 1, Duration.ofSeconds(1));
    private static BucketIdentity key(String raw) { return new BucketIdentity(DOMAIN, "root", Scope.USER, raw); }

    private static final class Pause implements AutoCloseable {
        final AtomicLong now = new AtomicLong();
        final CountDownLatch prepared = new CountDownLatch(1), resume = new CountDownLatch(1);
        final AtomicBoolean once = new AtomicBoolean();
        final ExecutorService worker = Executors.newSingleThreadExecutor(r -> new Thread(r, "paused-transaction"));
        final LocalRateLimitStore store = new LocalRateLimitStore(now::get, 4096, () -> {
            if (Thread.currentThread().getName().equals("paused-transaction") && once.compareAndSet(false, true)) {
                prepared.countDown();
                try { assertTrue(resume.await(5, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
            }
        });
        void awaitPreparation() throws Exception { assertTrue(prepared.await(5, TimeUnit.SECONDS)); }
        @Override public void close() { resume.countDown(); worker.shutdownNow(); }
    }

    @Test void staleSnapshotCannotEraseNewConsumption() throws Exception {
        for (Algorithm algorithm : Algorithm.values()) try (Pause p = new Pause()) {
            assertTrue(p.store.tryAcquire(key("same"), ONE, algorithm, 1).acquired());
            p.now.set(1_000_000_000L);
            Future<List<BucketState>> snapshot = p.worker.submit(p.store::snapshot);
            p.awaitPreparation();
            assertTrue(p.store.tryAcquire(key("same"), ONE, algorithm, 1).acquired());
            p.resume.countDown();
            snapshot.get(5, TimeUnit.SECONDS);
            assertFalse(p.store.tryAcquire(key("same"), ONE, algorithm, 1).acquired());
            assertEquals(1, p.store.cellCount());
        }
    }

    @Test void emptyTreeAbaCannotCommitAnObsoleteTimestamp() throws Exception {
        for (Algorithm algorithm : Algorithm.values()) try (Pause p = new Pause()) {
            Future<StoreResult> pending = p.worker.submit(() -> p.store.tryAcquire(key("same"), ONE, algorithm, 1));
            p.awaitPreparation();
            assertTrue(p.store.tryAcquire(key("same"), ONE, algorithm, 1).acquired());
            p.now.set(1_000_000_000L);
            assertTrue(p.store.snapshot().isEmpty());
            assertEquals(0, p.store.cellCount());
            p.resume.countDown();
            assertTrue(pending.get(5, TimeUnit.SECONDS).acquired());
            assertFalse(p.store.tryAcquire(key("same"), ONE, algorithm, 1).acquired(),
                    "reusing an empty root must not publish the old time-zero preparation");
        }
    }

    @Test void stalledChainDoesNotExposePartialDebitOrBlockOtherDomains() throws Exception {
        for (Algorithm algorithm : Algorithm.values()) try (Pause p = new Pause()) {
            BucketIdentity child = new BucketIdentity(DOMAIN, "child", Scope.KEY, "child");
            Future<ChainResult> pending = p.worker.submit(() -> p.store.tryAcquireAll(List.of(
                    new LevelRequest(key("same"), ONE, algorithm, 1),
                    new LevelRequest(child, ONE, algorithm, 1))).toCompletableFuture().join());
            p.awaitPreparation();
            BucketIdentity independent = new BucketIdentity(new QuotaDomain("atomic", "other"), "other", Scope.USER, "same");
            assertTrue(p.store.tryAcquire(independent, ONE, algorithm, 1).acquired());
            assertTrue(p.store.tryAcquire(key("same"), ONE, algorithm, 1).acquired());
            assertFalse(p.store.snapshot().stream().anyMatch(bucket -> bucket.storageKey().equals(child)),
                    "uncommitted child is not visible in snapshots");
            p.resume.countDown();
            assertFalse(pending.get(5, TimeUnit.SECONDS).acquired());
            assertTrue(p.store.tryAcquire(child, ONE, algorithm, 1).acquired());
            assertFalse(p.store.tryAcquire(key("same"), ONE, algorithm, 1).acquired());
        }
    }

    @Test void parameterChangeDuringPreparedChainCannotRestoreCredit() throws Exception {
        for (Algorithm algorithm : Algorithm.values()) try (Pause p = new Pause()) {
            Limit old = new Limit(10, 1, Duration.ofSeconds(1));
            Limit updated = new Limit(20, 1, Duration.ofSeconds(1));
            p.store.tryAcquire(key("same"), old, algorithm, 2);
            Future<ChainResult> pending = p.worker.submit(() -> p.store.tryAcquireAll(List.of(
                    new LevelRequest(key("same"), old, algorithm, 8))).toCompletableFuture().join());
            p.awaitPreparation();
            assertTrue(p.store.tryAcquire(key("same"), updated, algorithm, 8).acquired());
            p.resume.countDown();
            assertFalse(pending.get(5, TimeUnit.SECONDS).acquired());
            assertFalse(p.store.tryAcquire(key("same"), updated, algorithm, 1).acquired());
        }
    }

    @Test void rejectedTrafficDoesNotAllocateFreshChildCells() {
        LocalRateLimitStore store = new LocalRateLimitStore(() -> 0);
        assertTrue(store.tryAcquire(key("parent"), ONE, Algorithm.GCRA, 1).acquired());
        for (int i = 0; i < 1000; i++) {
            BucketIdentity child = new BucketIdentity(DOMAIN, "child", Scope.KEY, "u-" + i);
            assertFalse(store.tryAcquireAll(List.of(new LevelRequest(key("parent"), ONE, Algorithm.GCRA, 1),
                    new LevelRequest(child, ONE, Algorithm.GCRA, 1))).toCompletableFuture().join().acquired());
        }
        assertEquals(1, store.cellCount());
    }

    @Test void persistentTreePreservesIdentitiesThroughVariedInsertionAndExpiryOrders() {
        for (int seed = 0; seed < 12; seed++) {
            AtomicLong clock = new AtomicLong();
            LocalRateLimitStore store = new LocalRateLimitStore(clock::get);
            List<String> keys = new ArrayList<>();
            // Include known String hash collisions and sorted/reversed insertion orders.
            keys.add("Aa"); keys.add("BB");
            for (int i = 0; i < 128; i++) keys.add(String.format("key-%03d", i));
            if (seed == 1) Collections.reverse(keys);
            else if (seed > 1) Collections.shuffle(keys, new Random(seed));
            Limit limit = new Limit(2, 1, Duration.ofSeconds(1));
            for (String raw : keys) assertEquals(1, store.tryAcquire(key(raw), limit, Algorithm.GCRA, 1).remaining());
            assertEquals(keys.size(), store.cellCount());
            clock.set(1_000_000_000L);
            Set<String> live = new HashSet<>();
            for (int i = 0; i < keys.size(); i += 3) {
                String raw = keys.get(i); live.add(raw);
                assertTrue(store.tryAcquire(key(raw), limit, Algorithm.GCRA, 2).acquired());
            }
            Set<String> actual = new HashSet<>();
            for (BucketState bucket : store.snapshot()) {
                actual.add(bucket.storageKey().rawKey()); assertEquals(0, bucket.remaining());
            }
            assertEquals(live, actual, "seed=" + seed);
            assertEquals(live.size(), store.cellCount());
            clock.set(3_000_000_000L);
            assertTrue(store.snapshot().isEmpty());
            assertEquals(0, store.cellCount());
            for (String raw : keys) assertTrue(store.tryAcquire(key(raw), limit, Algorithm.GCRA, 2).acquired());
        }
    }
}
