package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ColdGuardFenceTest {
    private static final QuotaDomain DOMAIN = new QuotaDomain("guard", "root");
    private static final BucketIdentity KEY = new BucketIdentity(DOMAIN, "root", Scope.USER, "u");
    private static final Limit LIMIT = new Limit(1, 1, Duration.ofSeconds(1));
    private static RecoveryPending pending(long generation) {
        return new RecoveryPending(DOMAIN, generation, new CompletableFuture<>());
    }
    @Test void coldGuardsAccumulateFractionsAndRecreationHasNoBurst() {
        for (Algorithm algorithm : Algorithm.values()) {
            AtomicLong now = new AtomicLong();
            LocalRateLimitStore local = new LocalRateLimitStore(now::get, 10, LocalRateLimitStore.InitialCredit.EMPTY);
            assertFalse(local.tryAcquire(KEY, LIMIT, algorithm, 1).acquired());
            for (int i = 0; i < 9; i++) {
                now.addAndGet(100_000_000);
                assertFalse(local.tryAcquire(KEY, LIMIT, algorithm, 1).acquired());
            }
            now.addAndGet(100_000_000);
            assertTrue(local.tryAcquire(KEY, LIMIT, algorithm, 1).acquired());
            now.addAndGet(1_000_000_000);
            assertTrue(local.snapshot().isEmpty());
            assertFalse(local.tryAcquire(KEY, LIMIT, algorithm, 1).acquired());
        }
    }
    @Test void oldFenceCannotResumeOrClearNewerDebt() {
        var local = new LocalRateLimitStore(() -> 0);
        local.tryAcquire(KEY, LIMIT, Algorithm.GCRA, 1);
        var first = pending(1); var second = pending(2);
        local.fence(first); local.fence(second);
        assertFalse(local.resume(first));
        assertFalse(local.clearFenced(first));
        assertSame(second, local.tryAcquire(KEY, LIMIT, Algorithm.GCRA, 1).recoveryPending());
        assertEquals(0, local.snapshotFenced(second).get(0).remaining());
        assertThrows(IllegalStateException.class, () -> local.snapshotFenced(first));
        assertTrue(local.resume(second));
        assertFalse(local.tryAcquire(KEY, LIMIT, Algorithm.GCRA, 1).acquired());
    }
    @Test void pausedPreparationCannotCrossFenceAndResumeAba() throws Exception {
        var prepared = new CountDownLatch(1); var resume = new CountDownLatch(1);
        var once = new AtomicBoolean();
        var local = new LocalRateLimitStore(() -> 0, 10, LocalRateLimitStore.InitialCredit.FULL, () -> {
            if (Thread.currentThread().getName().equals("guard-attempt") && once.compareAndSet(false, true)) {
                prepared.countDown();
                try { assertTrue(resume.await(5, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { throw new AssertionError(e); }
            }
        });
        local.registerPolicies(List.of(PolicyBinding.of(KEY, Algorithm.GCRA)));
        var worker = Executors.newSingleThreadExecutor(r -> new Thread(r, "guard-attempt"));
        try {
            Future<StoreResult> attempt = worker.submit(() -> local.tryAcquire(KEY, LIMIT, Algorithm.GCRA, 1));
            assertTrue(prepared.await(5, TimeUnit.SECONDS));
            var fence = pending(1); local.fence(fence);
            assertTrue(local.snapshotFenced(fence).isEmpty());
            assertTrue(local.clearFenced(fence));
            assertTrue(local.resume(fence));
            resume.countDown();
            assertNotNull(attempt.get(5, TimeUnit.SECONDS).recoveryPending());
            assertTrue(local.tryAcquire(KEY, LIMIT, Algorithm.GCRA, 1).acquired());
        } finally { resume.countDown(); worker.shutdownNow(); }
    }
    @Test void delayedFenceCannotReplaceANewerOrResumedGeneration() {
        var local = new LocalRateLimitStore(() -> 0, 10, LocalRateLimitStore.InitialCredit.EMPTY);
        local.registerPolicies(List.of(PolicyBinding.of(KEY, Algorithm.GCRA)));
        var older = pending(1); var newer = pending(2);
        local.fence(newer); local.fence(older);
        assertSame(newer, local.tryAcquire(KEY, LIMIT, Algorithm.GCRA, 1).recoveryPending());
        assertTrue(local.resume(newer));
        local.fence(older); local.fence(newer);
        assertNull(local.tryAcquire(KEY, LIMIT, Algorithm.GCRA, 1).recoveryPending(), "resumed generations cannot re-fence live admission");
        var latest = pending(3); local.fence(latest);
        assertSame(latest, local.tryAcquire(KEY, LIMIT, Algorithm.GCRA, 1).recoveryPending());
    }
}
