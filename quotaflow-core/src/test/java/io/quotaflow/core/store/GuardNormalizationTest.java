package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class GuardNormalizationTest {
    private static final QuotaDomain DOMAIN = new QuotaDomain("n", "p");
    private static BucketIdentity key(String raw) { return new BucketIdentity(DOMAIN, "p", Scope.USER, raw); }
    private static RecoveryPending fence(long generation) { return new RecoveryPending(DOMAIN, generation, new CompletableFuture<>()); }
    @ParameterizedTest @EnumSource(Algorithm.class)
    void untouchedColdCellsClampWholeCreditAndDisabledSharesRecreateEmpty(Algorithm algorithm) {
        var now = new AtomicLong(); var store = new LocalRateLimitStore(now::get, 10, LocalRateLimitStore.InitialCredit.EMPTY);
        assertEquals(LocalRateLimitStore.InitialCredit.EMPTY, store.initialCredit());
        var limit = new Limit(5, 5, Duration.ofSeconds(1)); var smaller = new Limit(2, 1, Duration.ofSeconds(1));
        assertTrue(store.inspect(key("absent")).isEmpty());
        assertFalse(store.tryAcquire(key("a"), limit, algorithm, 1).acquired());
        now.set(800_000_000); assertTrue(store.tryAcquire(key("a"), limit, algorithm, 1).acquired());
        var pending = fence(1); store.fence(pending); store.fence(pending);
        now.set(1_100_000_000);
        assertTrue(store.constrainFenced(pending, List.of(new GuardConstraint(key("a"), algorithm, smaller, true),
                new GuardConstraint(key("absent"), algorithm, smaller, true))));
        assertEquals(2, store.snapshotFenced(pending).get(0).remaining());
        assertEquals(2, store.snapshot().get(0).remaining(), "full fenced cells cannot disappear from a recovery snapshot");
        assertEquals(2, store.inspect(key("a")).orElseThrow().remaining());
        assertEquals(1, store.cellCount());
        assertTrue(store.constrainFenced(pending, List.of(new GuardConstraint(key("a"), algorithm, null, true))));
        assertTrue(store.snapshotFenced(pending).isEmpty());
        assertTrue(store.resume(pending));
        assertFalse(store.tryAcquire(key("a"), smaller, algorithm, 1).acquired());
    }
    @Test void matchingScheduleKeepsEarnedFractionsButBackwardTimeNeverCreatesCredit() {
        var now = new AtomicLong(); var store = new LocalRateLimitStore(now::get, 10, LocalRateLimitStore.InitialCredit.EMPTY);
        var limit = new Limit(5, 5, Duration.ofSeconds(1));
        store.tryAcquire(key("a"), limit, Algorithm.GCRA, 1);
        now.set(900_000_000); store.tryAcquire(key("a"), limit, Algorithm.GCRA, 1);
        var pending = fence(1); store.fence(pending);
        assertTrue(store.constrainFenced(pending, List.of(new GuardConstraint(key("a"), Algorithm.GCRA, limit, false))));
        assertEquals(3, store.snapshotFenced(pending).get(0).remaining());
        now.set(800_000_000);
        assertTrue(store.constrainFenced(pending, List.of(new GuardConstraint(key("a"), Algorithm.GCRA, limit, true))));
        store.resume(pending);
        assertTrue(store.tryAcquire(key("a"), limit, Algorithm.GCRA, 3).acquired());
        now.set(1_000_000_000);
        assertFalse(store.tryAcquire(key("a"), limit, Algorithm.GCRA, 1).acquired(), "future accounted timestamp is retained and old fractions discarded");
        now.set(1_100_000_000); assertTrue(store.tryAcquire(key("a"), limit, Algorithm.GCRA, 1).acquired());
    }
    @Test void missingOrSupersededFencesCannotNormalizeOrRetireState() {
        var store = new LocalRateLimitStore(() -> 0, 10, LocalRateLimitStore.InitialCredit.EMPTY);
        var first = fence(1); var second = fence(2);
        assertThrows(PolicyConfigurationException.class, () -> store.fence(first));
        assertFalse(store.resume(first)); assertFalse(store.clearFenced(first));
        assertFalse(store.constrainFenced(first, List.of()));
        assertThrows(IllegalStateException.class, () -> store.snapshotFenced(first));
        var limit = new Limit(1, 1, Duration.ofSeconds(1));
        store.tryAcquire(key("a"), limit, Algorithm.GCRA, 1); store.fence(first); store.fence(second);
        assertFalse(store.constrainFenced(first, List.of(new GuardConstraint(key("a"), Algorithm.GCRA, null, true))));
        assertEquals(1, store.cellCount());
        var constraint = new GuardConstraint(key("a"), Algorithm.GCRA, null, true);
        assertThrows(IllegalArgumentException.class, () -> store.constrainFenced(second, List.of(constraint, constraint)));
        var foreign = new BucketIdentity(new QuotaDomain("other", "p"), "p", Scope.USER, "a");
        assertThrows(IllegalArgumentException.class, () -> store.constrainFenced(second,
                List.of(new GuardConstraint(foreign, Algorithm.GCRA, null, true))));
        assertTrue(store.clearFenced(second)); assertTrue(store.snapshotFenced(second).isEmpty());
    }
    @Test void normalizationPreparedBeforeANewFenceCannotCommit() {
        var newer = fence(2); var changed = new AtomicBoolean(); var armed = new AtomicBoolean();
        var reference = new AtomicReference<LocalRateLimitStore>();
        var store = new LocalRateLimitStore(() -> 0, 10, LocalRateLimitStore.InitialCredit.EMPTY, () -> {
            if (armed.get() && changed.compareAndSet(false, true)) reference.get().fence(newer);
        }); reference.set(store);
        store.tryAcquire(key("a"), new Limit(1, 1, Duration.ofSeconds(1)), Algorithm.GCRA, 1);
        var old = fence(1); store.fence(old); armed.set(true);
        assertFalse(store.constrainFenced(old, List.of(new GuardConstraint(key("a"), Algorithm.GCRA, null, true))));
        assertEquals(1, store.snapshotFenced(newer).size());
    }
}
