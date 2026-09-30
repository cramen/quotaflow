package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.testing.RecoveryPrimaryFixture;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class RecoveryReloadTest {
    @Test void unavailableTariffDefersSeedingAndUntouchedGuardUsesTheNewFullLimit() throws Exception {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limitRef("plan").build()));
        var snapshotKey = new LimitSnapshot.Key("plan", "global");
        var published = new AtomicReference<Optional<LimitSnapshot>>(Optional.of(new LimitSnapshot(1,
                Map.of(snapshotKey, new Limit(10, 1, Duration.ofSeconds(1))))));
        VersionedLimitResolver resolver = published::get;
        var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong();
        var cohort = new RecoveryCohort(List.of("a", "b"));
        try (var store = new FallbackRateLimitStore(primary, new RecoverySettings("default", "test", "a", cohort, 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1)), List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).limitResolver(resolver).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false;
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            time.set(Duration.ofSeconds(5).toNanos());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty(), 5).isAllowed());
            time.set(Duration.ofSeconds(10).toNanos());
            assertTrue(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            published.set(Optional.empty());
            store.configureRecovery(policies, "default", resolver).toCompletableFuture().join();
            primary.available = true;
            Thread.sleep(100);
            assertEquals(0, primary.seeds.get(), "an unavailable tariff cannot be replaced by stale limit metadata");
            assertNotEquals(DegradationState.CLOSED, store.state());
            var replacement = new Limit(2, 1, Duration.ofSeconds(1));
            published.set(Optional.of(new LimitSnapshot(2, Map.of(snapshotKey, replacement))));
            time.addAndGet(Duration.ofMillis(100).toNanos());
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertEquals(1, primary.lastSeed.size());
            assertEquals(replacement, primary.lastSeed.get(0).limit(), "seed fingerprint uses the full current policy");
            assertEquals(1, primary.lastSeed.get(0).remaining(), "untouched old balance is clamped to this owner's new share");
        }
    }
    @Test void removedPoliciesRetainDebtUntilReconciledAndReaddingCannotReviveIt() throws Exception {
        var root = RateLimitPolicy.builder("root").scope(Scope.GLOBAL).limit(new Limit(100, 1, Duration.ofSeconds(1))).build();
        var child = RateLimitPolicy.builder("child").scope(Scope.USER).parentId("root").limit(new Limit(10, 1, Duration.ofSeconds(1))).build();
        var policies = PolicySet.compile(List.of(root, child)); var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong();
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            var context = RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "user").build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false;
            flow.tryAcquire("child", context); flow.tryAcquire("child", context);
            time.set(Duration.ofSeconds(4).toNanos()); assertTrue(flow.tryAcquire("child", context).isAllowed());
            primary.available = true;
            flow.replacePolicySet(PolicySet.compile(List.of(root)));
            time.addAndGet(Duration.ofMillis(100).toNanos());
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            var retired = primary.lastSeed.stream().filter(bucket -> bucket.storageKey().policyId().equals("child")).findFirst().orElseThrow();
            assertEquals(child.limit().orElseThrow(), retired.limit()); assertEquals(3, retired.remaining());
            assertEquals(0, store.trackedBuckets());
            flow.replacePolicySet(policies);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false;
            assertFalse(flow.tryAcquire("child", context).isAllowed());
            assertFalse(flow.tryAcquire("child", context).isAllowed());
        }
    }
    @Test void newerSnapshotsAdoptLocallyDuringAnOutageWithoutBurstOrVersionRollback() throws Exception {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limitRef("plan").build()));
        var key = new LimitSnapshot.Key("plan", "global");
        var first = new LimitSnapshot(1, Map.of(key, new Limit(10, 10, Duration.ofSeconds(1))));
        var second = new LimitSnapshot(2, Map.of(key, new Limit(2, 1, Duration.ofSeconds(1))));
        var published = new AtomicReference<>(first); VersionedLimitResolver resolver = () -> Optional.of(published.get());
        var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong(); var cohort = new RecoveryCohort(List.of("a", "b"));
        try (var store = new FallbackRateLimitStore(primary, new RecoverySettings("default", "test", "a", cohort, 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1)), List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).limitResolver(resolver).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false; flow.tryAcquire("quota", RateLimitContext.empty()); flow.tryAcquire("quota", RateLimitContext.empty());
            time.set(500_000_000L); assertFalse(flow.tryAcquire("quota", RateLimitContext.empty(), 5).isAllowed());
            time.set(1_000_000_000L); assertTrue(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            published.set(second);
            if (!flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed())
                FallbackRateLimitStoreTest.await(() -> flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed(), "only the conserved clamped whole token was available");
            time.set(3_000_000_000L); assertTrue(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            assertEquals(0, primary.seeds.get(), "local target adoption does not pretend a remote write succeeded");
            published.set(new LimitSnapshot(3, first.limits()));
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            FallbackRateLimitStoreTest.await(() -> flow.tryAcquire("quota", RateLimitContext.empty()).retryAfter().isPresent());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty(), 5).isAllowed(), "returning to old parameters cannot revive old credit");
            published.set(first); time.set(Duration.ofSeconds(100).toNanos());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed(), "older provider revision cannot spend the current guard");
        }
    }

    @Test void newerProviderViewCannotResumeAReadyOwnerWithoutAnAuthoritativeOutcome() throws Exception {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limitRef("plan").build()));
        var key = new LimitSnapshot.Key("plan", "global");
        var published = new AtomicReference<>(new LimitSnapshot(1, Map.of(key, new Limit(10, 10, Duration.ofSeconds(1)))));
        VersionedLimitResolver resolver = () -> Optional.of(published.get()); var primary = new RecoveryPrimaryFixture(policies);
        var time = new AtomicLong(); var lost = new java.util.concurrent.CompletableFuture<RecoveryControlResult>();
        try (var store = new FallbackRateLimitStore(primary, new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofMillis(100)), List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).limitResolver(resolver).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false; flow.tryAcquire("quota", RateLimitContext.empty()); flow.tryAcquire("quota", RateLimitContext.empty());
            primary.delayedReady = lost; primary.onReady = () -> primary.available = false;
            primary.available = true; time.addAndGet(Duration.ofSeconds(1).toNanos());
            FallbackRateLimitStoreTest.await(() -> primary.readiness.get() == 1);
            primary.available = false;
            published.set(new LimitSnapshot(2, Map.of(key, new Limit(100, 100, Duration.ofSeconds(1)))));
            time.addAndGet(Duration.ofSeconds(100).toNanos());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            Thread.sleep(150);
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed(), "timeout or a larger tariff is not proof of a completed barrier");
            primary.available = true; time.addAndGet(Duration.ofSeconds(1).toNanos());
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertEquals(0, store.trackedBuckets()); lost.complete(primary.lastReady);
            assertEquals(DegradationState.CLOSED, store.state());
        }
    }
    @Test void changingOnlyProviderVersionCannotReopenAnAuthoritativelyRejectedPolicy() throws Exception {
        var oldPolicy = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limitRef("old-plan").build()));
        var newPolicy = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limitRef("new-plan").build()));
        var snapshot = new LimitSnapshot(1, Map.of(new LimitSnapshot.Key("old-plan", "global"), new Limit(10, 10, Duration.ofSeconds(1)),
                new LimitSnapshot.Key("new-plan", "global"), new Limit(2, 1, Duration.ofSeconds(1))));
        var published = new AtomicReference<>(snapshot); VersionedLimitResolver resolver = () -> Optional.of(published.get());
        var primary = new RecoveryPrimaryFixture(oldPolicy); var time = new AtomicLong(); var cohort = new RecoveryCohort(List.of("a", "b"));
        var domain = new QuotaDomain("default", "quota");
        try (var store = new FallbackRateLimitStore(primary, new RecoverySettings("default", "test", "a", cohort, 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1)), List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(oldPolicy, store).limitResolver(resolver).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.configure(primary.current(domain).context(), new RecoveryConfiguration(newPolicy.recoveryFingerprint("quota"), 1,
                    snapshot.revision(), snapshot.fingerprint())).toCompletableFuture().join();
            primary.rejectConfigurationProposals = true;
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.OPEN);
            primary.available = false; published.set(new LimitSnapshot(2, snapshot.limits()));
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            Thread.sleep(50);
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            time.set(Duration.ofSeconds(1).toNanos());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed(), "transport failure cannot reactivate the rejected policy target");
        }
    }
}
