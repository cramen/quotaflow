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
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED && store.trackedBuckets() == 0);
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

    @Test void newlyPublishedPolicyWaitsForItsCapturedRecoveryTargetInsteadOfThrowing() throws Exception {
        var old = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL)
                .limit(new Limit(10, 10, Duration.ofSeconds(1))).build()));
        var next = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL)
                .limit(new Limit(20, 20, Duration.ofSeconds(1))).build()));
        var primary = new RecoveryPrimaryFixture(old);
        var entered = new java.util.concurrent.CountDownLatch(1);
        var held = new AtomicBoolean();
        var reply = new java.util.concurrent.CompletableFuture<RecoveryControlResult>();
        var gated = (RecoveryPrimary) java.lang.reflect.Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(),
                new Class<?>[]{RecoveryPrimary.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("read") && held.get()) { entered.countDown(); return reply; }
                    try { return method.invoke(primary, arguments); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(2));
        try (var store = new FallbackRateLimitStore(gated, settings, List.of())) {
            DefaultQuotaFlow.builder(old, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            held.set(true);
            assertTrue(entered.await(2, java.util.concurrent.TimeUnit.SECONDS));
            store.configureRecovery(next, "default", null).toCompletableFuture().join();
            var key = new BucketIdentity(new QuotaDomain("default", "quota"), "quota", Scope.GLOBAL, "global");
            var request = new LevelRequest(key, next.policy("quota").limit().orElseThrow(), Algorithm.TOKEN_BUCKET, 1,
                    "global", next.recoveryFingerprint("quota"));
            var decision = store.tryAcquireAll(List.of(request)).toCompletableFuture().get(2, java.util.concurrent.TimeUnit.SECONDS);
            assertFalse(decision.acquired());
            assertNotNull(decision.recoveryPending());
            assertEquals(0, primary.acquisitions.get());
        } finally { reply.cancel(false); }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(Algorithm.class)
    void delayedSeedKeepsAdmissionsFencedAndRepeatedBarriersRetireAllTracking(Algorithm algorithm) throws Exception {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).algorithm(algorithm)
                .limit(new Limit(10, 10, Duration.ofSeconds(1))).build()));
        var primary = new RecoveryPrimaryFixture(policies); var clock = new AtomicLong();
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(2));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), clock::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            for (int cycle = 0; cycle < 3; cycle++) {
                primary.available = false;
                assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
                assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
                clock.addAndGet(Duration.ofSeconds(1).toNanos());
                assertTrue(flow.tryAcquire("quota", RateLimitContext.empty(), 5).isAllowed());
                var seed = new java.util.concurrent.CompletableFuture<Boolean>();
                primary.delayedSeed = seed; int previous = primary.seeds.get(); primary.available = true;
                FallbackRateLimitStoreTest.await(() -> primary.seeds.get() > previous);
                int dispatched = primary.acquisitions.get();
                for (int i = 0; i < 10; i++) assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
                assertEquals(dispatched, primary.acquisitions.get(), "a stalled seed cannot authorize a primary debit");
                assertNotEquals(DegradationState.CLOSED, store.state());
                assertEquals(1, store.trackedBuckets());
                assertEquals(5, primary.lastSeed.get(0).remaining());
                seed.complete(true); primary.delayedSeed = null;
                clock.addAndGet(Duration.ofSeconds(1).toNanos());
                FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED && store.trackedBuckets() == 0);
            }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(Algorithm.class)
    void losingProviderAvailabilityFencesAnExistingLocalLedgerUntilTheSnapshotReturns(Algorithm algorithm) throws Exception {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).algorithm(algorithm).limitRef("plan").build()));
        var full = new Limit(10, 10, Duration.ofSeconds(1));
        var snapshot = new LimitSnapshot(1, Map.of(new LimitSnapshot.Key("plan", "global"), full));
        var published = new AtomicReference<Optional<LimitSnapshot>>(Optional.of(snapshot));
        VersionedLimitResolver resolver = published::get;
        var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong();
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).limitResolver(resolver).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false;
            flow.tryAcquire("quota", RateLimitContext.empty()); flow.tryAcquire("quota", RateLimitContext.empty());
            time.set(Duration.ofSeconds(1).toNanos());
            assertTrue(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            var request = new LevelRequest(new BucketIdentity(new QuotaDomain("default", "quota"), "quota", Scope.GLOBAL, "global"),
                    full, algorithm, 1, "global", policies.recoveryFingerprint("quota"), snapshot.revision(), snapshot.fingerprint());
            published.set(Optional.empty()); time.addAndGet(Duration.ofMillis(100).toNanos());
            FallbackRateLimitStoreTest.await(() -> store.tryAcquireAll(List.of(request)).toCompletableFuture().join().recoveryPending() != null);
            time.addAndGet(Duration.ofMillis(100).toNanos());
            assertNotNull(store.tryAcquireAll(List.of(request)).toCompletableFuture().join().recoveryPending());
            assertEquals(0, primary.seeds.get());
            published.set(Optional.of(snapshot)); time.addAndGet(Duration.ofMillis(100).toNanos());
            FallbackRateLimitStoreTest.await(() -> store.tryAcquireAll(List.of(request)).toCompletableFuture().join().recoveryPending() == null);
            assertEquals(DegradationState.OPEN, store.state());
        }
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(Algorithm.class)
    void onlyIdleFullyRefilledEntriesCanBeReclaimedAndTheyReappearCold(Algorithm algorithm) throws Exception {
        var limit = new Limit(2, 2, Duration.ofSeconds(1));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.USER).algorithm(algorithm).limit(limit).build()));
        var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong();
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 1, 2,
                Duration.ofMillis(10), Duration.ofSeconds(1));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), time::get)) {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false;
            var key = new BucketIdentity(new QuotaDomain("default", "quota"), "quota", Scope.USER, "a");
            assertFalse(store.tryAcquire(key, limit, algorithm, 1).acquired());
            FallbackRateLimitStoreTest.await(() -> {
                var decision = store.tryAcquire(key, limit, algorithm, 1);
                assertFalse(decision.acquired());
                return decision.recoveryPending() == null;
            });
            assertEquals(1, store.trackedBuckets());
            time.set(Duration.ofMillis(500).toNanos()); assertTrue(store.tryAcquire(key, limit, algorithm, 1).acquired());
            time.set(Duration.ofMillis(2499).toNanos());
            Thread.sleep(40); assertEquals(1, store.trackedBuckets(), "debt history must retain the attempt grace period");
            time.set(Duration.ofMillis(2500).toNanos());
            FallbackRateLimitStoreTest.await(() -> store.trackedBuckets() == 0);
            // Removing the entry precedes release of the domain maintenance fence.
            FallbackRateLimitStoreTest.await(() -> {
                var decision = store.tryAcquire(key, limit, algorithm, 1);
                assertFalse(decision.acquired(), "expired observations must allocate empty guards");
                return decision.recoveryPending() == null;
            });
            assertEquals(1, store.trackedBuckets());
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(Algorithm.class)
    void missingTheNormalReplyStillRetiresItsLedgerBeforeTheNextEpoch(Algorithm algorithm) throws Exception {
        var limit = new Limit(10, 10, Duration.ofSeconds(100));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).algorithm(algorithm).limit(limit).build()));
        var fixture = new RecoveryPrimaryFixture(policies); var online = new AtomicBoolean(true); var time = new AtomicLong();
        var primary = (RecoveryPrimary) java.lang.reflect.Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (!online.get()) return java.util.concurrent.CompletableFuture.failedFuture(
                            new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED));
                    try { return method.invoke(fixture, args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var cohort = new RecoveryCohort(List.of("a", "b"));
        var settings = new RecoverySettings("default", "test", "a", cohort, 10, 10, Duration.ofMillis(10), Duration.ofMillis(200));
        var domain = new QuotaDomain("default", "quota");
        var lost = new java.util.concurrent.CompletableFuture<RecoveryControlResult>();
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            online.set(false); flow.tryAcquire("quota", RateLimitContext.empty()); flow.tryAcquire("quota", RateLimitContext.empty());
            time.addAndGet(Duration.ofSeconds(100).toNanos()); assertTrue(flow.tryAcquire("quota", RateLimitContext.empty(), 5).isAllowed());
            fixture.delayedReady = lost; fixture.onReady = () -> online.set(false); online.set(true);
            FallbackRateLimitStoreTest.await(() -> fixture.lastReady != null);
            var oldNormal = fixture.lastReady;
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            fixture.begin(oldNormal.context(), new RecoveryConfiguration(policies.recoveryFingerprint("quota"), 0)).toCompletableFuture().join();
            fixture.awaitOtherMembers = true; fixture.onReady = () -> {}; fixture.delayedReady = null;
            int before = fixture.joins.get();
            time.addAndGet(Duration.ofSeconds(100).toNanos()); online.set(true);
            FallbackRateLimitStoreTest.await(() -> { time.addAndGet(1_000_000); return fixture.joins.get() > before; });
            assertTrue(fixture.lastSeed.isEmpty(), "retired tracking must not be replayed into the new epoch");
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed(), "a new epoch starts with an empty guard");
            assertEquals(DegradationState.OPEN, store.state());
            lost.complete(oldNormal);
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed(), "old NORMAL cannot reopen a newer epoch");
            fixture.awaitOtherMembers = false;
            fixture.join(fixture.current(domain).context()).toCompletableFuture().join();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED && store.trackedBuckets() == 0);
            assertEquals(0, store.trackedBuckets());
        } finally { lost.cancel(false); }
    }

    @Test void rejectedProviderProposalCannotBecomeAnOfflineBypassAndANewerRevisionCanRecover() throws Exception {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limitRef("plan").build()));
        var key = new LimitSnapshot.Key("plan", "global");
        var published = new AtomicReference<>(new LimitSnapshot(1, Map.of(key, new Limit(10, 10, Duration.ofSeconds(1)))));
        VersionedLimitResolver resolver = () -> Optional.of(published.get());
        var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong();
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).limitResolver(resolver).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            int proposals = primary.configurations.get(); primary.rejectConfigurationProposals = true;
            published.set(new LimitSnapshot(2, Map.of(key, new Limit(20, 20, Duration.ofSeconds(1)))));
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            FallbackRateLimitStoreTest.await(() -> primary.configurations.get() > proposals);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.OPEN);
            Thread.sleep(40);
            int rejectedProposals = primary.configurations.get();
            primary.available = false; time.set(Duration.ofSeconds(100).toNanos());
            for (int i = 0; i < 10; i++) assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            assertEquals(rejectedProposals, primary.configurations.get(), "a rejected snapshot must not be reproposed without a new version");
            primary.available = true; primary.rejectConfigurationProposals = false;
            published.set(new LimitSnapshot(3, Map.of(key, new Limit(30, 30, Duration.ofSeconds(1)))));
            time.addAndGet(Duration.ofSeconds(1).toNanos());
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertTrue(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            assertEquals(3, primary.current(new QuotaDomain("default", "quota")).resolverFloor());
        }
    }

    @Test void authoritativeNewerProviderFloorFencesOldLocalSnapshotsEvenWhenTheTransportThenFails() throws Exception {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limitRef("plan").build()));
        var key = new LimitSnapshot.Key("plan", "global");
        var one = new LimitSnapshot(1, Map.of(key, new Limit(10, 10, Duration.ofSeconds(1))));
        var two = new LimitSnapshot(2, Map.of(key, new Limit(20, 20, Duration.ofSeconds(1))));
        var published = new AtomicReference<>(one); VersionedLimitResolver resolver = () -> Optional.of(published.get());
        var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong();
        var domain = new QuotaDomain("default", "quota");
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).limitResolver(resolver).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.configure(primary.current(domain).context(), new RecoveryConfiguration(policies.recoveryFingerprint("quota"),
                    1, two.revision(), two.fingerprint())).toCompletableFuture().join();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.OPEN);
            primary.available = false; time.set(Duration.ofSeconds(100).toNanos());
            for (int i = 0; i < 10; i++) assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            assertEquals(0, store.trackedBuckets());
            published.set(two); primary.available = true; time.addAndGet(Duration.ofSeconds(1).toNanos());
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertTrue(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
        }
    }
    @Test void aReusedProviderRevisionWithDifferentContentIsAPermanentCompatibilityError() throws Exception {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limitRef("plan").build()));
        var key = new LimitSnapshot.Key("plan", "global");
        var published = new AtomicReference<>(new LimitSnapshot(1, Map.of(key, new Limit(10, 10, Duration.ofSeconds(1)))));
        VersionedLimitResolver resolver = () -> Optional.of(published.get());
        var primary = new RecoveryPrimaryFixture(policies);
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of())) {
            DefaultQuotaFlow.builder(policies, store).limitResolver(resolver).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            published.set(new LimitSnapshot(1, Map.of(key, new Limit(20, 20, Duration.ofSeconds(1)))));
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.OPEN);
            var failure = assertThrows(java.util.concurrent.CompletionException.class, () -> store.tryAcquireAsync(
                    new BucketIdentity(new QuotaDomain("default", "quota"), "quota", Scope.GLOBAL, "global"),
                    new Limit(10, 10, Duration.ofSeconds(1)), Algorithm.TOKEN_BUCKET, 1).toCompletableFuture().join());
            assertInstanceOf(StateCompatibilityException.class, failure.getCause());
            assertEquals(0, primary.acquisitions.get()); assertEquals(0, store.trackedBuckets());
        }
    }

    @Test void transientProviderExceptionsQuiesceAdmissionAndCanRecoverWithoutRestart() throws Exception {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limitRef("plan").build()));
        var limit = new Limit(10, 10, Duration.ofSeconds(1));
        var snapshot = new LimitSnapshot(1, Map.of(new LimitSnapshot.Key("plan", "global"), limit));
        var unavailable = new AtomicBoolean(); var time = new AtomicLong();
        VersionedLimitResolver resolver = () -> {
            if (unavailable.get()) throw new IllegalStateException("provider temporarily unavailable");
            return Optional.of(snapshot);
        };
        var primary = new RecoveryPrimaryFixture(policies);
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1));
        var request = new LevelRequest(new BucketIdentity(new QuotaDomain("default", "quota"), "quota", Scope.GLOBAL, "global"),
                limit, Algorithm.TOKEN_BUCKET, 1, "global", policies.recoveryFingerprint("quota"), 1, snapshot.fingerprint());
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), time::get)) {
            DefaultQuotaFlow.builder(policies, store).limitResolver(resolver).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            unavailable.set(true);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.OPEN);
            assertNotNull(store.tryAcquireAll(List.of(request)).toCompletableFuture().join().recoveryPending());
            assertEquals(0, primary.acquisitions.get());
            unavailable.set(false); time.set(Duration.ofSeconds(1).toNanos());
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertTrue(store.tryAcquireAll(List.of(request)).toCompletableFuture().join().acquired());
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(longs = {4, 20})
    void validatedIdentityCanAdoptANewStaticLimitOfflineWithoutCreatingInitialCredit(long capacity) throws Exception {
        var old = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL)
                .limit(new Limit(10, 10, Duration.ofSeconds(100))).build()));
        var limit = new Limit(capacity, capacity, Duration.ofSeconds(100));
        var next = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limit(limit).build()));
        var fixture = new RecoveryPrimaryFixture(old); var hold = new AtomicBoolean();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var read = new java.util.concurrent.CompletableFuture<RecoveryControlResult>();
        var primary = (RecoveryPrimary) java.lang.reflect.Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("read") && hold.get()) { entered.countDown(); return read; }
                    try { return method.invoke(fixture, args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        var time = new AtomicLong(); var domain = new QuotaDomain("default", "quota");
        var settings = new RecoverySettings("default", "test", "a", new RecoveryCohort(List.of("a", "b")), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(old, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            hold.set(true); assertTrue(entered.await(2, java.util.concurrent.TimeUnit.SECONDS));
            fixture.available = false; flow.replacePolicySet(next);
            hold.set(false); read.complete(fixture.current(domain));
            FallbackRateLimitStoreTest.await(() -> flow.tryAcquire("quota", RateLimitContext.empty()).retryAfter().isPresent());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed(), "the new guard starts empty");
            time.set(Duration.ofSeconds(100).toNanos());
            assertTrue(flow.tryAcquire("quota", RateLimitContext.empty(), capacity / 2).isAllowed());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            assertTrue(flow.tryAcquire("quota", RateLimitContext.empty(), capacity / 2 + 1).retryAfter().isEmpty());
            fixture.available = true; time.addAndGet(Duration.ofMillis(100).toNanos());
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED && store.trackedBuckets() == 0);
            assertEquals(limit, fixture.lastSeed.get(0).limit()); assertEquals(0, fixture.lastSeed.get(0).remaining());
        } finally { read.cancel(false); }
    }
}
