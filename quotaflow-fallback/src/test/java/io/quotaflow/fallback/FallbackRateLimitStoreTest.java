package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.testing.RecoveryPrimaryFixture;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class FallbackRateLimitStoreTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    private static PolicySet policies(Algorithm algorithm, long capacity, long refill) {
        return PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL)
                .algorithm(algorithm).limit(new Limit(capacity, refill, Duration.ofSeconds(1))).build()));
    }
    private static RecoverySettings settings(int instances, int cap) {
        List<String> members = java.util.stream.IntStream.range(0, instances).mapToObj(Integer::toString).toList();
        return new RecoverySettings("default", "test", "0", new RecoveryCohort(members), cap, 2,
                Duration.ofMillis(10), TIMEOUT);
    }
    static void await(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(5);
        }
        assertTrue(condition.getAsBoolean(), "recovery condition did not become true");
    }
    @Test void oldStoreAndNoOpSeederCannotAuthorizeRecovery() {
        assertThrows(IllegalArgumentException.class, () -> new FallbackRateLimitStore(new LocalRateLimitStore(),
                StateSeeder.noOp(), FallbackConfig.defaults(), List.of()));
    }
    @ParameterizedTest @EnumSource(Algorithm.class)
    void coldSharesDivideBothBurstAndRefillAndNewOutagesRetireOldGuards(Algorithm algorithm) throws Exception {
        var policies = policies(algorithm, 100, 100);
        var primary = new RecoveryPrimaryFixture(policies);
        var time = new AtomicLong();
        try (var store = new FallbackRateLimitStore(primary, settings(4, 10), List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false;
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            time.set(500_000_000L);
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty(), 25).isAllowed());
            time.set(TimeUnit.SECONDS.toNanos(1));
            assertTrue(flow.tryAcquire("quota", RateLimitContext.empty(), 25).isAllowed());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            time.set(1_500_000_000L);
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty(), 25).isAllowed());
            time.set(TimeUnit.SECONDS.toNanos(2));
            assertTrue(flow.tryAcquire("quota", RateLimitContext.empty(), 10).isAllowed());
            primary.available = true;
            time.addAndGet(TimeUnit.SECONDS.toNanos(1));
            await(() -> store.state() == DegradationState.CLOSED);
            assertEquals(0, store.trackedBuckets());
            primary.available = false;
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed(), "a new outage must not revive retired balance");
        }
    }
    @ParameterizedTest @EnumSource(Algorithm.class)
    void infeasibleSharesAreDataRejectionsWithoutSchedulesOrTracking(Algorithm algorithm) throws Exception {
        var policies = policies(algorithm, 3, 1);
        var primary = new RecoveryPrimaryFixture(policies);
        try (var store = new FallbackRateLimitStore(primary, settings(4, 10), List.of())) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            await(() -> store.state() == DegradationState.CLOSED);
            assertTrue(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed(), "small healthy policies remain valid");
            primary.available = false;
            flow.tryAcquire("quota", RateLimitContext.empty());
            var result = store.tryAcquire(new BucketIdentity(new QuotaDomain("default", "quota"), "quota", Scope.GLOBAL, "all"),
                    policies.policy("quota").limit().orElseThrow(), algorithm, 1);
            assertFalse(result.acquired()); assertEquals(0, result.remaining()); assertEquals(0, result.retryAfterMillis());
            assertEquals(0, store.trackedBuckets());
        }
    }
    @Test void failedStartupNeverAllocatesUnprovenCreditAndCountsRejections() {
        var policies = policies(Algorithm.TOKEN_BUCKET, 100, 100);
        var primary = new RecoveryPrimaryFixture(policies); primary.available = false;
        var decisions = new AtomicInteger();
        var listener = new DegradationListener() {
            public void onTransition(DegradationState from, DegradationState to, String reason) { }
            public void onFallbackDecision(String policy, String group, Verdict verdict) { decisions.incrementAndGet(); }
        };
        try (var store = new FallbackRateLimitStore(primary, settings(1, 10), List.of(listener))) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            for (int i = 0; i < 10; i++) assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            assertEquals(10, decisions.get()); assertEquals(0, store.trackedBuckets());
        }
    }
    @Test void lateOldPrimaryResponseCannotRetireTheCurrentOutage() throws Exception {
        var policies = policies(Algorithm.TOKEN_BUCKET, 100, 100);
        var primary = new RecoveryPrimaryFixture(policies);
        var delayed = new CompletableFuture<ChainResult>();
        try (var store = new FallbackRateLimitStore(primary, settings(1, 10), List.of())) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            await(() -> store.state() == DegradationState.CLOSED);
            primary.delayedAcquisition = delayed;
            var key = new BucketIdentity(new QuotaDomain("default", "quota"), "quota", Scope.GLOBAL, "all");
            var request = List.of(new LevelRequest(key, policies.policy("quota").limit().orElseThrow(), Algorithm.TOKEN_BUCKET, 1));
            var old = store.tryAcquireAll(request).toCompletableFuture();
            await(() -> primary.acquisitions.get() == 1);
            primary.available = false;
            assertFalse(store.tryAcquireAll(request).toCompletableFuture().join().acquired());
            delayed.complete(ChainResult.acquired(0, 99));
            assertFalse(old.join().acquired());
            assertEquals(DegradationState.OPEN, store.state());
        }
    }
    @Test void trackingPressureAcrossSiblingsCannotDebitTheirSharedParent() throws Exception {
        var policies = PolicySet.compile(List.of(
                RateLimitPolicy.builder("provider").scope(Scope.GLOBAL).limit(new Limit(10, 10, Duration.ofSeconds(1))).build(),
                RateLimitPolicy.builder("leaf").scope(Scope.USER).parentId("provider").limit(new Limit(10, 10, Duration.ofSeconds(1))).build()));
        var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong();
        try (var store = new FallbackRateLimitStore(primary, settings(1, 2), List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            await(() -> store.state() == DegradationState.CLOSED);
            var alice = RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "alice").build();
            var bob = RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "bob").build();
            primary.available = false;
            assertFalse(flow.tryAcquire("leaf", alice).isAllowed());
            assertFalse(flow.tryAcquire("leaf", alice).isAllowed());
            assertEquals(2, store.trackedBuckets());
            time.set(Duration.ofMillis(100).toNanos());
            assertFalse(flow.tryAcquire("leaf", bob).isAllowed(), "new child exceeds the tracking bound");
            assertTrue(flow.tryAcquire("leaf", alice).isAllowed(), "rejected sibling must not debit the common parent");
            assertFalse(flow.tryAcquire("provider", RateLimitContext.empty()).isAllowed(), "direct parent uses the same local route and balance");
            assertEquals(2, store.trackedBuckets());
        }
    }
    @Test void idleProbeFailureEntersDegradationWithoutConsumingQuota() throws Exception {
        var policies = policies(Algorithm.TOKEN_BUCKET, 10, 10); var primary = new RecoveryPrimaryFixture(policies);
        try (var store = new FallbackRateLimitStore(primary, settings(1, 10), List.of())) {
            DefaultQuotaFlow.builder(policies, store).build();
            await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false;
            await(() -> store.state() == DegradationState.OPEN);
            assertEquals(0, primary.acquisitions.get());
            assertEquals(0, store.trackedBuckets());
        }
    }
    @Test void stalledLocalAdmissionCannotExtendTheRecoveryAttemptOrCrossItsFence() throws Exception {
        var policies = policies(Algorithm.TOKEN_BUCKET, 10, 10); var primary = new RecoveryPrimaryFixture(policies);
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var calls = new AtomicInteger();
        java.util.function.LongSupplier clock = () -> {
            if (Thread.currentThread().getName().equals("stalled-guard") && calls.incrementAndGet() == 2) {
                entered.countDown();
                try { assertTrue(release.await(3, TimeUnit.SECONDS)); }
                catch (InterruptedException failure) { throw new AssertionError(failure); }
            }
            return System.nanoTime();
        };
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofMillis(100));
        var worker = Executors.newSingleThreadExecutor(action -> new Thread(action, "stalled-guard"));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), clock)) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            await(() -> store.state() == DegradationState.CLOSED); primary.available = false;
            flow.tryAcquire("quota", RateLimitContext.empty()); flow.tryAcquire("quota", RateLimitContext.empty());
            // Exercise the guard directly: facade calls now isolate SPI invocation on bounded workers.
            var attempt = worker.submit(() -> new PolicyEngine(store).evaluate(policies, "quota", RateLimitContext.empty(), 1));
            assertTrue(entered.await(2, TimeUnit.SECONDS)); primary.available = true;
            Thread.sleep(200);
            assertEquals(0, primary.seeds.get(), "a stalled entrant cannot be omitted from final accounting");
            assertNotEquals(DegradationState.CLOSED, store.state());
            release.countDown(); assertFalse(attempt.get(2, TimeUnit.SECONDS).isAllowed());
            await(() -> store.state() == DegradationState.CLOSED);
        } finally { release.countDown(); worker.shutdownNow(); }
    }
    @Test void inFlightSaturationRejectsBeforeDispatchAndShutdownRetiresLeases() throws Exception {
        var policies = policies(Algorithm.TOKEN_BUCKET, 10, 10); var primary = new RecoveryPrimaryFixture(policies);
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 1,
                Duration.ofMillis(20), Duration.ofSeconds(1));
        var delayed = new CompletableFuture<ChainResult>();
        try (var store = new FallbackRateLimitStore(primary, settings, List.of())) {
            DefaultQuotaFlow.builder(policies, store).build(); await(() -> store.state() == DegradationState.CLOSED);
            primary.delayedAcquisition = delayed;
            var key = new BucketIdentity(new QuotaDomain("default", "quota"), "quota", Scope.GLOBAL, "all");
            var request = List.of(new LevelRequest(key, policies.policy("quota").limit().orElseThrow(), Algorithm.TOKEN_BUCKET, 1));
            var first = store.tryAcquireAll(request).toCompletableFuture(); await(() -> primary.acquisitions.get() == 1);
            assertFalse(store.tryAcquireAll(request).toCompletableFuture().join().acquired());
            assertEquals(1, primary.acquisitions.get());
            store.close(); assertEquals(0, store.trackedBuckets()); assertFalse(first.join().acquired());
            delayed.complete(ChainResult.acquired(0, 9));
            assertEquals(0, store.trackedBuckets()); assertFalse(first.join().acquired());
            assertFalse(store.tryAcquireAll(request).toCompletableFuture().join().acquired());
            assertThrows(CompletionException.class, () -> store.configureRecovery(policies, "default", null).toCompletableFuture().join());
        }
    }
    @Test void lostReadyReplyKeepsAdmissionQuiescedUntilAnAuthoritativeRead() throws Exception {
        var policies = policies(Algorithm.TOKEN_BUCKET, 10, 10); var primary = new RecoveryPrimaryFixture(policies);
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofMillis(200));
        var lost = new CompletableFuture<RecoveryControlResult>();
        try (var store = new FallbackRateLimitStore(primary, settings, List.of())) {
            var flow = DefaultQuotaFlow.builder(policies, store).build(); await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false;
            flow.tryAcquire("quota", RateLimitContext.empty()); flow.tryAcquire("quota", RateLimitContext.empty());
            var key = new BucketIdentity(new QuotaDomain("default", "quota"), "quota", Scope.GLOBAL, "global");
            var held = new AtomicReference<StoreResult>();
            primary.onReady = () -> held.set(store.tryAcquire(key, policies.policy("quota").limit().orElseThrow(), Algorithm.TOKEN_BUCKET, 1));
            primary.delayedReady = lost; primary.available = true;
            await(() -> held.get() != null);
            assertNotNull(held.get().recoveryPending());
            await(() -> store.state() == DegradationState.CLOSED);
            assertEquals(0, store.trackedBuckets());
            primary.available = false;
            flow.tryAcquire("quota", RateLimitContext.empty()); flow.tryAcquire("quota", RateLimitContext.empty());
            int tracked = store.trackedBuckets();
            lost.complete(primary.lastReady);
            assertEquals(DegradationState.OPEN, store.state()); assertEquals(tracked, store.trackedBuckets());
        }
    }
    @Test void zeroShareChildRejectsWeightedChainsWithoutSpendingItsUsableParent() throws Exception {
        var root = RateLimitPolicy.builder("root").scope(Scope.GLOBAL).limit(new Limit(100, 100, Duration.ofSeconds(1))).build();
        var child = RateLimitPolicy.builder("child").scope(Scope.USER).parentId("root").limit(new Limit(3, 1, Duration.ofSeconds(1))).build();
        var policies = PolicySet.compile(List.of(root, child)); var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong();
        try (var store = new FallbackRateLimitStore(primary, settings(4, 10), List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            var user = RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "user").build();
            await(() -> store.state() == DegradationState.CLOSED);
            assertTrue(flow.tryAcquire("child", user).isAllowed());
            primary.available = false; flow.tryAcquire("child", user);
            assertFalse(flow.tryAcquire("root", RateLimitContext.empty()).isAllowed());
            time.set(80_000_000L);
            var blocked = flow.tryAcquire("child", user, 2);
            assertFalse(blocked.isAllowed()); assertEquals(0, blocked.remaining()); assertTrue(blocked.retryAfter().isEmpty());
            assertTrue(flow.tryAcquire("root", RateLimitContext.empty(), 2).isAllowed());
        }
    }
    @Test void invalidAdapterOutcomesFailBoundedlyInsteadOfHangingOrGrantingFallback() throws Exception {
        for (ChainResult invalid : Arrays.asList(null, ChainResult.acquired(5, 0), ChainResult.rejected(0, -1, 10))) {
            var policies = policies(Algorithm.TOKEN_BUCKET, 10, 10); var primary = new RecoveryPrimaryFixture(policies);
            try (var store = new FallbackRateLimitStore(primary, settings(1, 10), List.of())) {
                DefaultQuotaFlow.builder(policies, store).build(); await(() -> store.state() == DegradationState.CLOSED);
                primary.delayedAcquisition = CompletableFuture.completedFuture(invalid);
                var key = new BucketIdentity(new QuotaDomain("default", "quota"), "quota", Scope.GLOBAL, "all");
                var error = assertThrows(ExecutionException.class, () -> store.tryAcquireAsync(key,
                        policies.policy("quota").limit().orElseThrow(), Algorithm.TOKEN_BUCKET, 1).toCompletableFuture().get(1, TimeUnit.SECONDS));
                assertInstanceOf(StateCompatibilityException.class, error.getCause());
                assertEquals(DegradationState.OPEN, store.state()); assertEquals(0, store.trackedBuckets());
            }
        }
    }
    @Test void obsoletePrimaryFailureCannotEmitAnotherFallbackDecisionAfterFencing() throws Exception {
        var policies = policies(Algorithm.TOKEN_BUCKET, 10, 10); var primary = new RecoveryPrimaryFixture(policies);
        var notifications = new AtomicInteger();
        var listener = new DegradationListener() {
            public void onTransition(DegradationState from, DegradationState to, String reason) { }
            public void onFallbackDecision(String policy, String group, Verdict verdict) { notifications.incrementAndGet(); }
        };
        var delayed = new CompletableFuture<ChainResult>();
        try (var store = new FallbackRateLimitStore(primary, settings(1, 10), List.of(listener))) {
            DefaultQuotaFlow.builder(policies, store).build(); await(() -> store.state() == DegradationState.CLOSED);
            var key = new BucketIdentity(new QuotaDomain("default", "quota"), "quota", Scope.GLOBAL, "all");
            var request = List.of(new LevelRequest(key, policies.policy("quota").limit().orElseThrow(), Algorithm.TOKEN_BUCKET, 1));
            primary.delayedAcquisition = delayed;
            var old = store.tryAcquireAll(request).toCompletableFuture(); await(() -> primary.acquisitions.get() == 1);
            primary.available = false; assertFalse(store.tryAcquireAll(request).toCompletableFuture().join().acquired());
            primary.delayedAcquisition = null; primary.available = true;
            await(() -> store.state() == DegradationState.CLOSED); assertFalse(old.join().acquired());
            primary.available = false; store.tryAcquireAll(request).toCompletableFuture().join();
            int before = notifications.get();
            delayed.completeExceptionally(new IllegalStateException("lost old response"));
            assertEquals(before, notifications.get()); assertEquals(DegradationState.OPEN, store.state());
        }
    }
    @Test void guardedPrimaryReportsTheConservativeBalanceAndRetryOfBothLimiters() throws Exception {
        var policies = policies(Algorithm.TOKEN_BUCKET, 10, 10); var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong();
        try (var store = new FallbackRateLimitStore(primary, settings(2, 10), List.of(), time::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).build(); await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false;
            flow.tryAcquire("quota", RateLimitContext.empty()); flow.tryAcquire("quota", RateLimitContext.empty());
            time.set(500_000_000L); flow.tryAcquire("quota", RateLimitContext.empty(), 5);
            time.set(1_000_000_000L); assertTrue(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            primary.awaitOtherMembers = true; primary.reportedRemaining = 10; primary.available = true;
            await(() -> primary.joins.get() > 0);
            Decision allowed; long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            do { allowed = flow.tryAcquire("quota", RateLimitContext.empty()); if (allowed.isAllowed()) break; Thread.sleep(5); }
            while (System.nanoTime() < deadline);
            assertTrue(allowed.isAllowed()); assertEquals(3, allowed.remaining()); assertEquals(1, primary.acquisitions.get());
            primary.allow = false;
            var rejected = flow.tryAcquire("quota", RateLimitContext.empty(), 3);
            assertFalse(rejected.isAllowed()); assertEquals(0, rejected.remaining());
            assertEquals(Duration.ofMillis(600), rejected.retryAfter().orElseThrow());
        }
    }
    @Test void shutdownDoesNotEnrollAfterALateHealthProbeCompletion() throws Exception {
        var policies = policies(Algorithm.TOKEN_BUCKET, 10, 10); var primary = new RecoveryPrimaryFixture(policies);
        var probe = new CompletableFuture<Void>(); primary.delayedProbe = probe;
        var store = new FallbackRateLimitStore(primary, settings(1, 10), List.of());
        DefaultQuotaFlow.builder(policies, store).build(); await(() -> primary.probes.get() == 1);
        store.close(); probe.complete(null);
        Thread.sleep(30);
        assertEquals(0, primary.enrollments.get()); assertEquals(0, store.trackedBuckets());
    }
}
