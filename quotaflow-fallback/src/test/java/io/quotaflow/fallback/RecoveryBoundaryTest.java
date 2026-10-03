package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.testing.RecoveryPrimaryFixture;
import java.lang.reflect.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class RecoveryBoundaryTest {
    private static final Limit LIMIT = new Limit(10, 10, Duration.ofSeconds(1));
    private static final QuotaDomain DOMAIN = new QuotaDomain("default", "quota");
    private static final BucketIdentity KEY = new BucketIdentity(DOMAIN, "quota", Scope.GLOBAL, "global");
    private static PolicySet policies() {
        return PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limit(LIMIT).build()));
    }
    private static RecoverySettings settings() {
        return new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 2,
                Duration.ofMillis(10), Duration.ofMillis(500));
    }
    private static RecoveryContext context(RecoveryContext c, QuotaDomain domain, RecoverySession session,
                                           long epoch, long dispatch, RecoveryPhase phase) {
        return new RecoveryContext(domain, session, epoch, dispatch, c.configurationVersion(), c.configurationFingerprint(), phase);
    }
    static Stream<UnaryOperator<RecoveryControlResult>> corruptions() {
        return Stream.of(
            r -> new RecoveryControlResult(true, r.context(), -1, 1, 1),
            r -> new RecoveryControlResult(true, r.context(), 2, 1, 1),
            r -> new RecoveryControlResult(true, r.context(), 1, -1, 1),
            r -> new RecoveryControlResult(true, r.context(), 1, 2, 1),
            r -> new RecoveryControlResult(true, r.context(), 0, 1, 1),
            r -> new RecoveryControlResult(true, r.context(), 1, 0, 1),
            r -> new RecoveryControlResult(true, r.context(), 1, 1, -1),
            r -> new RecoveryControlResult(true, r.context(), 1, 1, 2),
            r -> new RecoveryControlResult(true, r.context(), 1, 1, 0),
            r -> new RecoveryControlResult(true, context(r.context(), new QuotaDomain("default", "other"), r.context().session(), 1, 1, RecoveryPhase.NORMAL), 1, 1, 1),
            r -> new RecoveryControlResult(true, context(r.context(), DOMAIN,
                    new RecoverySession("other", r.context().session().cohortDigest(), "single", 0, 1, "other"), 1, 1, RecoveryPhase.NORMAL), 1, 1, 1),
            r -> new RecoveryControlResult(true, context(r.context(), DOMAIN, r.context().session(), 0, 1, RecoveryPhase.NORMAL), 1, 1, 0),
            r -> new RecoveryControlResult(true, context(r.context(), DOMAIN, r.context().session(), 1, 0, RecoveryPhase.NORMAL), 1, 1, 1),
            r -> new RecoveryControlResult(true, context(r.context(), DOMAIN, r.context().session(), 2, 2, RecoveryPhase.GATHER), 1, 0, 1),
            r -> new RecoveryControlResult(true, context(r.context(), DOMAIN, r.context().session(), 2, 2, RecoveryPhase.GATHER), 0, 1, 1),
            r -> new RecoveryControlResult(true, context(r.context(), DOMAIN, r.context().session(), 2, 2, RecoveryPhase.GATHER), 0, 0, 2),
            r -> new RecoveryControlResult(true, context(r.context(), DOMAIN, r.context().session(), 2, 2, RecoveryPhase.DRAIN), 0, 0, 1),
            r -> new RecoveryControlResult(true, context(r.context(), DOMAIN, r.context().session(), 2, 2, RecoveryPhase.DRAIN), 1, 1, 1),
            r -> new RecoveryControlResult(true, context(r.context(), DOMAIN, r.context().session(), 2, 2, RecoveryPhase.DRAIN), 1, 0, 2)
        );
    }
    @ParameterizedTest @MethodSource("corruptions")
    void incompatibleControlMetadataCannotReopenOrAllocateFallback(UnaryOperator<RecoveryControlResult> corrupt) throws Exception {
        var policies = policies(); var fixture = new RecoveryPrimaryFixture(policies);
        var enabled = new AtomicBoolean();
        RecoveryPrimary primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(),
                new Class<?>[]{RecoveryPrimary.class}, (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(fixture, args);
                        if (method.getName().equals("read") && enabled.get()) {
                            @SuppressWarnings("unchecked") var stage = (CompletionStage<RecoveryControlResult>) result;
                            return stage.thenApply(corrupt);
                        }
                        return result;
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of())) {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            enabled.set(true);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.OPEN);
            var failure = assertThrows(CompletionException.class, () -> store.tryAcquireAsync(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1)
                    .toCompletableFuture().join());
            assertInstanceOf(StateCompatibilityException.class, failure.getCause());
            assertEquals(0, fixture.acquisitions.get());
            assertEquals(0, store.trackedBuckets());
        }
    }

    @Test void directStoreRequestsCannotBypassTheCapturedPolicy() throws Exception {
        var policies = policies(); var primary = new RecoveryPrimaryFixture(policies);
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of())) {
            assertThrows(PolicyConfigurationException.class, () -> store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1));
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            for (var wrong : List.of(new Limit(11, 10, Duration.ofSeconds(1)), new Limit(10, 9, Duration.ofSeconds(1))))
                assertThrows(PolicyConfigurationException.class, () -> store.tryAcquire(KEY, wrong, Algorithm.TOKEN_BUCKET, 1));
            var unknown = new BucketIdentity(new QuotaDomain("default", "unknown"), "unknown", Scope.GLOBAL, "global");
            assertThrows(PolicyConfigurationException.class, () -> store.tryAcquire(unknown, LIMIT, Algorithm.TOKEN_BUCKET, 1));
            var inactive = new BucketIdentity(DOMAIN, "removed", Scope.GLOBAL, "global");
            assertFalse(store.tryAcquire(inactive, LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired());
            assertFalse(store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 11).acquired());
            var stale = new LevelRequest(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1, "global", "f".repeat(64));
            assertNotNull(store.tryAcquireAll(List.of(stale)).toCompletableFuture().join().recoveryPending());
            assertNotNull(store.tryAcquireAsync(KEY, LIMIT, Algorithm.GCRA, 1).toCompletableFuture().join().recoveryPending());
            assertEquals(0, primary.acquisitions.get()); assertEquals(0, store.trackedBuckets());
            assertTrue(store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired());
            assertEquals(1, primary.acquisitions.get());
        }
    }

    @Test void generationCallbacksRetireExactlyOnceAndCloseAfterRetirement() throws Exception {
        var events = new CopyOnWriteArrayList<String>();
        var listener = new DegradationListener() {
            public void onTransition(DegradationState from, DegradationState to, String reason) { }
            public void onFallbackDecision(String policy, String group, Verdict verdict) { }
            public void onConfiguration(long revision, Set<String> policies) { events.add("configuration:" + revision); }
            public void onRetired(long revision) { events.add("retired:" + revision); }
            public void onClosed() { events.add("closed"); }
        };
        var policies = policies(); var primary = new RecoveryPrimaryFixture(policies);
        var store = new FallbackRateLimitStore(primary, settings(), List.of(listener));
        try {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            store.configureRecovery(policies, "default", null).toCompletableFuture().join();
            store.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(List.of("configuration:0", "configuration:1", "retired:0"), events);
            store.close(); store.close();
            FallbackRateLimitStoreTest.await(() -> events.contains("closed"));
            assertEquals(List.of("configuration:0", "configuration:1", "retired:0", "retired:1", "closed"), events);
            assertEquals(0, store.observationFailures());
        } finally { store.close(); }
    }

    static Stream<UnaryOperator<RecoverySession>> invalidOwners() {
        return Stream.of(owner -> null,
                owner -> new RecoverySession(owner.cohortIncarnation(), "f".repeat(64), owner.instanceId(), owner.slot(), owner.generation(), owner.token()),
                owner -> new RecoverySession(owner.cohortIncarnation(), owner.cohortDigest(), "impostor", owner.slot(), owner.generation(), owner.token()),
                owner -> new RecoverySession(owner.cohortIncarnation(), owner.cohortDigest(), owner.instanceId(), 1, owner.generation(), owner.token()));
    }
    @ParameterizedTest @MethodSource("invalidOwners")
    void unprovenOwnerCannotObtainEvenAConservativeShare(UnaryOperator<RecoverySession> invalid) throws Exception {
        var policies = policies(); var fixture = new RecoveryPrimaryFixture(policies);
        var answered = new AtomicBoolean();
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(fixture, args);
                        if (method.getName().equals("enroll")) {
                            @SuppressWarnings("unchecked") var stage = (CompletionStage<RecoverySession>) result;
                            return stage.thenApply(owner -> { answered.set(true); return invalid.apply(owner); });
                        }
                        return result;
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of())) {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(answered::get);
            // A control callback may still be publishing its terminal ownership error.
            FallbackRateLimitStoreTest.await(() -> {
                try { store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1); return false; }
                catch (CompletionException failure) { return failure.getCause() instanceof StateCompatibilityException; }
            });
            assertEquals(DegradationState.OPEN, store.state());
            assertEquals(0, fixture.acquisitions.get()); assertEquals(0, store.trackedBuckets());
            assertEquals(1, fixture.enrollments.get());
        }
    }
    @Test void configurationAndRegistrationErrorsAreNotTransportDegradation() throws Exception {
        var policies = policies(); var fixture = new RecoveryPrimaryFixture(policies);
        var registrationError = new AtomicBoolean();
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("registerPolicies") && registrationError.get())
                        return CompletableFuture.failedFuture(new PolicyConfigurationException("incompatible test binding"));
                    try { return method.invoke(fixture, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of())) {
            assertThrows(PolicyConfigurationException.class, () -> store.registerPolicies(List.of(new PolicyBinding(
                    new QuotaDomain("other", "quota"), "quota", Scope.GLOBAL, Algorithm.TOKEN_BUCKET))));
            assertThrows(PolicyConfigurationException.class, () -> store.validateRecoveryConfiguration(policies, "other", null));
            assertThrows(PolicyConfigurationException.class, () -> store.configureRecovery(policies, "other", null));
            var dynamic = PolicySet.compile(List.of(RateLimitPolicy.builder("dynamic").scope(Scope.GLOBAL).limitRef("plan").build()));
            assertThrows(PolicyConfigurationException.class, () -> store.configureRecovery(dynamic, "default", (reference, group) -> Optional.of(LIMIT)));
            registrationError.set(true);
            var error = assertThrows(CompletionException.class, () -> store.registerPolicies(List.of(PolicyBinding.of(KEY, Algorithm.TOKEN_BUCKET)))
                    .toCompletableFuture().join());
            assertInstanceOf(PolicyConfigurationException.class, error.getCause());
            registrationError.set(false);
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            store.close();
            assertThrows(CompletionException.class, () -> store.registerPolicies(List.of(PolicyBinding.of(KEY, Algorithm.TOKEN_BUCKET)))
                    .toCompletableFuture().join());
        }
    }
    @Test void failedObserverCannotAffectQuotaOrPreventOtherObserversFromRetiring() throws Exception {
        var closed = new AtomicInteger();
        var failing = new DegradationListener() {
            public void onTransition(DegradationState from, DegradationState to, String reason) { throw new IllegalStateException("observer failure"); }
            public void onFallbackDecision(String policy, String group, Verdict verdict) { throw new IllegalStateException("observer failure"); }
        };
        var healthy = new DegradationListener() {
            public void onTransition(DegradationState from, DegradationState to, String reason) { }
            public void onFallbackDecision(String policy, String group, Verdict verdict) { }
            public void onClosed() { closed.incrementAndGet(); }
        };
        var policies = policies(); var primary = new RecoveryPrimaryFixture(policies);
        var store = new FallbackRateLimitStore(primary, settings(), List.of(failing, healthy));
        try {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertTrue(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            store.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertTrue(store.observationFailures() > 0);
        } finally { store.close(); }
        FallbackRateLimitStoreTest.await(() -> closed.get() == 1);
    }

    @Test void inFlightControlReadDoesNotPauseAnUnchangedHealthyRoute() throws Exception {
        var policies = policies(); var fixture = new RecoveryPrimaryFixture(policies);
        var holdReads = new AtomicBoolean(); var entered = new CountDownLatch(1);
        var reply = new CompletableFuture<RecoveryControlResult>();
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("read") && holdReads.get()) { entered.countDown(); return reply; }
                    try { return method.invoke(fixture, arguments); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of())) {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            holdReads.set(true); assertTrue(entered.await(2, TimeUnit.SECONDS));
            for (int i = 0; i < 20; i++) assertTrue(store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired(),
                    "a background read of the same configuration cannot close healthy admission");
            assertEquals(20, fixture.acquisitions.get()); assertEquals(0, store.trackedBuckets());
            reply.complete(fixture.current(DOMAIN));
            assertEquals(DegradationState.CLOSED, store.state());
        } finally { reply.cancel(false); }
    }
    @Test void retiredGenerationDeliversItsLastDecisionBeforeItsRetirementAndStoreClosure() throws Exception {
        var policies = policies(); var fixture = new RecoveryPrimaryFixture(policies);
        var hold = new AtomicBoolean(); var entered = new CountDownLatch(1);
        var control = new CompletableFuture<RecoveryControlResult>();
        var acquisition = new CompletableFuture<ChainResult>();
        var events = new CopyOnWriteArrayList<String>();
        var listener = new DegradationListener() {
            public void onTransition(DegradationState from, DegradationState to, String reason) { }
            public void onFallbackDecision(String policy, String group, Verdict verdict) { }
            public void onConfiguration(long revision, Set<String> membership) { events.add("configuration:" + revision); }
            public void onFallbackDecision(long revision, boolean current, String policy, String group, Verdict verdict) {
                events.add("decision:" + revision + ":" + current);
            }
            public void onRetired(long revision) { events.add("retired:" + revision); }
            public void onClosed() { events.add("closed"); }
        };
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("read") && hold.get()) { entered.countDown(); return control; }
                    try { return method.invoke(fixture, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        var store = new FallbackRateLimitStore(primary, settings(), List.of(listener));
        try {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            hold.set(true); assertTrue(entered.await(2, TimeUnit.SECONDS));
            fixture.delayedAcquisition = acquisition;
            var old = store.tryAcquireAll(List.of(new LevelRequest(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1))).toCompletableFuture();
            FallbackRateLimitStoreTest.await(() -> fixture.acquisitions.get() == 1);
            store.configureRecovery(policies, "default", null).toCompletableFuture().join();
            store.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(List.of("configuration:0", "configuration:1"), events, "old owner still has an admitted attempt");
            acquisition.complete(ChainResult.acquired(0, 9));
            assertFalse(old.get(2, TimeUnit.SECONDS).acquired(), "a late pre-reload result cannot cross its route fence");
            store.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(List.of("configuration:0", "configuration:1", "decision:0:true", "retired:0"), events);
            store.close();
            FallbackRateLimitStoreTest.await(() -> events.contains("closed"));
            assertEquals("retired:1", events.get(events.size() - 2)); assertEquals("closed", events.get(events.size() - 1));
        } finally { store.close(); control.cancel(false); acquisition.cancel(false); }
    }

    @Test void freshOwnershipRequiresBothInitialSeedPhasesBeforeHealthyAdmission() throws Exception {
        var policies = policies(); var fixture = new RecoveryPrimaryFixture(policies);
        fixture.startGather = true;
        var seeded = new CompletableFuture<Boolean>(); fixture.delayedSeed = seeded;
        try (var store = new FallbackRateLimitStore(fixture, settings(), List.of())) {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> fixture.seeds.get() > 0);
            assertEquals(DegradationState.OPEN, store.state());
            assertNotNull(store.tryAcquireAsync(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).toCompletableFuture().join().recoveryPending());
            assertEquals(0, fixture.acquisitions.get());
            assertEquals(0, fixture.joins.get()); assertEquals(0, fixture.readiness.get());
            seeded.complete(true);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertTrue(fixture.seeds.get() >= 2, "GATHER and DRAIN need distinct final accounting");
            assertEquals(1, fixture.joins.get()); assertEquals(1, fixture.readiness.get());
            assertTrue(store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired());
            assertEquals(0, store.trackedBuckets());
        }
    }

    @Test void callerSnapshotsCannotBypassMissingLimitsOrReopenAnOlderTarget() throws Exception {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limitRef("plan").build()));
        var snapshot = new LimitSnapshot(1, Map.of(new LimitSnapshot.Key("plan", "global"), LIMIT));
        var published = new AtomicReference<>(snapshot);
        var fixture = new RecoveryPrimaryFixture(policies);
        var hold = new AtomicBoolean(); var entered = new CountDownLatch(1);
        var read = new CompletableFuture<RecoveryControlResult>();
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("read") && hold.get()) { entered.countDown(); return read; }
                    try { return method.invoke(fixture, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        VersionedLimitResolver resolver = () -> Optional.of(published.get());
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of())) {
            DefaultQuotaFlow.builder(policies, store).limitResolver(resolver).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            hold.set(true); assertTrue(entered.await(2, TimeUnit.SECONDS));
            var correct = new LevelRequest(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1, "global", policies.recoveryFingerprint("quota"), 1, snapshot.fingerprint());
            var wrongGroup = new LevelRequest(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1, "unresolved", policies.recoveryFingerprint("quota"), 1, snapshot.fingerprint());
            var unresolved = store.tryAcquireAll(List.of(wrongGroup)).toCompletableFuture().join();
            assertFalse(unresolved.acquired()); assertNull(unresolved.recoveryPending());
            var conflict = new LevelRequest(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1, "global", policies.recoveryFingerprint("quota"), 1, "f".repeat(64));
            assertNotNull(store.tryAcquireAll(List.of(conflict)).toCompletableFuture().join().recoveryPending());
            assertTrue(store.tryAcquireAll(List.of(correct)).toCompletableFuture().join().acquired(), "bad same-version input cannot invalidate the proven target");
            var newer = new LevelRequest(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1, "global", policies.recoveryFingerprint("quota"), 2, "e".repeat(64));
            assertNotNull(store.tryAcquireAll(List.of(newer)).toCompletableFuture().join().recoveryPending());
            assertNotNull(store.tryAcquireAll(List.of(correct)).toCompletableFuture().join().recoveryPending(), "observing a newer target fences old requests");
            assertEquals(1, fixture.acquisitions.get()); assertEquals(0, store.trackedBuckets());
        } finally { read.cancel(false); }
    }
    @Test void removingAnEntireDomainRejectsStaleDirectRequestsWithoutDispatch() throws Exception {
        var quota = RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limit(LIMIT).build();
        var other = RateLimitPolicy.builder("other").scope(Scope.GLOBAL).limit(LIMIT).build();
        var all = PolicySet.compile(List.of(quota, other));
        var fixture = new RecoveryPrimaryFixture(all);
        var retiredDecisions = new AtomicInteger();
        var listener = new DegradationListener() {
            public void onTransition(DegradationState from, DegradationState to, String reason) { }
            public void onFallbackDecision(String policy, String group, Verdict verdict) { }
            public void onFallbackDecision(long revision, boolean current, String policy, String group, Verdict verdict) {
                if (policy.equals("quota") && !current && verdict == Verdict.REJECTED) retiredDecisions.incrementAndGet();
            }
        };
        try (var store = new FallbackRateLimitStore(fixture, settings(), List.of(listener))) {
            var flow = DefaultQuotaFlow.builder(all, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            flow.replacePolicySet(PolicySet.compile(List.of(other)));
            var result = store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1);
            assertFalse(result.acquired()); assertEquals(0, result.remaining()); assertEquals(0, result.retryAfterMillis());
            assertNull(result.recoveryPending()); assertEquals(0, fixture.acquisitions.get());
            store.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(1, retiredDecisions.get());
        }
    }

    @Test void guardedHierarchyCombinesEveryRemoteBudgetAndKeepsPermanentDenialUnscheduled() throws Exception {
        var limit = new Limit(10, 10, Duration.ofSeconds(100));
        var childLimit = new Limit(20, 20, Duration.ofSeconds(100));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limit(limit).build(),
                RateLimitPolicy.builder("child").scope(Scope.USER).parentId("quota").limit(childLimit).build()));
        var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong();
        var settings = new RecoverySettings("default", "test", "a", new RecoveryCohort(List.of("a", "b")),
                10, 10, Duration.ofMillis(10), Duration.ofSeconds(1));
        var child = new BucketIdentity(DOMAIN, "child", Scope.USER, "user");
        var one = List.of(new LevelRequest(KEY, limit, Algorithm.TOKEN_BUCKET, 1), new LevelRequest(child, childLimit, Algorithm.TOKEN_BUCKET, 1));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), time::get)) {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false; store.tryAcquireAll(one).toCompletableFuture().join(); store.tryAcquireAll(one).toCompletableFuture().join();
            time.set(Duration.ofSeconds(100).toNanos()); assertTrue(store.tryAcquireAll(one).toCompletableFuture().join().acquired());
            primary.awaitOtherMembers = true;
            primary.accounting = chain -> CompletableFuture.completedFuture(ChainResult.acquired(1, 2).withBudgets(List.of(
                    new LevelBudget(0, new StoreBudget(10, 2, false)), new LevelBudget(1, new StoreBudget(20, 3, false)))));
            primary.available = true; time.addAndGet(Duration.ofSeconds(1).toNanos());
            FallbackRateLimitStoreTest.await(() -> primary.joins.get() > 0);
            var allowed = new AtomicReference<ChainResult>();
            FallbackRateLimitStoreTest.await(() -> {
                var result = store.tryAcquireAll(one).toCompletableFuture().join();
                if (result.recoveryPending() != null) return false;
                allowed.set(result); return true;
            });
            assertTrue(allowed.get().acquired()); assertEquals(2, allowed.get().remaining());
            assertEquals(List.of(new LevelBudget(0, new StoreBudget(5, 2, true)), new LevelBudget(1, new StoreBudget(10, 3, true))), allowed.get().budgets());
            primary.accounting = chain -> CompletableFuture.completedFuture(ChainResult.rejected(0, 2, 0).withBudgets(List.of(
                    new LevelBudget(0, new StoreBudget(10, 2, false)), new LevelBudget(1, new StoreBudget(20, 3, false)))));
            var three = List.of(new LevelRequest(KEY, limit, Algorithm.TOKEN_BUCKET, 3), new LevelRequest(child, childLimit, Algorithm.TOKEN_BUCKET, 3));
            var rejected = store.tryAcquireAll(three).toCompletableFuture().join();
            assertFalse(rejected.acquired()); assertEquals(0, rejected.remaining()); assertEquals(0, rejected.retryAfterMillis());
            assertEquals(List.of(new LevelBudget(0, new StoreBudget(5, 0, true)), new LevelBudget(1, new StoreBudget(10, 3, true))), rejected.budgets());
        }
    }

    @Test void boundedControlWorkEventuallyVisitsEveryIndependentDomain() throws Exception {
        var policies = PolicySet.compile(java.util.stream.IntStream.range(0, 9)
                .mapToObj(i -> RateLimitPolicy.builder("quota-" + i).scope(Scope.GLOBAL).limit(LIMIT).build()).toList());
        var primary = new RecoveryPrimaryFixture(policies); primary.startGather = true;
        var barrier = new CompletableFuture<Boolean>(); primary.delayedSeed = barrier;
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 32, 32,
                Duration.ofMillis(10), Duration.ofSeconds(5));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of())) {
            var flow = DefaultQuotaFlow.builder(PolicySet.compile(List.of(policies.policy("quota-0"))), store).build();
            FallbackRateLimitStoreTest.await(() -> primary.seeds.get() == 1);
            Thread.sleep(60); // Repeated ticks encounter the same in-flight domain ticket.
            barrier.complete(true);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.delayedSeed = new CompletableFuture<>();
            flow.replacePolicySet(policies);
            FallbackRateLimitStoreTest.await(() -> primary.seeds.get() >= 6);
            Thread.sleep(40);
            assertEquals(6, primary.seeds.get(), "control dispatch must remain bounded while native replies are pending");
            assertEquals(DegradationState.OPEN, store.state());
            primary.delayedSeed.complete(true);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertEquals(9, primary.joins.get()); assertEquals(9, primary.readiness.get());
            assertEquals(18, primary.seeds.get());
            for (var policy : policies.policies()) assertTrue(flow.tryAcquire(policy.id(), RateLimitContext.empty()).isAllowed());
            assertEquals(9, primary.acquisitions.get()); assertEquals(0, store.trackedBuckets());
        } finally { barrier.cancel(false); primary.delayedSeed.cancel(false); }
    }

    @Test void shutdownTerminatesPendingReadinessWithoutCancellingNativeSeedOrSendingReady() throws Exception {
        var policies = policies(); var primary = new RecoveryPrimaryFixture(policies); primary.startGather = true;
        var seed = new CompletableFuture<Boolean>(); primary.delayedSeed = seed;
        var store = new FallbackRateLimitStore(primary, settings(), List.of());
        try {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> primary.seeds.get() == 1);
            var pending = store.tryAcquireAsync(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).toCompletableFuture().join().recoveryPending();
            assertNotNull(pending);
            var caller = pending.readiness().toCompletableFuture();
            var observer = pending.readiness().toCompletableFuture();
            caller.cancel(false); assertFalse(observer.isDone());
            store.close();
            assertTrue(observer.isCompletedExceptionally()); assertFalse(seed.isDone());
            seed.complete(true);
            assertEquals(0, primary.joins.get()); assertEquals(0, primary.readiness.get());
            assertEquals(0, store.trackedBuckets()); assertEquals(DegradationState.OPEN, store.state());
        } finally { store.close(); seed.cancel(false); }
    }

    @Test void controlRetriesBackOffOnTheMonotonicClockAndResetAfterRecovery() throws Exception {
        var policies = policies(); var fixture = new RecoveryPrimaryFixture(policies);
        var unavailable = new AtomicBoolean(); var failures = new AtomicInteger(); var time = new AtomicLong();
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("read") && unavailable.get()) {
                        failures.incrementAndGet();
                        return CompletableFuture.failedFuture(new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED));
                    }
                    try { return method.invoke(fixture, arguments); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        var observedFailureClock = new AtomicInteger();
        java.util.function.LongSupplier clock = () -> {
            long captured = time.get();
            observedFailureClock.set(failures.get());
            return captured;
        };
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of(), clock)) {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            unavailable.set(true); FallbackRateLimitStoreTest.await(() -> failures.get() == 1 && observedFailureClock.get() == 1);
            Thread.sleep(40); assertEquals(1, failures.get());
            time.set(10_000_000); FallbackRateLimitStoreTest.await(() -> failures.get() == 2 && observedFailureClock.get() == 2);
            time.set(29_000_000); Thread.sleep(40); assertEquals(2, failures.get(), "the second backoff is twice the base interval");
            time.set(30_000_000); FallbackRateLimitStoreTest.await(() -> failures.get() == 3 && observedFailureClock.get() == 3);
            unavailable.set(false); time.set(70_000_000);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            Thread.sleep(20);
            unavailable.set(true); FallbackRateLimitStoreTest.await(() -> failures.get() == 4 && observedFailureClock.get() == 4);
            time.set(80_000_000); FallbackRateLimitStoreTest.await(() -> failures.get() == 5);
            assertEquals(DegradationState.OPEN, store.state());
        }
    }

    @Test void transitionsIncludeUnvalidatedStartupAndShutdownExactlyOnce() throws Exception {
        var events = new CopyOnWriteArrayList<String>();
        var listener = new DegradationListener() {
            public void onTransition(DegradationState from, DegradationState to, String reason) { events.add(from + ">" + to); }
            public void onFallbackDecision(String policy, String group, Verdict verdict) { }
            public void onClosed() { events.add("closed"); }
        };
        var fixture = new RecoveryPrimaryFixture(policies());
        var store = new FallbackRateLimitStore(fixture, settings(), List.of(listener));
        try {
            DefaultQuotaFlow.builder(policies(), store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            store.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(List.of("CLOSED>OPEN", "OPEN>CLOSED"), events);
            store.close();
            FallbackRateLimitStoreTest.await(() -> events.contains("closed"));
            assertEquals(List.of("CLOSED>OPEN", "OPEN>CLOSED", "CLOSED>OPEN", "closed"), events);
        } finally { store.close(); }
        events.clear();
        var unconfigured = new FallbackRateLimitStore(fixture, settings(), List.of(listener));
        unconfigured.close(); unconfigured.close();
        FallbackRateLimitStoreTest.await(() -> events.contains("closed"));
        assertEquals(List.of("CLOSED>OPEN", "closed"), events);
        var executor = Executors.newSingleThreadExecutor(action -> { var thread = new Thread(action); thread.setDaemon(true); return thread; });
        try {
            assertFalse(executor.submit(() -> unconfigured.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired()).get(2, TimeUnit.SECONDS));
        } finally { executor.shutdownNow(); }
    }
    @Test void malformedChainsAndConflictingBindingsCannotLeakConfigurationOwnership() throws Exception {
        var retired = new CopyOnWriteArrayList<Long>();
        var closed = new CountDownLatch(1);
        var listener = new DegradationListener() {
            public void onTransition(DegradationState from, DegradationState to, String reason) { }
            public void onFallbackDecision(String policy, String group, Verdict verdict) { }
            public void onRetired(long revision) { retired.add(revision); }
            public void onClosed() { closed.countDown(); }
        };
        var policies = policies(); var fixture = new RecoveryPrimaryFixture(policies);
        try (var store = new FallbackRateLimitStore(fixture, settings(), List.of(listener))) {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            var request = new LevelRequest(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1);
            assertThrows(IllegalArgumentException.class, () -> store.tryAcquireAll(List.of(request, request)));
            assertThrows(IllegalArgumentException.class, () -> store.tryAcquireAll(List.of()));
            assertThrows(PolicyConfigurationException.class, () -> store.registerPolicies(List.of(PolicyBinding.of(KEY, Algorithm.GCRA))));
            assertEquals(0, fixture.acquisitions.get()); assertEquals(0, store.trackedBuckets());
            store.configureRecovery(policies, "default", null).toCompletableFuture().join();
            store.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(List.of(0L), retired, "invalid input must still release its captured configuration owner");
        }
        assertTrue(closed.await(2, TimeUnit.SECONDS)); assertEquals(List.of(0L, 1L), retired);
    }
    @Test void successfulReadyReplyRetiresTheGuardWithoutAnotherControlRoundTrip() throws Exception {
        var fixture = new RecoveryPrimaryFixture(policies()); fixture.startGather = true;
        var holdRead = new AtomicBoolean(); var pending = new CompletableFuture<RecoveryControlResult>();
        fixture.onReady = () -> holdRead.set(true);
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("read") && holdRead.get()) return pending;
                    try { return method.invoke(fixture, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(10));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of())) {
            DefaultQuotaFlow.builder(policies(), store).build();
            FallbackRateLimitStoreTest.await(() -> fixture.readiness.get() == 1);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertTrue(store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired());
            assertEquals(0, store.trackedBuckets());
        } finally { pending.cancel(false); }
    }

    static Stream<String> controlOperations() { return Stream.of("begin", "configure", "join", "ready", "abort"); }
    @ParameterizedTest @MethodSource("controlOperations")
    void everyControlMutationReplyMustProveTheSameDomainAndOwner(String operation) throws Exception {
        var policies = policies(); var fixture = new RecoveryPrimaryFixture(policies);
        fixture.startGather = operation.equals("join") || operation.equals("ready") || operation.equals("abort");
        var failReadyForAbort = new AtomicBoolean(operation.equals("abort"));
        var enabled = new AtomicBoolean(fixture.startGather);
        var returned = new CountDownLatch(1);
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("ready") && failReadyForAbort.compareAndSet(true, false))
                        return CompletableFuture.failedFuture(new TimeoutException("uncertain readiness"));
                    try {
                        var result = method.invoke(fixture, args);
                        if (method.getName().equals(operation) && enabled.get()) {
                            @SuppressWarnings("unchecked") var stage = (CompletionStage<RecoveryControlResult>) result;
                            return stage.thenApply(reply -> {
                                var c = reply.context();
                                var wrong = new RecoveryContext(new QuotaDomain("other", "quota"), c.session(), c.epoch(),
                                        c.dispatchGeneration(), c.configurationVersion(), c.configurationFingerprint(), c.phase());
                                returned.countDown();
                                return new RecoveryControlResult(reply.applied(), wrong, reply.joinedMembers(), reply.readyMembers(), reply.retiredEpoch());
                            });
                        }
                        return result;
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        var clock = new AtomicLong();
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of(), clock::get)) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            if (!fixture.startGather) {
                FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
                enabled.set(true);
                if (operation.equals("configure")) store.configureRecovery(policies, "default", null).toCompletableFuture().join();
                else {
                    fixture.available = false; flow.tryAcquire("quota", RateLimitContext.empty());
                    fixture.available = true; clock.set(Duration.ofSeconds(1).toNanos());
                }
            }
            if (operation.equals("abort")) {
                FallbackRateLimitStoreTest.await(() -> { clock.addAndGet(1_000_000); return returned.getCount() == 0; });
            } else assertTrue(returned.await(3, TimeUnit.SECONDS));
            FallbackRateLimitStoreTest.await(() -> {
                try { store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1); return false; }
                catch (CompletionException failure) { return failure.getCause() instanceof StateCompatibilityException; }
            });
            assertEquals(DegradationState.OPEN, store.state());
        }
    }

    @Test void supersededSeedCompletionCannotJoinTheOldConfigurationBarrier() throws Exception {
        var old = policies();
        var next = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL)
                .limit(new Limit(20, 20, Duration.ofSeconds(1))).build()));
        var primary = new RecoveryPrimaryFixture(old); primary.startGather = true;
        var oldSeed = new CompletableFuture<Boolean>(); var newSeed = new CompletableFuture<Boolean>();
        primary.delayedSeed = oldSeed;
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(5));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of())) {
            DefaultQuotaFlow.builder(old, store).build();
            FallbackRateLimitStoreTest.await(() -> primary.seeds.get() == 1);
            store.configureRecovery(next, "default", null).toCompletableFuture().join();
            primary.delayedSeed = newSeed; oldSeed.complete(true);
            FallbackRateLimitStoreTest.await(() -> primary.seeds.get() == 2);
            assertEquals(0, primary.joins.get(), "a stale seed completion cannot write readiness for its old target");
            assertFalse(store.tryAcquire(KEY, next.policy("quota").limit().orElseThrow(), Algorithm.TOKEN_BUCKET, 1).acquired());
            newSeed.complete(true);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertEquals(1, primary.joins.get());
            assertEquals(next.recoveryFingerprint("quota"), primary.current(DOMAIN).context().configurationFingerprint());
        } finally { oldSeed.cancel(false); newSeed.cancel(false); }
    }

    @Test void authoritativePendingQuiescesDispatchUntilTheControllerRevalidatesTheRoute() throws Exception {
        var fixture = new RecoveryPrimaryFixture(policies());
        var hold = new AtomicBoolean(); var entered = new CountDownLatch(1);
        var read = new CompletableFuture<RecoveryControlResult>();
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("read") && hold.get()) { entered.countDown(); return read; }
                    try { return method.invoke(fixture, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(5));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of())) {
            DefaultQuotaFlow.builder(policies(), store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            hold.set(true); assertTrue(entered.await(2, TimeUnit.SECONDS));
            var remoteSignal = new CompletableFuture<Void>();
            fixture.delayedAcquisition = CompletableFuture.completedFuture(ChainResult.pending(0, new RecoveryPending(DOMAIN, 4242, remoteSignal)));
            var pending = store.tryAcquireAsync(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).toCompletableFuture().join().recoveryPending();
            assertNotNull(pending); assertNotEquals(4242, pending.generation());
            remoteSignal.complete(null);
            assertNotNull(store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).recoveryPending());
            assertEquals(1, fixture.acquisitions.get()); assertEquals(0, store.trackedBuckets());
            assertEquals(DegradationState.OPEN, store.state());
        } finally { read.cancel(false); }
    }
    @Test void failedDrainReadinessIsAbortedBeforeAnotherSeedAndReadyAttempt() throws Exception {
        var fixture = new RecoveryPrimaryFixture(policies()); fixture.startGather = true;
        var failReady = new AtomicBoolean(true); var aborts = new AtomicInteger();
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("ready") && failReady.compareAndSet(true, false))
                        return CompletableFuture.failedFuture(new TimeoutException("uncertain readiness"));
                    if (method.getName().equals("abort")) aborts.incrementAndGet();
                    try { return method.invoke(fixture, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of())) {
            DefaultQuotaFlow.builder(policies(), store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertEquals(1, aborts.get()); assertEquals(2, fixture.joins.get());
            assertEquals(4, fixture.seeds.get()); assertEquals(1, fixture.readiness.get());
            assertEquals(0, store.trackedBuckets());
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void shutdownRacingAReservedObservationCannotLeaveTrackingOrAnUnfinishedCaller(boolean local) throws Exception {
        var fixture = new RecoveryPrimaryFixture(policies());
        var hold = new AtomicBoolean(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        java.util.function.LongSupplier clock = () -> {
            if (hold.get() && Thread.currentThread().getName().equals("held-observation")) {
                entered.countDown();
                try { assertTrue(release.await(3, TimeUnit.SECONDS)); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            }
            return 0;
        };
        var worker = Executors.newSingleThreadExecutor(action -> new Thread(action, "held-observation"));
        var store = new FallbackRateLimitStore(fixture, settings(), List.of(), clock);
        try {
            DefaultQuotaFlow.builder(policies(), store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            if (local) {
                fixture.available = false; store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1); store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1);
                assertEquals(1, store.trackedBuckets());
            }
            hold.set(true);
            var result = worker.submit(() -> store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            store.close(); release.countDown();
            assertFalse(result.get(2, TimeUnit.SECONDS).acquired());
            assertEquals(0, store.trackedBuckets()); assertEquals(0, fixture.acquisitions.get());
        } finally { release.countDown(); store.close(); worker.shutdownNow(); }
    }
    @Test void reloadRacingObservationReservationReleasesTheOldLeaseBeforeDispatch() throws Exception {
        var fixture = new RecoveryPrimaryFixture(policies());
        var hold = new AtomicBoolean(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        java.util.function.LongSupplier clock = () -> {
            if (hold.get() && Thread.currentThread().getName().equals("held-observation")) {
                entered.countDown();
                try { assertTrue(release.await(3, TimeUnit.SECONDS)); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            }
            return 0;
        };
        var worker = Executors.newSingleThreadExecutor(action -> new Thread(action, "held-observation"));
        try (var store = new FallbackRateLimitStore(fixture, settings(), List.of(), clock)) {
            DefaultQuotaFlow.builder(policies(), store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            hold.set(true);
            var result = worker.submit(() -> store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            store.configureRecovery(policies(), "default", null).toCompletableFuture().join();
            release.countDown(); assertFalse(result.get(2, TimeUnit.SECONDS).acquired());
            assertEquals(0, store.trackedBuckets()); assertEquals(0, fixture.acquisitions.get());
        } finally { release.countDown(); worker.shutdownNow(); }
    }

    @Test void anAlreadyReadyOwnerWaitsForPeersWithoutReseedingAndAcceptsTheFinalReadyReply() throws Exception {
        var fixture = new RecoveryPrimaryFixture(policies()); fixture.startGather = true; fixture.awaitOtherReadiness = true;
        var blockReads = new AtomicBoolean(); var pendingRead = new CompletableFuture<RecoveryControlResult>();
        fixture.onReady = () -> { if (fixture.lastReady.context().phase() == RecoveryPhase.NORMAL) blockReads.set(true); };
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("read") && blockReads.get()) return pendingRead;
                    try { return method.invoke(fixture, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        var settings = new RecoverySettings("default", "test", "a", new RecoveryCohort(List.of("a", "b")), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(5));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of())) {
            DefaultQuotaFlow.builder(policies(), store).build();
            FallbackRateLimitStoreTest.await(() -> fixture.readiness.get() >= 2);
            assertEquals(2, fixture.seeds.get(), "readiness polling must not seed an owner that already stopped admissions");
            assertEquals(DegradationState.OPEN, store.state());
            assertNotNull(store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).recoveryPending());
            assertEquals(0, fixture.acquisitions.get());
            fixture.awaitOtherReadiness = false;
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertTrue(store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired());
            assertEquals(2, fixture.seeds.get()); assertEquals(0, store.trackedBuckets());
        } finally { pendingRead.cancel(false); }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"epoch", "dispatch", "configuration"})
    void eachAuthoritativeFenceRetiresOldAttemptsBeforeTheirTransportDeadline(String counter) throws Exception {
        var fixture = new RecoveryPrimaryFixture(policies());
        var advance = new AtomicBoolean(); var reads = new AtomicInteger();
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    try {
                        Object response = method.invoke(fixture, args);
                        if (method.getName().equals("read")) {
                            reads.incrementAndGet();
                            @SuppressWarnings("unchecked") var stage = (CompletionStage<RecoveryControlResult>) response;
                            return stage.thenApply(reply -> {
                                if (!advance.get()) return reply;
                                var c = reply.context();
                                long epoch = c.epoch() + (counter.equals("epoch") ? 1 : 0);
                                var newer = new RecoveryContext(c.domain(), c.session(), epoch,
                                        c.dispatchGeneration() + (counter.equals("dispatch") ? 1 : 0),
                                        c.configurationVersion() + (counter.equals("configuration") ? 2 : 0),
                                        c.configurationFingerprint(), RecoveryPhase.NORMAL);
                                return new RecoveryControlResult(true, newer, 1, 1, epoch);
                            });
                        }
                        return response;
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        var settings = new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 10, 1,
                Duration.ofMillis(10), Duration.ofSeconds(10));
        var nativeReply = new CompletableFuture<ChainResult>();
        try (var store = new FallbackRateLimitStore(primary, settings, List.of())) {
            DefaultQuotaFlow.builder(policies(), store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            fixture.delayedAcquisition = nativeReply;
            var old = store.tryAcquireAll(List.of(new LevelRequest(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1))).toCompletableFuture();
            FallbackRateLimitStoreTest.await(() -> fixture.acquisitions.get() == 1);
            int before = reads.get();
            FallbackRateLimitStoreTest.await(() -> reads.get() >= before + 2);
            assertFalse(old.isDone(), "equal controller counters do not retire an active attempt");
            advance.set(true);
            var retired = old.get(2, TimeUnit.SECONDS);
            assertFalse(retired.acquired()); assertNotNull(retired.recoveryPending());
            assertFalse(nativeReply.isDone(), "fencing must not cancel or refund native commands");
            FallbackRateLimitStoreTest.await(() -> store.trackedBuckets() == 0);
            int afterFence = reads.get();
            FallbackRateLimitStoreTest.await(() -> reads.get() >= afterFence + 2);
            fixture.delayedAcquisition = null;
            assertTrue(store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1).acquired(), "the bounded dispatch permit must be released");
            nativeReply.complete(ChainResult.acquired(0, 9));
            assertFalse(old.join().acquired()); assertEquals(0, store.trackedBuckets());
        } finally { nativeReply.cancel(false); }
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(strings = {"seed", "join"})
    void gatherTransportFailureResumesOnlyTheConservedLocalShare(String operation) throws Exception {
        var limit = new Limit(10, 10, Duration.ofSeconds(100));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limit(limit).build()));
        var fixture = new RecoveryPrimaryFixture(policies); var fail = new AtomicBoolean(); var failures = new AtomicInteger();
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (method.getName().equals(operation) && fail.get()) {
                        failures.incrementAndGet(); return CompletableFuture.failedFuture(new java.io.IOException("control transport unavailable"));
                    }
                    try { return method.invoke(fixture, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        var time = new AtomicLong();
        var settings = new RecoverySettings("default", "test", "a", new RecoveryCohort(List.of("a", "b")), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), time::get)) {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            fixture.available = false; store.tryAcquire(KEY, limit, Algorithm.TOKEN_BUCKET, 1); store.tryAcquire(KEY, limit, Algorithm.TOKEN_BUCKET, 1);
            time.set(Duration.ofSeconds(100).toNanos()); assertTrue(store.tryAcquire(KEY, limit, Algorithm.TOKEN_BUCKET, 1).acquired());
            fail.set(true); fixture.available = true; time.set(Duration.ofSeconds(101).toNanos());
            FallbackRateLimitStoreTest.await(() -> failures.get() > 0);
            var result = new AtomicReference<StoreResult>();
            FallbackRateLimitStoreTest.await(() -> {
                var decision = store.tryAcquire(KEY, limit, Algorithm.TOKEN_BUCKET, 2);
                if (decision.recoveryPending() != null) return false;
                result.set(decision); return true;
            });
            assertTrue(result.get().acquired()); assertEquals(2, result.get().remaining());
            assertEquals(0, fixture.acquisitions.get()); assertEquals(DegradationState.OPEN, store.state());
        }
    }

    @Test void rejectedSeedsNeverAuthorizeJoinOrReadiness() throws Exception {
        var fixture = new RecoveryPrimaryFixture(policies()); fixture.startGather = true;
        fixture.delayedSeed = CompletableFuture.completedFuture(false);
        try (var store = new FallbackRateLimitStore(fixture, settings(), List.of())) {
            DefaultQuotaFlow.builder(policies(), store).build();
            FallbackRateLimitStoreTest.await(() -> fixture.seeds.get() >= 3);
            assertEquals(0, fixture.joins.get()); assertEquals(0, fixture.readiness.get());
            assertEquals(DegradationState.OPEN, store.state());
            fixture.delayedSeed = CompletableFuture.completedFuture(true);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertEquals(1, fixture.joins.get()); assertEquals(1, fixture.readiness.get());
        }
    }
    @Test void rejectedJoinCannotBeTreatedAsAJoinedParticipant() throws Exception {
        var fixture = new RecoveryPrimaryFixture(policies()); fixture.startGather = true;
        var reject = new AtomicBoolean(true); var rejected = new AtomicInteger();
        var primary = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("join") && reject.get()) {
                        rejected.incrementAndGet();
                        var current = fixture.current(DOMAIN);
                        return CompletableFuture.completedFuture(new RecoveryControlResult(false, current.context(),
                                current.joinedMembers(), current.readyMembers(), current.retiredEpoch()));
                    }
                    try { return method.invoke(fixture, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of())) {
            DefaultQuotaFlow.builder(policies(), store).build();
            FallbackRateLimitStoreTest.await(() -> rejected.get() >= 3);
            assertEquals(0, fixture.readiness.get()); assertEquals(DegradationState.OPEN, store.state());
            reject.set(false);
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            assertEquals(1, fixture.joins.get()); assertEquals(1, fixture.readiness.get());
        }
    }

    @Test void guardedRetryUsesTheMissingWeightAndRoundsFractionalMillisecondsUp() throws Exception {
        var limit = new Limit(10, 3, Duration.ofSeconds(1));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limit(limit).build()));
        var primary = new RecoveryPrimaryFixture(policies); var time = new AtomicLong();
        var settings = new RecoverySettings("default", "test", "a", new RecoveryCohort(List.of("a", "b")), 10, 10,
                Duration.ofMillis(10), Duration.ofSeconds(1));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(), time::get)) {
            DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false; store.tryAcquire(KEY, limit, Algorithm.TOKEN_BUCKET, 1); store.tryAcquire(KEY, limit, Algorithm.TOKEN_BUCKET, 1);
            time.set(4_000_000_000L); assertTrue(store.tryAcquire(KEY, limit, Algorithm.TOKEN_BUCKET, 1).acquired());
            primary.awaitOtherMembers = true;
            primary.accounting = requests -> CompletableFuture.completedFuture(ChainResult.rejected(0, 2, 1));
            primary.available = true; time.set(4_100_000_000L);
            FallbackRateLimitStoreTest.await(() -> primary.joins.get() > 0);
            var result = new AtomicReference<StoreResult>();
            FallbackRateLimitStoreTest.await(() -> {
                var decision = store.tryAcquire(KEY, limit, Algorithm.TOKEN_BUCKET, 3);
                if (decision.recoveryPending() != null) return false;
                result.set(decision); return true;
            });
            assertFalse(result.get().acquired()); assertEquals(1, result.get().remaining());
            // Two missing tokens at ceil(1s/3) * 2 per local token, rounded to milliseconds.
            assertEquals(1334, result.get().retryAfterMillis()); assertEquals(1, primary.acquisitions.get());
        }
    }
}
