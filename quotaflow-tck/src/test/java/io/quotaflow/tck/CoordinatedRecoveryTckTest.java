package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.fallback.*;
import io.quotaflow.store.redis.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CoordinatedRecoveryTckTest extends TckContainers {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void twoHealthyMembersCompleteBarrierEvenWhenOneHasNoTraffic(boolean valkey) throws Exception {
        var cohort = new RecoveryCohort(List.of("a", "b"));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL)
                .limit(new Limit(10, 1, Duration.ofSeconds(1))).build()));
        var domain = new QuotaDomain("default", "quota");
        var client = RedisClientFactory.createClient(valkey ? valkeyUri() : redisUri(), Duration.ofSeconds(2));
        try (var aConnection = client.connect(); var bConnection = client.connect();
             var aStore = new RedisRateLimitStore(aConnection, RedisStoreConfig.defaults());
             var bStore = new RedisRateLimitStore(bConnection, RedisStoreConfig.defaults())) {
            aStore.registerPolicies(List.of(new PolicyBinding(domain, "quota", Scope.GLOBAL, Algorithm.TOKEN_BUCKET))).toCompletableFuture().join();
            var admin = new RedisRecoveryController(aConnection, Duration.ofSeconds(2));
            admin.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            admin.provisionDomain(domain, cohort, "initial", policies.recoveryFingerprint("quota"), true, true).toCompletableFuture().join();
            var endpointAvailable = new java.util.concurrent.atomic.AtomicBoolean(false);
            var disposed = new java.util.concurrent.atomic.AtomicInteger();
            try (var reconnecting = new ReconnectingRecoveryPrimary(() -> {
                if (!endpointAvailable.get()) throw new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED);
                var ownedConnection = client.connect();
                var ownedStore = new RedisRateLimitStore(ownedConnection, RedisStoreConfig.defaults());
                return new ReconnectingRecoveryPrimary.Connection(new RedisRecoveryPrimary("default", ownedConnection, ownedStore, Duration.ofSeconds(2), true),
                        () -> { ownedStore.close(); ownedConnection.close(); disposed.incrementAndGet(); });
            }, Duration.ofSeconds(2));
                 var a = new CoordinatedFallbackStore(reconnecting, settings("a", cohort), List.of());
                 var b = new CoordinatedFallbackStore(new RedisRecoveryPrimary("default", bConnection, bStore, Duration.ofSeconds(2), true),
                         settings("b", cohort), List.of())) {
                var flowA = DefaultQuotaFlow.builder(policies, a).build();
                DefaultQuotaFlow.builder(policies, b).build(); // deliberately idle owner
                assertFalse(flowA.tryAcquire("quota", RateLimitContext.empty()).isAllowed(), "unvalidated startup cannot grant a local share");
                assertEquals(DegradationState.OPEN, a.state());
                endpointAvailable.set(true);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while ((a.state() != DegradationState.CLOSED || b.state() != DegradationState.CLOSED) && System.nanoTime() < deadline)
                    Thread.sleep(20);
                assertEquals(DegradationState.CLOSED, a.state());
                assertEquals(DegradationState.CLOSED, b.state());
                assertTrue(flowA.tryAcquire("quota", RateLimitContext.empty(), 10).isAllowed());
                assertFalse(flowA.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            }
            assertEquals(1, disposed.get());
        } finally { client.shutdown(); }
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(Algorithm.class)
    void staggeredRecoveryRetainsSharesAndNeverSeedsAnInstanceCountMultiple(Algorithm algorithm) throws Exception {
        var cohort = new RecoveryCohort(List.of("a", "b"));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).algorithm(algorithm)
                .limit(new Limit(10, 10, Duration.ofSeconds(1))).build()));
        var domain = new QuotaDomain("default", "quota");
        var client = RedisClientFactory.createClient(redisUri(), Duration.ofSeconds(2));
        var clock = new java.util.concurrent.atomic.AtomicLong();
        var availableA = new java.util.concurrent.atomic.AtomicBoolean(true);
        var availableB = new java.util.concurrent.atomic.AtomicBoolean(true);
        try (var ca = client.connect(); var cb = client.connect();
             var sa = new RedisRateLimitStore(ca, RedisStoreConfig.defaults());
             var sb = new RedisRateLimitStore(cb, RedisStoreConfig.defaults())) {
            sa.registerPolicies(List.of(new PolicyBinding(domain, "quota", Scope.GLOBAL, algorithm))).toCompletableFuture().join();
            var admin = new RedisRecoveryController(ca, Duration.ofSeconds(2));
            admin.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            admin.provisionDomain(domain, cohort, "initial", policies.recoveryFingerprint("quota"), true, true).toCompletableFuture().join();
            try (var a = new CoordinatedFallbackStore(gated(new RedisRecoveryPrimary("default", ca, sa, Duration.ofSeconds(2), true), availableA), settings("a", cohort), List.of(), clock::get);
                 var b = new CoordinatedFallbackStore(gated(new RedisRecoveryPrimary("default", cb, sb, Duration.ofSeconds(2), true), availableB), settings("b", cohort), List.of(), clock::get)) {
                var fa = DefaultQuotaFlow.builder(policies, a).build();
                var fb = DefaultQuotaFlow.builder(policies, b).build();
                await(() -> a.state() == DegradationState.CLOSED && b.state() == DegradationState.CLOSED);
                availableA.set(false); availableB.set(false);
                assertFalse(fa.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
                assertFalse(fb.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
                // First local observations allocate empty shares; elapsed time earns their credit.
                assertFalse(fa.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
                assertFalse(fb.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
                clock.set(1_000_000_000L);
                assertTrue(fb.tryAcquire("quota", RateLimitContext.empty(), 5).isAllowed());
                availableA.set(true);
                Thread.sleep(150);
                assertNotEquals(DegradationState.CLOSED, a.state(), "one owner cannot retire the fleet guard");
                int allowed = 0;
                for (int i = 0; i < 6; i++) if (fa.tryAcquire("quota", RateLimitContext.empty()).isAllowed()) allowed++;
                assertTrue(allowed <= 5, "recovered owner cannot spend more than its conserved share");
                availableB.set(true); clock.set(1_100_000_000L);
                await(() -> a.state() == DegradationState.CLOSED && b.state() == DegradationState.CLOSED);
                assertFalse(fa.tryAcquire("quota", RateLimitContext.empty(), 10).isAllowed(), "handoff cannot create another full burst");
                assertEquals(0, a.trackedBuckets()); assertEquals(0, b.trackedBuckets());
            }
        } finally { client.shutdown(); }
    }

    @Test void rotatingIdentitiesExpireOnlyWhenIdleAndRecreateWithoutBurst() throws Exception {
        var cohort = RecoveryCohort.single();
        var limit = new Limit(2, 2, Duration.ofSeconds(1));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.USER).limit(limit).build()));
        var domain = new QuotaDomain("default", "quota");
        var client = RedisClientFactory.createClient(redisUri(), Duration.ofSeconds(2));
        var clock = new java.util.concurrent.atomic.AtomicLong();
        var available = new java.util.concurrent.atomic.AtomicBoolean(true);
        try (var connection = client.connect(); var primary = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            primary.registerPolicies(List.of(new PolicyBinding(domain, "quota", Scope.USER, Algorithm.TOKEN_BUCKET))).toCompletableFuture().join();
            var admin = new RedisRecoveryController(connection, Duration.ofSeconds(2));
            admin.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            admin.provisionDomain(domain, cohort, "initial", policies.recoveryFingerprint("quota"), true, true).toCompletableFuture().join();
            var settings = new RecoverySettings("default", "test", "single", cohort, 2, 2, Duration.ofMillis(20), Duration.ofSeconds(2));
            try (var store = new CoordinatedFallbackStore(gated(new RedisRecoveryPrimary("default", connection, primary,
                    Duration.ofSeconds(2), true), available), settings, List.of(), clock::get)) {
                store.registerPolicies(List.of(new PolicyBinding(domain, "quota", Scope.USER, Algorithm.TOKEN_BUCKET))).toCompletableFuture().join();
                store.configureRecovery(policies, "default", null).toCompletableFuture().join();
                await(() -> store.state() == DegradationState.CLOSED);
                available.set(false);
                var first = new BucketIdentity(domain, "quota", Scope.USER, "first");
                assertFalse(store.tryAcquire(first, limit, Algorithm.TOKEN_BUCKET, 1).acquired());
                for (int round = 0; round < 20; round++) {
                    var a = new BucketIdentity(domain, "quota", Scope.USER, "a-" + round);
                    var b = new BucketIdentity(domain, "quota", Scope.USER, "b-" + round);
                    assertFalse(store.tryAcquire(a, limit, Algorithm.TOKEN_BUCKET, 1).acquired());
                    assertFalse(store.tryAcquire(b, limit, Algorithm.TOKEN_BUCKET, 1).acquired());
                    assertFalse(store.tryAcquire(first, limit, Algorithm.TOKEN_BUCKET, 1).acquired());
                    assertEquals(2, store.trackedBuckets());
                    clock.addAndGet(500_000_000L);
                    assertTrue(store.tryAcquire(a, limit, Algorithm.TOKEN_BUCKET, 1).acquired(), "pressure must keep existing identities usable");
                    clock.addAndGet(4_000_000_000L);
                    await(() -> store.trackedBuckets() == 0);
                    assertFalse(store.tryAcquire(a, limit, Algorithm.TOKEN_BUCKET, 1).acquired(), "expired state must recreate empty");
                    clock.addAndGet(4_000_000_000L);
                    await(() -> store.trackedBuckets() == 0);
                }
            }
        } finally { client.shutdown(); }
    }

    @Test void lostPrimaryReplyNeverCreatesASecondGrantAndUncertaintyPreventsExpiry() throws Exception {
        var cohort = RecoveryCohort.single();
        var limit = new Limit(10, 10, Duration.ofSeconds(1));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limit(limit).build()));
        var domain = new QuotaDomain("default", "quota");
        var key = new BucketIdentity(domain, "quota", Scope.GLOBAL, "all");
        var client = RedisClientFactory.createClient(redisUri(), Duration.ofSeconds(2));
        var clock = new java.util.concurrent.atomic.AtomicLong();
        var available = new java.util.concurrent.atomic.AtomicBoolean(true);
        var loseReply = new java.util.concurrent.atomic.AtomicBoolean();
        var executions = new java.util.concurrent.atomic.AtomicInteger();
        try (var connection = client.connect(); var primary = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            primary.registerPolicies(List.of(new PolicyBinding(domain, "quota", Scope.GLOBAL, Algorithm.TOKEN_BUCKET))).toCompletableFuture().join();
            var admin = new RedisRecoveryController(connection, Duration.ofSeconds(2));
            admin.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            admin.provisionDomain(domain, cohort, "initial", policies.recoveryFingerprint("quota"), true, true).toCompletableFuture().join();
            RecoveryPrimary delegate = gated(new RedisRecoveryPrimary("default", connection, primary, Duration.ofSeconds(2), true), available);
            RecoveryPrimary lossy = (RecoveryPrimary) java.lang.reflect.Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(),
                    new Class<?>[]{RecoveryPrimary.class}, (proxy, method, args) -> {
                        try {
                            Object result = method.invoke(delegate, args);
                            if (method.getName().equals("acquire")) {
                                executions.incrementAndGet();
                                if (loseReply.compareAndSet(true, false)) {
                                    return ((java.util.concurrent.CompletionStage<?>) result).thenCompose(acknowledged -> {
                                        available.set(false);
                                        return new java.util.concurrent.CompletableFuture<>();
                                    });
                                }
                            }
                            return result;
                        } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var settings = new RecoverySettings("default", "test", "single", cohort, 2, 1, Duration.ofMillis(20), Duration.ofMillis(200));
            try (var store = new CoordinatedFallbackStore(lossy, settings, List.of(), clock::get)) {
                store.registerPolicies(List.of(new PolicyBinding(domain, "quota", Scope.GLOBAL, Algorithm.TOKEN_BUCKET))).toCompletableFuture().join();
                store.configureRecovery(policies, "default", null).toCompletableFuture().join();
                await(() -> store.state() == DegradationState.CLOSED);
                loseReply.set(true);
                assertFalse(store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 5).acquired(), "lost response cannot be reissued as local allow");
                assertEquals(1, executions.get());
                assertFalse(store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1).acquired(), "uncertain state starts empty");
                clock.set(TimeUnit.SECONDS.toNanos(100));
                Thread.sleep(100);
                assertEquals(1, store.trackedBuckets(), "unacknowledged dispatch must survive an elapsed refill horizon");
                available.set(true); clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
                await(() -> store.state() == DegradationState.CLOSED);
                assertEquals(0, store.trackedBuckets(), "authoritative dispatch fencing retires old uncertainty");
                assertEquals(1, executions.get(), "control probes must not execute business acquisitions");
            }
        } finally { client.shutdown(); }
    }

    private static RecoveryPrimary gated(RecoveryPrimary delegate, java.util.concurrent.atomic.AtomicBoolean available) {
        return (RecoveryPrimary) java.lang.reflect.Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(),
                new Class<?>[]{RecoveryPrimary.class}, (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) return method.invoke(delegate, args);
                    if (!available.get()) return java.util.concurrent.CompletableFuture.failedFuture(
                            new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED));
                    try { return method.invoke(delegate, args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
    }
    private static void await(java.util.function.BooleanSupplier ready) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!ready.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(20);
        assertTrue(ready.getAsBoolean(), "cohort did not complete a stable recovery round");
    }

    private static RecoverySettings settings(String id, RecoveryCohort cohort) {
        return new RecoverySettings("default", "test", id, cohort, 100, 100, Duration.ofMillis(20), Duration.ofSeconds(2));
    }
}
