package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.fallback.*;
import io.quotaflow.store.redis.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

class VersionedRecoveryTckTest extends TckContainers {
    static Stream<Arguments> backendsAndAlgorithms() {
        return Stream.of(false, true).flatMap(valkey -> Arrays.stream(Algorithm.values()).map(algorithm -> Arguments.of(valkey, algorithm)));
    }
    @ParameterizedTest @MethodSource("backendsAndAlgorithms")
    void staleProviderCannotRecoverOrGainLocalCreditAfterLearningANewerTarget(boolean valkey, Algorithm algorithm) throws Exception {
        var cohort = new RecoveryCohort(List.of("a", "b"));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).algorithm(algorithm).limitRef("plan").build()));
        var domain = new QuotaDomain("default", "quota"); var lookup = new LimitSnapshot.Key("plan", "global");
        var oldLimit = new Limit(10, 10, Duration.ofSeconds(1)); var currentLimit = new Limit(2, 1, Duration.ofSeconds(1));
        var first = new LimitSnapshot(1, Map.of(lookup, oldLimit)); var second = new LimitSnapshot(2, Map.of(lookup, currentLimit));
        var aView = new AtomicReference<>(first); var bView = new AtomicReference<>(first);
        VersionedLimitResolver aResolver = () -> Optional.of(aView.get()), bResolver = () -> Optional.of(bView.get());
        var bConnected = new AtomicBoolean(true);
        var client = RedisClientFactory.createClient(valkey ? valkeyUri() : redisUri(), Duration.ofSeconds(2));
        try (var ca = client.connect(); var cb = client.connect(); var sa = new RedisRateLimitStore(ca, RedisStoreConfig.defaults());
             var sb = new RedisRateLimitStore(cb, RedisStoreConfig.defaults())) {
            sa.registerPolicies(List.of(new PolicyBinding(domain, "quota", Scope.GLOBAL, algorithm))).toCompletableFuture().join();
            var admin = new RedisRecoveryController(ca, Duration.ofSeconds(2));
            admin.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            admin.provisionDomain(domain, cohort, "initial", policies.recoveryFingerprint("quota"), true, true).toCompletableFuture().join();
            var bPrimary = new RedisRecoveryPrimary("default", cb, sb, Duration.ofSeconds(2), true);
            var gated = (RecoveryPrimary) java.lang.reflect.Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                    (proxy, method, args) -> {
                        if (!bConnected.get()) return CompletableFuture.failedFuture(new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED));
                        try { return method.invoke(bPrimary, args); }
                        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    });
            try (var a = new FallbackRateLimitStore(new RedisRecoveryPrimary("default", ca, sa, Duration.ofSeconds(2), true), settings("a", cohort), List.of());
                 var b = new FallbackRateLimitStore(gated, settings("b", cohort), List.of())) {
                var fa = DefaultQuotaFlow.builder(policies, a).limitResolver(aResolver).build();
                var fb = DefaultQuotaFlow.builder(policies, b).limitResolver(bResolver).build();
                await(() -> a.state() == DegradationState.CLOSED && b.state() == DegradationState.CLOSED);
                aView.set(second);
                await(() -> "2".equals(ca.sync().hget(RedisKeyScheme.defaults().controlKey(domain), "resolver")));
                await(() -> b.state() == DegradationState.OPEN);
                await(() -> a.state() == DegradationState.CLOSED);
                assertTrue(fa.tryAcquire("quota", RateLimitContext.empty(), 2).isAllowed());
                var bucket = new BucketIdentity(domain, "quota", Scope.GLOBAL, "global");
                String protectedFingerprint = ca.sync().get(RedisKeyScheme.defaults().singleKey(bucket)).split(":")[2];
                assertFalse(fb.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
                fb.replacePolicySet(policies); // a larger local reload counter cannot make revision 1 current
                assertFalse(fb.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
                bConnected.set(false);
                Thread.sleep(100);
                assertFalse(fb.tryAcquire("quota", RateLimitContext.empty()).isAllowed(), "known stale provider state cannot become a local fallback bypass");
                assertEquals(protectedFingerprint, ca.sync().get(RedisKeyScheme.defaults().singleKey(bucket)).split(":")[2]);
                bView.set(second);
                await(() -> fb.tryAcquire("quota", RateLimitContext.empty()).retryAfter().isPresent());
                Thread.sleep(2100); // revision 2 gives this member one token every two seconds
                assertTrue(fb.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
                bConnected.set(true);
                await(() -> a.state() == DegradationState.CLOSED && b.state() == DegradationState.CLOSED);
                assertEquals("2", ca.sync().hget(RedisKeyScheme.defaults().controlKey(domain), "resolver-floor"));
                assertEquals(0, a.trackedBuckets()); assertEquals(0, b.trackedBuckets());
            }
        } finally { client.shutdown(); }
    }
    private static RecoverySettings settings(String id, RecoveryCohort cohort) {
        return new RecoverySettings("default", "versioned", id, cohort, 100, 100, Duration.ofMillis(20), Duration.ofSeconds(2));
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(20);
        assertTrue(condition.getAsBoolean(), "versioned cohort did not reach the expected state");
    }
}
