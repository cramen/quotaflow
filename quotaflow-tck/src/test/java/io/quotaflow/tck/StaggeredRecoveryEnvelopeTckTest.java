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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Exact business-admission comparison, independent of host throughput or scheduler speed. */
class StaggeredRecoveryEnvelopeTckTest extends TckContainers {
    @ParameterizedTest @EnumSource(Algorithm.class)
    void completeStaggeredTraceAddsNoCreditAndStaysWithinTheReferenceRate(Algorithm algorithm) throws Exception {
        var limit = new Limit(10, 1, Duration.ofSeconds(100));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).algorithm(algorithm).limit(limit).build()));
        var domain = new QuotaDomain("default", "quota"); var cohort = new RecoveryCohort(List.of("a", "b"));
        var time = new AtomicLong(); var onlineA = new AtomicBoolean(true); var onlineB = new AtomicBoolean(true);
        var client = RedisClientFactory.createClient(redisUri(), Duration.ofSeconds(2));
        try (var ca = client.connect(); var cb = client.connect(); var sa = new RedisRateLimitStore(ca, RedisStoreConfig.defaults());
             var sb = new RedisRateLimitStore(cb, RedisStoreConfig.defaults())) {
            String clockKey = ControlledRedisClock.install(sa, domain); ControlledRedisClock.install(sb, domain);
            ca.sync().set(clockKey, "0");
            sa.registerPolicies(List.of(new PolicyBinding(domain, "quota", Scope.GLOBAL, algorithm))).toCompletableFuture().join();
            var admin = new RedisRecoveryController(ca, Duration.ofSeconds(2));
            admin.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            admin.provisionDomain(domain, cohort, "initial", policies.recoveryFingerprint("quota"), true, true).toCompletableFuture().join();
            try (var a = new FallbackRateLimitStore(gated(sa.recoveryPrimary("default", Duration.ofSeconds(2), true), onlineA), settings("a", cohort), List.of(), time::get);
                 var b = new FallbackRateLimitStore(gated(sb.recoveryPrimary("default", Duration.ofSeconds(2), true), onlineB), settings("b", cohort), List.of(), time::get)) {
                var fa = DefaultQuotaFlow.builder(policies, a).build(); var fb = DefaultQuotaFlow.builder(policies, b).build();
                await(() -> a.state() == DegradationState.CLOSED && b.state() == DegradationState.CLOSED);
                var trace = new Trace(limit, time);
                assertTrue(trace.ask(fa, 10));
                onlineA.set(false); onlineB.set(false);
                assertFalse(trace.ask(fa, 1)); assertFalse(trace.ask(fb, 1));
                assertFalse(trace.ask(fa, 1)); assertFalse(trace.ask(fb, 1));
                setTime(ca, clockKey, time, 500);
                assertFalse(trace.ask(fa, 5)); assertFalse(trace.ask(fb, 5));
                setTime(ca, clockKey, time, 1000);
                assertTrue(trace.ask(fb, 5)); // exact A=5, B=0 guard state before A rejoins
                onlineA.set(true);
                String control = RedisKeyScheme.defaults().controlKey(domain);
                awaitRecovery(() -> "GATHER".equals(ca.sync().hget(control, "phase")) && "1".equals(ca.sync().hget(control, "joined")), ca, clockKey, time);
                var bucket = new BucketIdentity(domain, "quota", Scope.GLOBAL, "global");
                assertEquals(5, Long.parseLong(ca.sync().get(RedisKeyScheme.defaults().singleKey(bucket)).split(":")[3]), "seed is own r, never N*r");
                long before = trace.actual; long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (trace.actual - before < 5 && System.nanoTime() < deadline) {
                    trace.ask(fa, 1); Thread.sleep(5);
                }
                assertEquals(5, trace.actual - before);
                assertFalse(trace.ask(fa, 1), "guard prevents another primary burst while B is local-only");
                assertNotEquals(DegradationState.CLOSED, a.state());
                onlineB.set(true);
                awaitRecovery(() -> a.state() == DegradationState.CLOSED && b.state() == DegradationState.CLOSED
                        && a.trackedBuckets() == 0 && b.trackedBuckets() == 0, ca, clockKey, time);
                assertFalse(trace.ask(fa, 10), "NORMAL cannot mint a second full burst");
                long recoveredBefore = trace.actual;
                long recoveredAt = time.get();
                for (int i = 1; i <= 10; i++) {
                    setNanos(ca, clockKey, time, recoveredAt + TimeUnit.SECONDS.toNanos(i * 100L));
                    assertTrue(trace.ask(i % 2 == 0 ? fa : fb, 1));
                    assertFalse(trace.ask(fa, 1));
                }
                assertEquals(10, trace.actual - recoveredBefore, "pooled refill is restored at the configured rate");
                assertTrue(trace.actual * 5 <= trace.reference * 6,
                        "whole-trace admitted weight exceeds the 1.2x reference bound: " + trace.actual + "/" + trace.reference);
                assertEquals(0, a.trackedBuckets()); assertEquals(0, b.trackedBuckets());
            }
        } finally { client.shutdown(); }
    }
    private static final class Trace {
        private final long interval, maximum; private final AtomicLong time;
        private long at, envelopeCredit, referenceCredit, actual, reference;
        Trace(Limit limit, AtomicLong time) {
            this.time = time; interval = limit.emissionIntervalNanos(); maximum = limit.capacity() * interval;
            envelopeCredit = maximum; referenceCredit = maximum;
        }
        boolean ask(DefaultQuotaFlow flow, long weight) {
            long now = time.get(), elapsed = now - at; at = now;
            envelopeCredit = Math.min(maximum, envelopeCredit + elapsed); referenceCredit = Math.min(maximum, referenceCredit + elapsed);
            long cost = weight * interval;
            if (referenceCredit >= cost) { referenceCredit -= cost; reference += weight; }
            boolean allowed = flow.tryAcquire("quota", RateLimitContext.empty(), weight).isAllowed();
            if (allowed) {
                assertTrue(envelopeCredit >= cost, "actual trace exceeded its exact global admission envelope");
                envelopeCredit -= cost; actual += weight;
            }
            return allowed;
        }
    }
    private static void setTime(io.lettuce.core.api.StatefulRedisConnection<String, String> connection, String key, AtomicLong local, long seconds) {
        setNanos(connection, key, local, TimeUnit.SECONDS.toNanos(seconds));
    }
    private static void setNanos(io.lettuce.core.api.StatefulRedisConnection<String, String> connection, String key, AtomicLong local, long nanos) {
        connection.sync().set(key, Long.toString(nanos)); local.set(nanos);
    }
    private static void awaitRecovery(BooleanSupplier condition, io.lettuce.core.api.StatefulRedisConnection<String, String> connection,
                                      String key, AtomicLong time) throws Exception {
        // A failed in-flight control call can publish backoff after connectivity returns.
        // Keep both clocks advancing until the barrier completes, while asserting that
        // the entire handoff earns less than one additional global token (100 seconds).
        await(() -> {
            if (condition.getAsBoolean()) return true;
            long next = time.get() + TimeUnit.MILLISECONDS.toNanos(50);
            assertTrue(next < TimeUnit.SECONDS.toNanos(1090), "recovery exceeded the sub-token clock budget");
            setNanos(connection, key, time, next);
            return false;
        });
    }
    private static RecoveryPrimary gated(RecoveryPrimary primary, AtomicBoolean online) {
        return (RecoveryPrimary) java.lang.reflect.Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, args) -> {
                    if (!online.get()) return CompletableFuture.failedFuture(new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED));
                    try { return method.invoke(primary, args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
    }
    private static RecoverySettings settings(String id, RecoveryCohort cohort) {
        return new RecoverySettings("default", "trace", id, cohort, 100, 100, Duration.ofMillis(20), Duration.ofSeconds(2));
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        do {
            if (condition.getAsBoolean()) return;
            Thread.sleep(10);
        } while (System.nanoTime() < deadline);
        assertTrue(condition.getAsBoolean(), "cohort did not reach the trace checkpoint");
    }
}
