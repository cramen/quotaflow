package io.quotaflow.micrometer;

import static io.quotaflow.testing.TestIdentities.key;
import io.quotaflow.core.store.BucketIdentity;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.store.BatchRateLimitStore;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.StateSeeder;
import io.quotaflow.core.store.StoreResult;
import io.quotaflow.fallback.DegradationState;
import io.quotaflow.fallback.FallbackConfig;
import io.quotaflow.fallback.FallbackRateLimitStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MicrometerDegradationListenerTest {

    private static final Limit LIMIT = new Limit(100, 100, Duration.ofSeconds(1));
    private static final String KEY = "policy:global:shared";

    @Test
    void outageAndRecoveryMoveTheGaugeAndCountFallbackDecisions() throws Exception {
        var policies = io.quotaflow.core.PolicySet.compile(List.of(io.quotaflow.core.RateLimitPolicy.builder("policy")
                .scope(io.quotaflow.core.Scope.GLOBAL).limit(LIMIT).build()));
        var primary = new io.quotaflow.testing.RecoveryPrimaryFixture(policies);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        var listener = new MicrometerDegradationListener(registry);
        var settings = new io.quotaflow.fallback.RecoverySettings("default", "test", "single",
                io.quotaflow.core.store.RecoveryCohort.single(), 100, 100, Duration.ofMillis(10), Duration.ofSeconds(1));
        try (var store = new FallbackRateLimitStore(primary, settings, List.of(listener))) {
            var flow = io.quotaflow.core.DefaultQuotaFlow.builder(policies, store).build();
            await(() -> degraded(registry) == 0);
            flow.tryAcquire("policy", io.quotaflow.core.RateLimitContext.empty());
            store.flushObservations().toCompletableFuture().orTimeout(2, java.util.concurrent.TimeUnit.SECONDS).join();
            assertEquals(0, fallbackDecisions(registry));
            primary.available = false;
            flow.tryAcquire("policy", io.quotaflow.core.RateLimitContext.empty());
            store.flushObservations().toCompletableFuture().orTimeout(2, java.util.concurrent.TimeUnit.SECONDS).join();
            assertEquals(1, degraded(registry)); assertEquals(1, fallbackDecisions(registry));
            flow.tryAcquire("policy", io.quotaflow.core.RateLimitContext.empty());
            store.flushObservations().toCompletableFuture().orTimeout(2, java.util.concurrent.TimeUnit.SECONDS).join();
            assertEquals(2, fallbackDecisions(registry));
            primary.available = true;
            await(() -> degraded(registry) == 0);
            assertEquals(DegradationState.CLOSED, store.state());
            assertEquals(2, fallbackDecisions(registry), "control probes are not business decisions");
        }
    }
    private static void await(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(5);
        org.junit.jupiter.api.Assertions.assertTrue(condition.getAsBoolean());
    }

    private static double degraded(SimpleMeterRegistry registry) {
        return registry.get(QuotaFlowMetrics.DEGRADED).gauge().value();
    }

    private static double fallbackDecisions(SimpleMeterRegistry registry) {
        var counter = registry.find(QuotaFlowMetrics.FALLBACK_DECISIONS)
                .tags("policy", "policy", "key-group", "global")
                .counter();
        return counter == null ? 0.0 : counter.count();
    }
}
