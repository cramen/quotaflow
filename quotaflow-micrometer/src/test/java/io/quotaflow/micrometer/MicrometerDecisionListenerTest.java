package io.quotaflow.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.Limit;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.RateLimitContext;
import io.quotaflow.core.RateLimitPolicy;
import io.quotaflow.core.Scope;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MicrometerDecisionListenerTest {

    private static RateLimitContext tenant(String tenantId) {
        return RateLimitContext.builder().put(RateLimitContext.TENANT_ID, tenantId).build();
    }

    @Test
    void decisionsAreMeteredByResultPolicyAndKeyGroup() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PolicySet policies = PolicySet.compile(List.of(
                RateLimitPolicy.builder("per-tenant")
                        .limit(new Limit(4, 1, Duration.ofHours(1)))
                        .scope(Scope.TENANT)
                        .build()));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore())
                .addListener(MicrometerDecisionListener.withStaticLimits(registry, () -> policies))
                .build();

        flow.tryAcquire("per-tenant", tenant("acme"));
        flow.tryAcquire("per-tenant", tenant("acme"));
        flow.tryAcquire("per-tenant", tenant("globex"));
        // drain the remaining tokens of acme, then reject
        flow.tryAcquire("per-tenant", tenant("acme"));
        flow.tryAcquire("per-tenant", tenant("acme"));
        flow.tryAcquire("per-tenant", tenant("acme"));

        Counter allowed = registry.get(QuotaFlowMetrics.DECISIONS)
                .tags("result", "allow", "policy", "per-tenant", "key-group", "tenant")
                .counter();
        Counter rejected = registry.get(QuotaFlowMetrics.DECISIONS)
                .tags("result", "reject", "policy", "per-tenant", "key-group", "tenant")
                .counter();
        assertEquals(5.0, allowed.count());
        assertEquals(1.0, rejected.count());
    }

    @Test
    void utilizationApproachesOneBeforeRejections() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PolicySet policies = PolicySet.compile(List.of(
                RateLimitPolicy.builder("per-tenant")
                        .limit(new Limit(4, 1, Duration.ofHours(1)))
                        .scope(Scope.TENANT)
                        .build()));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore())
                .addListener(MicrometerDecisionListener.withStaticLimits(registry, () -> policies))
                .build();

        flow.tryAcquire("per-tenant", tenant("acme"));
        assertEquals(0.25, utilization(registry), 1e-9);
        flow.tryAcquire("per-tenant", tenant("acme"));
        assertEquals(0.5, utilization(registry), 1e-9);
        flow.tryAcquire("per-tenant", tenant("acme"));
        flow.tryAcquire("per-tenant", tenant("acme"));

        // fully consumed but not yet rejecting: the gauge reads 1 while the
        // reject counter has not moved
        assertEquals(1.0, utilization(registry), 1e-9);
        assertNull(registry.find(QuotaFlowMetrics.DECISIONS).tags("result", "reject").counter());

        flow.tryAcquire("per-tenant", tenant("acme"));
        assertEquals(1.0, registry.get(QuotaFlowMetrics.DECISIONS)
                .tags("result", "reject").counter().count());
    }

    @Test
    void utilizationUsesResolvedCapacityForDynamicLimits() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PolicySet policies = PolicySet.compile(List.of(
                RateLimitPolicy.builder("dynamic")
                        .limitRef("plan")
                        .scope(Scope.TENANT)
                        .build()));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore())
                .limitResolver((limitRef, keyGroup) -> Optional.of(new Limit(10, 10, Duration.ofMinutes(1))))
                .addListener(MicrometerDecisionListener.withLimitResolver(
                        registry, () -> policies,
                        (limitRef, keyGroup) -> Optional.of(new Limit(10, 10, Duration.ofMinutes(1)))))
                .build();

        flow.tryAcquire("dynamic", tenant("acme"));

        assertEquals(0.1, registry.get(QuotaFlowMetrics.UTILIZATION)
                .tags("policy", "dynamic", "key-group", "tenant").gauge().value(), 1e-9);
    }

    @Test
    void unresolvableCapacityLeavesNoGaugeButStillCounts() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PolicySet policies = PolicySet.compile(List.of(
                RateLimitPolicy.builder("dynamic")
                        .limitRef("plan")
                        .scope(Scope.TENANT)
                        .build()));
        // the engine rejects when the limit reference cannot be resolved
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore())
                .limitResolver((limitRef, keyGroup) -> Optional.empty())
                .addListener(MicrometerDecisionListener.withLimitResolver(
                        registry, () -> policies, (limitRef, keyGroup) -> Optional.empty()))
                .build();

        flow.tryAcquire("dynamic", tenant("acme"));

        assertEquals(1.0, registry.get(QuotaFlowMetrics.DECISIONS)
                .tags("result", "reject", "policy", "dynamic", "key-group", "tenant").counter().count());
        assertNull(registry.find(QuotaFlowMetrics.UTILIZATION).gauge());
    }

    private static double utilization(SimpleMeterRegistry registry) {
        return registry.get(QuotaFlowMetrics.UTILIZATION)
                .tags("policy", "per-tenant", "key-group", "tenant")
                .gauge()
                .value();
    }

    @Test
    void utilizationFollowsAnAtomicallyReplacedPolicySet() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        java.util.concurrent.atomic.AtomicReference<PolicySet> current = new java.util.concurrent.atomic.AtomicReference<>(
                PolicySet.compile(List.of(
                        RateLimitPolicy.builder("per-tenant")
                                .limit(new Limit(100, 1, Duration.ofHours(1)))
                                .scope(Scope.TENANT)
                                .build())));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(current.get(), new LocalRateLimitStore())
                .addListener(MicrometerDecisionListener.withStaticLimits(registry, current::get))
                .build();

        PolicySet tightened = PolicySet.compile(List.of(
                RateLimitPolicy.builder("per-tenant")
                        .limit(new Limit(10, 1, Duration.ofHours(1)))
                        .scope(Scope.TENANT)
                        .build()));
        flow.replacePolicySet(tightened);
        current.set(tightened);

        flow.tryAcquire("per-tenant", tenant("acme"));

        // remaining 9 against the new capacity 10, not the old capacity 100
        assertEquals(0.1, utilization(registry), 1e-9);
        assertNotNull(flow);
    }
}
