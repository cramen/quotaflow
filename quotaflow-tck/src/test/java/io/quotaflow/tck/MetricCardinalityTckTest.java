package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.Limit;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.RateLimitContext;
import io.quotaflow.core.RateLimitPolicy;
import io.quotaflow.core.Scope;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.micrometer.MicrometerDecisionListener;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Metric cardinality conformance: 10,000 distinct raw limit keys sharing a
 * bounded set of key-groups must produce a meter count bounded by policies
 * times key-groups times results — independent of the raw key count. The
 * second wave of 10,000 fresh keys must not add a single meter, and no raw
 * key may appear in any tag.
 */
class MetricCardinalityTckTest {

    private static final int DISTINCT_KEYS = 10_000;
    private static final int POLICIES = 3;
    private static final int KEY_GROUPS = 2;
    private static final int RESULTS = 2;
    private static final int METER_KINDS = 2; // decisions counter + utilization gauge

    @Test
    void tenThousandUniqueKeysDoNotExplodeMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PolicySet policies = PolicySet.compile(List.of(
                RateLimitPolicy.builder("global")
                        .limit(new Limit(100_000_000, 100_000_000, Duration.ofMinutes(1)))
                        .scope(Scope.GLOBAL)
                        .build(),
                RateLimitPolicy.builder("per-tenant")
                        .limit(new Limit(100_000_000, 100_000_000, Duration.ofMinutes(1)))
                        .scope(Scope.TENANT)
                        .parentId("global")
                        .build(),
                RateLimitPolicy.builder("limited")
                        .limit(new Limit(1, 1, Duration.ofHours(1)))
                        .scope(Scope.TENANT)
                        .build()));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore())
                .addListener(MicrometerDecisionListener.withStaticLimits(registry, () -> policies))
                .build();

        // wave 1: 10,000 distinct raw tenant keys, all sharing two key-groups
        for (int i = 0; i < DISTINCT_KEYS; i++) {
            flow.tryAcquire("per-tenant", tenant("tenant-" + i));
        }
        flow.tryAcquire("global", RateLimitContext.empty());
        // a third policy exercised into rejection (result dimension > 1)
        flow.tryAcquire("limited", tenant("acme"));
        flow.tryAcquire("limited", tenant("acme"));

        int metersAfterFirstWave = registry.getMeters().size();
        int bound = POLICIES * KEY_GROUPS * RESULTS * METER_KINDS;
        assertTrue(metersAfterFirstWave <= bound,
                "meter count " + metersAfterFirstWave + " exceeds the bound policies x key-groups"
                        + " x results x meter kinds = " + bound);

        // wave 2: 10,000 fresh raw keys must not add a single meter
        for (int i = DISTINCT_KEYS; i < 2 * DISTINCT_KEYS; i++) {
            flow.tryAcquire("per-tenant", tenant("tenant-" + i));
        }
        assertEquals(metersAfterFirstWave, registry.getMeters().size(),
                "fresh raw keys must not create new meters");

        // no raw key leaks into any tag
        Set<String> keyGroupValues = new HashSet<>();
        for (Meter meter : registry.getMeters()) {
            for (io.micrometer.core.instrument.Tag tag : meter.getId().getTags()) {
                assertTrue(Set.of("result", "policy", "key-group").contains(tag.getKey()),
                        "unexpected tag key '" + tag.getKey() + "' on " + meter.getId());
                if (tag.getKey().equals("key-group")) {
                    keyGroupValues.add(tag.getValue());
                }
            }
        }
        assertEquals(Set.of("global", "tenant"), keyGroupValues,
                "only aggregated key-group identities may appear in tags");
    }

    private static RateLimitContext tenant(String tenantId) {
        return RateLimitContext.builder().put(RateLimitContext.TENANT_ID, tenantId).build();
    }
}
