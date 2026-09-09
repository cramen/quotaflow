package io.quotaflow.micrometer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quotaflow.core.Decision;
import io.quotaflow.core.DecisionListener;
import io.quotaflow.core.LimitResolver;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.RateLimitPolicy;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Bridges every limiter decision to Micrometer: a
 * {@code quotaflow.decisions} counter tagged with result (allow|reject),
 * policy and key-group, and a last-value {@code quotaflow.utilization} gauge
 * (0..1) per policy and key-group.
 *
 * <p>Utilization is derived from the decision stream alone —
 * {@code 1 - remaining / capacity}, where {@code remaining} already travels
 * in every {@link Decision} and capacity comes from the supplied
 * {@link CapacityResolver} — so the bridge adds zero store round-trips. The
 * gauge is a last-value gauge: a key-group with no traffic keeps its last
 * known utilization rather than expiring.
 *
 * <p>Cardinality is bounded by construction: the listener signature carries
 * the key-group only, so raw limit keys can never become metric tags.
 */
public final class MicrometerDecisionListener implements DecisionListener {

    private final MeterRegistry registry;
    private final CapacityResolver capacityResolver;
    private final ConcurrentHashMap<GaugeKey, AtomicLong> utilization = new ConcurrentHashMap<>();

    public MicrometerDecisionListener(MeterRegistry registry, CapacityResolver capacityResolver) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.capacityResolver = Objects.requireNonNull(capacityResolver, "capacityResolver");
    }

    /**
     * Bridge for policy sets with static limits only; the policy set is
     * re-read through {@code policySets} on every decision, so an atomically
     * replaced set takes effect without re-registering the listener.
     * Policies declaring a dynamic limit reference report no utilization.
     */
    public static MicrometerDecisionListener withStaticLimits(
            MeterRegistry registry, Supplier<PolicySet> policySets) {
        Objects.requireNonNull(policySets, "policySets");
        return new MicrometerDecisionListener(registry, (policyId, keyGroup) -> {
            RateLimitPolicy policy = policySets.get().policy(policyId);
            return policy.limit()
                    .map(limit -> OptionalLong.of(limit.capacity()))
                    .orElseGet(OptionalLong::empty);
        });
    }

    /**
     * Bridge resolving capacity for both static limits and dynamic limit
     * references through the given {@link LimitResolver}. The resolver is
     * expected to be a caching wrapper, exactly as in engine wiring.
     */
    public static MicrometerDecisionListener withLimitResolver(
            MeterRegistry registry, Supplier<PolicySet> policySets, LimitResolver limitResolver) {
        Objects.requireNonNull(policySets, "policySets");
        Objects.requireNonNull(limitResolver, "limitResolver");
        return new MicrometerDecisionListener(registry, (policyId, keyGroup) -> {
            RateLimitPolicy policy = policySets.get().policy(policyId);
            if (policy.limit().isPresent()) {
                return OptionalLong.of(policy.limit().orElseThrow().capacity());
            }
            return limitResolver
                    .resolve(policy.limitRef().orElseThrow(), keyGroup)
                    .map(limit -> OptionalLong.of(limit.capacity()))
                    .orElseGet(OptionalLong::empty);
        });
    }

    @Override
    public void onDecision(Decision decision, String keyGroup) {
        String result = decision.isAllowed() ? QuotaFlowMetrics.RESULT_ALLOW : QuotaFlowMetrics.RESULT_REJECT;
        registry.counter(
                        QuotaFlowMetrics.DECISIONS,
                        QuotaFlowMetrics.TAG_RESULT, result,
                        QuotaFlowMetrics.TAG_POLICY, decision.policyId(),
                        QuotaFlowMetrics.TAG_KEY_GROUP, keyGroup)
                .increment();
        OptionalLong capacity = capacityResolver.capacity(decision.policyId(), keyGroup);
        if (capacity.isEmpty() || capacity.orElseThrow() < 1) {
            return;
        }
        double value = 1.0 - (double) decision.remaining() / capacity.orElseThrow();
        double clamped = Math.max(0.0, Math.min(1.0, value));
        utilization
                .computeIfAbsent(new GaugeKey(decision.policyId(), keyGroup), this::registerGauge)
                .set(Double.doubleToRawLongBits(clamped));
    }

    private AtomicLong registerGauge(GaugeKey key) {
        AtomicLong holder = new AtomicLong();
        Gauge.builder(QuotaFlowMetrics.UTILIZATION, holder, bits -> Double.longBitsToDouble(bits.get()))
                .tags(QuotaFlowMetrics.TAG_POLICY, key.policyId(), QuotaFlowMetrics.TAG_KEY_GROUP, key.keyGroup())
                .register(registry);
        return holder;
    }

    private record GaugeKey(String policyId, String keyGroup) {
    }
}
