package io.quotaflow.micrometer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quotaflow.core.Verdict;
import io.quotaflow.fallback.DegradationListener;
import io.quotaflow.fallback.DegradationState;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bridges degradation events to Micrometer: a {@code quotaflow.degraded}
 * gauge holding 1 while the primary store is not trusted ({@code OPEN} or
 * {@code HALF_OPEN} — during probing all non-probe traffic is still served
 * locally) and 0 while {@code CLOSED}, plus a
 * {@code quotaflow.fallback.decisions} counter tagged with policy and
 * key-group.
 *
 * <p>Register one instance per {@code FallbackRateLimitStore}; the single-
 * store case is the norm and yields exactly one gauge per registry.
 */
public final class MicrometerDegradationListener implements DegradationListener {

    private final MeterRegistry registry;
    private final AtomicInteger degraded = new AtomicInteger(0);

    public MicrometerDegradationListener(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
        Gauge.builder(QuotaFlowMetrics.DEGRADED, degraded, AtomicInteger::get)
                .description("1 while the distributed store is degraded and decisions are served locally")
                .register(registry);
    }

    @Override
    public void onTransition(DegradationState from, DegradationState to, String reason) {
        degraded.set(to == DegradationState.CLOSED ? 0 : 1);
    }

    @Override
    public void onFallbackDecision(String policyId, String keyGroup, Verdict verdict) {
        registry.counter(
                        QuotaFlowMetrics.FALLBACK_DECISIONS,
                        QuotaFlowMetrics.TAG_POLICY, policyId,
                        QuotaFlowMetrics.TAG_KEY_GROUP, keyGroup)
                .increment();
    }
}
