package io.quotaflow.micrometer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.RateLimitPolicy;
import io.quotaflow.core.Reaction;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Exposes the facade's throttle waiter queues as a
 * {@code quotaflow.wait.queue.depth} gauge per throttle policy. Gauges poll
 * {@link DefaultQuotaFlow#waitQueueDepth(String)}, so they track both the
 * accumulation of waiters and the drain with no instrumentation on the wait
 * path. The queue is per policy (all key-groups share it), hence the policy
 * tag only.
 */
public final class MicrometerThrottleMetrics {

    private final MeterRegistry registry;
    private final DefaultQuotaFlow flow;
    private final Supplier<PolicySet> policySets;
    private final Set<String> registered = ConcurrentHashMap.newKeySet();

    /**
     * Registers depth gauges for the throttle policies of the current set.
     * The set is re-read through {@code policySets} on every {@link #sync()}.
     */
    public MicrometerThrottleMetrics(
            MeterRegistry registry, DefaultQuotaFlow flow, Supplier<PolicySet> policySets) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.flow = Objects.requireNonNull(flow, "flow");
        this.policySets = Objects.requireNonNull(policySets, "policySets");
        sync();
    }

    /**
     * Registers gauges for throttle policies that appeared since the last
     * sync (already-registered policies are skipped). Call it after a
     * configuration reload swaps the policy set.
     */
    public void sync() {
        for (RateLimitPolicy policy : policySets.get().policies()) {
            if (policy.reaction() == Reaction.THROTTLE && registered.add(policy.id())) {
                Gauge.builder(QuotaFlowMetrics.WAIT_QUEUE_DEPTH, flow,
                                quotaFlow -> quotaFlow.waitQueueDepth(policy.id()))
                        .tags(QuotaFlowMetrics.TAG_POLICY, policy.id())
                        .register(registry);
            }
        }
    }
}
