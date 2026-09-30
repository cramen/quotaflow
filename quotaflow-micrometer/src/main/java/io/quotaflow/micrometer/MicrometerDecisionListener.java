package io.quotaflow.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.quotaflow.core.*;
import io.quotaflow.core.observation.*;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Terminal and queue counters plus policy-only gauges from immutable evaluated budgets.
 * Adding this listener to DefaultQuotaFlow automatically opens an owned observation session.
 * Direct legacy callbacks report counters/timers only: no capacity is reconstructed or resolved.
 */
public final class MicrometerDecisionListener implements DecisionListener, WaitListener, ObservationListener, AutoCloseable {
    private final SharedMeters meters;
    private final Object manualOwner = new Object();
    private boolean closed;
    public MicrometerDecisionListener(MeterRegistry registry) { meters=SharedMeters.of(Objects.requireNonNull(registry,"registry")); }
    /** @deprecated Capacities must accompany evaluated outcomes; the supplied resolver is never invoked. */
    @Deprecated
    public MicrometerDecisionListener(MeterRegistry registry, CapacityResolver capacities) {
        this(registry); Objects.requireNonNull(capacities,"capacityResolver");
    }
    /** Source-compatible factory; observation sessions replace later policy lookups. */
    public static MicrometerDecisionListener withStaticLimits(MeterRegistry registry,Supplier<PolicySet> policies) {
        Objects.requireNonNull(policies,"policySets"); return new MicrometerDecisionListener(registry);
    }
    /** Source-compatible factory; metric delivery never invokes the tariff resolver. */
    public static MicrometerDecisionListener withLimitResolver(MeterRegistry registry,Supplier<PolicySet> policies,LimitResolver resolver) {
        Objects.requireNonNull(policies,"policySets"); Objects.requireNonNull(resolver,"limitResolver"); return new MicrometerDecisionListener(registry);
    }
    @Override public synchronized ObservationSession open(ObservationConfiguration initial) {
        if(closed) throw new IllegalStateException("metric listener is closed");
        return meters.open(initial);
    }
    @Override public synchronized void onDecision(Decision decision,String group) {
        if(!closed) meters.decision(manualOwner,decision,group);
    }
    @Override public synchronized void onQueued(String policy,String group) {
        if(!closed) meters.queued(manualOwner,policy,group);
    }
    /** Closes manual callback ownership; sessions returned by open are owned by their facades. */
    @Override public synchronized void close() { if(!closed) { closed=true; meters.release(manualOwner); } }
}
