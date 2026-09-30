package io.quotaflow.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.quotaflow.core.*;
import java.util.*;
import java.util.function.Supplier;

/** Shared policy queue gauges. Call sync after publication, and close when this adapter retires. */
public final class MicrometerThrottleMetrics implements AutoCloseable {
    private final SharedMeters meters;
    private DefaultQuotaFlow flow;
    private Supplier<PolicySet> policySets;
    private final Object owner = new Object();
    private final Set<String> registered = new HashSet<>();
    public MicrometerThrottleMetrics(MeterRegistry registry,DefaultQuotaFlow flow,Supplier<PolicySet> policies) {
        meters=SharedMeters.of(Objects.requireNonNull(registry,"registry"));
        this.flow=Objects.requireNonNull(flow,"flow"); policySets=Objects.requireNonNull(policies,"policySets"); sync();
    }
    public synchronized void sync() {
        if(flow==null) return;
        var active=new HashSet<String>();
        for(var policy:policySets.get().policies()) if(policy.reaction()==Reaction.THROTTLE) active.add(policy.id());
        for(String policy:List.copyOf(registered)) if(!active.contains(policy)) { meters.removeDepth(owner,policy); registered.remove(policy); }
        var source=flow;
        for(String policy:active) if(registered.add(policy)) meters.depth(owner,policy,()->source.waitQueueDepth(policy));
    }
    @Override public synchronized void close() {
        for(String policy:registered) meters.removeDepth(owner,policy);
        registered.clear(); flow=null; policySets=null;
    }
}
