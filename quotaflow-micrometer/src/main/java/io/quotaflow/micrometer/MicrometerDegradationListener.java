package io.quotaflow.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.quotaflow.core.Verdict;
import io.quotaflow.fallback.*;
import java.util.*;

/** One owner per fallback store. Shared registries report degraded when any live owner is degraded. */
public final class MicrometerDegradationListener implements DegradationListener, AutoCloseable {
    private final SharedMeters meters;
    private final Object stateOwner = new Object(), manualOwner = new Object();
    private final Map<Long,Object> generations = new HashMap<>();
    private boolean closed;
    public MicrometerDegradationListener(MeterRegistry registry) {
        meters=SharedMeters.of(Objects.requireNonNull(registry,"registry"));
        meters.degradation(stateOwner,1);
    }
    @Override public synchronized void onTransition(DegradationState from,DegradationState to,String reason) {
        if(!closed) meters.degradation(stateOwner,to==DegradationState.CLOSED?0:1);
    }
    @Override public synchronized void onConfiguration(long revision,Set<String> policies) {
        if(!closed) generations.computeIfAbsent(revision,ignored->new Object());
    }
    @Override public synchronized void onFallbackDecision(long revision,boolean currentTarget,String policy,String group,Verdict verdict) {
        if(closed) return;
        var owner=generations.get(revision); if(owner==null) return;
        if(currentTarget) meters.fallback(owner,policy,group);
        else {
            var transientOwner=new Object();
            try { meters.fallback(transientOwner,policy,group); }
            finally { meters.release(transientOwner); }
        }
    }
    @Override public synchronized void onFallbackDecision(String policy,String group,Verdict verdict) {
        if(!closed) meters.fallback(manualOwner,policy,group);
    }
    @Override public synchronized void onRetired(long revision) {
        var owner=generations.remove(revision); if(owner!=null) meters.release(owner);
    }
    @Override public void onClosed() { close(); }
    @Override public synchronized void close() {
        if(closed) return; closed=true;
        for(var owner:generations.values()) meters.release(owner);
        generations.clear(); meters.release(manualOwner); meters.removeDegradation(stateOwner);
    }
}
