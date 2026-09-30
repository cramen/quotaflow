package io.quotaflow.micrometer;

import io.micrometer.core.instrument.*;
import io.quotaflow.core.Decision;
import io.quotaflow.core.ThrottleRejection;
import io.quotaflow.core.observation.*;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;

/** Registry-scoped ownership. Internal tokens never become metric tags. */
final class SharedMeters {
    private static final Map<MeterRegistry, WeakReference<SharedMeters>> REGISTRIES = new WeakHashMap<>();
    static synchronized SharedMeters of(MeterRegistry registry) {
        var reference = REGISTRIES.get(registry);
        var existing = reference == null ? null : reference.get();
        if (existing != null) return existing;
        var created = new SharedMeters(registry); REGISTRIES.put(registry, new WeakReference<>(created)); return created;
    }
    private final MeterRegistry registry;
    private final Map<Meter.Id, OwnedMeter> meters = new HashMap<>();
    private final Map<String, BudgetGauge> budgets = new HashMap<>();
    private final Map<String, DepthGauge> depths = new HashMap<>();
    private final Map<Object,Integer> degradation = new IdentityHashMap<>();
    private final java.util.concurrent.atomic.AtomicInteger degraded = new java.util.concurrent.atomic.AtomicInteger();
    private Meter degradedMeter;
    private record OwnedMeter(Meter meter, Set<Object> owners, boolean created) { }
    private record Sample(long sequence, long revision, io.quotaflow.core.store.StoreBudget budget) { }
    private record Group(Object owner, String group) { }
    private static final class BudgetGauge {
        final Map<Group,Sample> samples = new HashMap<>();
        final AtomicReference<double[]> value = new AtomicReference<>(new double[]{Double.NaN,Double.NaN});
        Meter remaining, utilization;
        void recompute() {
            double minimum = Double.POSITIVE_INFINITY, maximum = 0;
            for (Sample sample : samples.values()) {
                var budget = sample.budget(); minimum = Math.min(minimum,budget.remaining());
                maximum = Math.max(maximum, budget.capacity() == 0 ? 1 : 1 - (double)budget.remaining()/budget.capacity());
            }
            value.set(samples.isEmpty() ? new double[]{Double.NaN,Double.NaN} : new double[]{minimum, Math.max(0,Math.min(1,maximum))});
        }
    }
    private static final class DepthGauge {
        final Map<Object,DoubleSupplier> owners = new IdentityHashMap<>();
        volatile List<DoubleSupplier> sources = List.of();
        Meter meter;
        double value() { double total = 0; for(var source:sources) total += Math.max(0,source.getAsDouble()); return total; }
    }
    private SharedMeters(MeterRegistry registry) { this.registry=Objects.requireNonNull(registry,"registry"); }
    synchronized Session open(ObservationConfiguration initial) { return new Session(initial); }

    final class Session implements ObservationSession {
        private final Map<Long,Generation> generations = new HashMap<>();
        private long active;
        private boolean closed;
        private final Object budgetOwner = new Object();
        private final Map<String,Long> revisions = new HashMap<>();
        private final class Generation {
            final ObservationConfiguration configuration;
            final Object counterOwner = new Object();
            Generation(ObservationConfiguration configuration) { this.configuration = configuration; }
        }
        private Session(ObservationConfiguration initial) { active=initial.generation(); generations.put(active,new Generation(initial)); }
        @Override public void onConfiguration(ObservationConfiguration configuration) {
            synchronized(SharedMeters.this) {
                if(closed || configuration.generation() <= active) return;
                removeBudgets(budgetOwner,null); revisions.clear();
                active=configuration.generation(); generations.put(active,new Generation(configuration));
            }
        }
        @Override public void onBudget(BudgetObservation observation) {
            synchronized(SharedMeters.this) {
                if(closed || observation.generation()!=active) return;
                var generation=generations.get(active); if(generation==null) return;
                long previous=revisions.getOrDefault(observation.rootPolicyId(),-1L);
                if(observation.resolverRevision()<previous) return;
                if(observation.resolverRevision()>previous) {
                    var affected = new HashSet<String>();
                    generation.configuration.policyRoots().forEach((policy,root)-> { if(root.equals(observation.rootPolicyId())) affected.add(policy); });
                    removeBudgets(budgetOwner,affected); revisions.put(observation.rootPolicyId(),observation.resolverRevision());
                }
                for(var sample:observation.samples()) {
                    if(!Objects.equals(generation.configuration.policyRoots().get(sample.policyId()),observation.rootPolicyId())) continue;
                    budget(budgetOwner,sample,observation.sequence(),observation.resolverRevision());
                }
            }
        }
        @Override public void onDecision(long generation,Decision decision,String group) {
            synchronized(SharedMeters.this) {
                var owner=generations.get(generation);
                if(closed || owner==null || !owner.configuration.policyRoots().containsKey(decision.policyId())) return;
                decision(owner.counterOwner,decision,group);
            }
        }
        @Override public void onQueued(long generation,String policy,String group) {
            synchronized(SharedMeters.this) {
                var owner=generations.get(generation);
                if(closed || owner==null || !owner.configuration.policyRoots().containsKey(policy)) return;
                counter(owner.counterOwner,QuotaFlowMetrics.DECISIONS,Tags.of("result","wait","policy",policy,"key-group",group)).increment();
            }
        }
        @Override public void onRetired(long generation) {
            synchronized(SharedMeters.this) {
                var old=generations.remove(generation); if(old!=null) release(old.counterOwner);
                if(generation==active) { removeBudgets(budgetOwner,null); revisions.clear(); }
            }
        }
        @Override public void close() {
            synchronized(SharedMeters.this) {
                if(closed) return; closed=true;
                for(var generation:generations.values()) release(generation.counterOwner);
                generations.clear(); revisions.clear(); removeBudgets(budgetOwner,null);
            }
        }
    }
    synchronized void decision(Object owner,Decision decision,String group) {
        var tags=Tags.of("policy",decision.policyId(),"key-group",group);
        counter(owner,QuotaFlowMetrics.DECISIONS,tags.and("result",decision.isAllowed()?"allow":"reject")).increment();
        timer(owner,QuotaFlowMetrics.WAIT_DURATION,tags).record(decision.waitDuration());
        if(decision.throttleRejection().orElse(null)==ThrottleRejection.WAIT_TIMEOUT) counter(owner,QuotaFlowMetrics.WAIT_TIMEOUTS,tags).increment();
    }
    synchronized void queued(Object owner,String policy,String group) {
        counter(owner,QuotaFlowMetrics.DECISIONS,Tags.of("result","wait","policy",policy,"key-group",group)).increment();
    }
    private Counter counter(Object owner,String name,Tags tags) {
        boolean existing=registry.find(name).tags(tags).counter()!=null;
        return own(owner,registry.counter(name,tags),!existing);
    }
    private io.micrometer.core.instrument.Timer timer(Object owner,String name,Tags tags) {
        boolean existing=registry.find(name).tags(tags).timer()!=null;
        return own(owner,registry.timer(name,tags),!existing);
    }
    private <T extends Meter> T own(Object owner,T meter,boolean created) {
        var owned=meters.get(meter.getId());
        if(owned==null || owned.meter()!=meter) {
            var references=Collections.newSetFromMap(new IdentityHashMap<Object,Boolean>());
            if(owned!=null) references.addAll(owned.owners());
            owned=new OwnedMeter(meter,references,created); meters.put(meter.getId(),owned);
        }
        owned.owners().add(owner); return meter;
    }
    synchronized void release(Object owner) {
        var iterator=meters.entrySet().iterator();
        while(iterator.hasNext()) {
            var owned=iterator.next().getValue();
            if(owned.owners().remove(owner) && owned.owners().isEmpty()) {
                if(owned.created()) removeExact(owned.meter()); iterator.remove();
            }
        }
    }
    private void budget(Object owner,BudgetSample sample,long sequence,long revision) {
        var key=new Group(owner,sample.keyGroup());
        var gauge=budgets.get(sample.policyId());
        if(gauge==null) {
            var candidate=new BudgetGauge();
            candidate.samples.put(key,new Sample(sequence,revision,sample.budget())); candidate.recompute();
            var tags=Tags.of("policy",sample.policyId());
            boolean existingRemaining=registry.find(QuotaFlowMetrics.TOKENS_REMAINING).tags(tags).gauge()!=null;
            boolean existingUtilization=registry.find(QuotaFlowMetrics.UTILIZATION).tags(tags).gauge()!=null;
            candidate.remaining=Gauge.builder(QuotaFlowMetrics.TOKENS_REMAINING,candidate,value->value.value.get()[0]).tags(tags).register(registry);
            try { candidate.utilization=Gauge.builder(QuotaFlowMetrics.UTILIZATION,candidate,value->value.value.get()[1]).tags(tags).register(registry); }
            catch(RuntimeException failure) {
                if(!existingRemaining) removeExact(candidate.remaining); throw failure;
            }
            own(owner,candidate.remaining,!existingRemaining); own(owner,candidate.utilization,!existingUtilization);
            budgets.put(sample.policyId(),candidate); return;
        }
        var old=gauge.samples.get(key);
        if(old!=null && old.sequence()>sequence) return;
        gauge.samples.put(key,new Sample(sequence,revision,sample.budget())); gauge.recompute();
        own(owner,gauge.remaining,false); own(owner,gauge.utilization,false);
    }
    private void removeExact(Meter meter) {
        if(registry.getMeters().stream().anyMatch(current->current==meter)) registry.remove(meter);
    }
    private void removeBudgets(Object owner,Set<String> policies) {
        var iterator=budgets.entrySet().iterator();
        while(iterator.hasNext()) {
            var entry=iterator.next(); if(policies!=null && !policies.contains(entry.getKey())) continue;
            var gauge=entry.getValue();
            gauge.samples.keySet().removeIf(key->key.owner()==owner); gauge.recompute();
            releaseMeter(owner,gauge.remaining); releaseMeter(owner,gauge.utilization);
            if(gauge.samples.isEmpty()) iterator.remove();
        }
    }
    private void releaseMeter(Object owner,Meter meter) {
        if(meter==null) return;
        var owned=meters.get(meter.getId());
        if(owned!=null && owned.owners().remove(owner) && owned.owners().isEmpty()) {
            if(owned.created()) removeExact(meter); meters.remove(meter.getId());
        }
    }
    synchronized void degradation(Object owner,int value) {
        degradation.put(owner,value); degraded.set(degradation.values().stream().mapToInt(Integer::intValue).max().orElse(0));
        if(degradedMeter==null) {
            boolean existing=registry.find(QuotaFlowMetrics.DEGRADED).gauge()!=null;
            degradedMeter=own(owner,Gauge.builder(QuotaFlowMetrics.DEGRADED,degraded,java.util.concurrent.atomic.AtomicInteger::get).register(registry),!existing);
        } else own(owner,degradedMeter,false);
    }
    synchronized void removeDegradation(Object owner) {
        degradation.remove(owner); degraded.set(degradation.values().stream().mapToInt(Integer::intValue).max().orElse(0));
        releaseMeter(owner,degradedMeter); if(degradation.isEmpty()) degradedMeter=null;
    }
    synchronized void fallback(Object owner,String policy,String group) {
        counter(owner,QuotaFlowMetrics.FALLBACK_DECISIONS,Tags.of("policy",policy,"key-group",group)).increment();
    }
    synchronized void depth(Object owner,String policy,DoubleSupplier source) {
        var gauge=depths.computeIfAbsent(policy,ignored->new DepthGauge());
        gauge.owners.put(owner,source); gauge.sources=List.copyOf(gauge.owners.values());
        var tags=Tags.of("policy",policy);
        if(gauge.meter==null) {
            boolean existing=registry.find(QuotaFlowMetrics.WAIT_QUEUE_DEPTH).tags(tags).gauge()!=null;
            gauge.meter=own(owner,Gauge.builder(QuotaFlowMetrics.WAIT_QUEUE_DEPTH,gauge,DepthGauge::value).tags(tags).register(registry),!existing);
        } else own(owner,gauge.meter,false);
    }
    synchronized void removeDepth(Object owner,String policy) {
        var gauge=depths.get(policy); if(gauge==null)return;
        gauge.owners.remove(owner); gauge.sources=List.copyOf(gauge.owners.values()); releaseMeter(owner,gauge.meter);
        if(gauge.owners.isEmpty()) depths.remove(policy);
    }
}
