package io.quotaflow.micrometer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quotaflow.core.*;
import io.quotaflow.core.observation.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ObservationOwnershipTest {
    static RateLimitPolicy policy(String id,long capacity) { return RateLimitPolicy.builder(id).scope(Scope.GLOBAL).limit(new Limit(capacity,1,Duration.ofHours(1))).build(); }
    static void drain(DefaultQuotaFlow flow) { flow.flushObservations().toCompletableFuture().orTimeout(3,TimeUnit.SECONDS).join(); }
    static ObservationConfiguration config(long generation,String policy) { return new ObservationConfiguration(generation,Map.of(policy,policy),Set.of()); }
    static BudgetObservation sample(long generation,long sequence,long revision,String policy,String group,long capacity,long remaining,boolean local) {
        return new BudgetObservation(generation,sequence,policy,revision,List.of(new BudgetSample(policy,group,Algorithm.TOKEN_BUCKET,new StoreBudget(capacity,remaining,local))));
    }
    static double gauge(SimpleMeterRegistry registry,String name,String policy) { return registry.get(name).tag("policy",policy).gauge().value(); }
    @Test void unequalHierarchyBudgetsAreNotReconstructedFromChainMinimum() {
        var registry=new SimpleMeterRegistry();var parent=policy("parent",3);
        var leaf=RateLimitPolicy.builder("leaf").scope(Scope.USER).parentId("parent").defaultKey("user").limit(new Limit(10,1,Duration.ofHours(1))).build();
        try(var flow=DefaultQuotaFlow.builder(PolicySet.compile(List.of(parent,leaf)),new LocalRateLimitStore())
                .addListener(new MicrometerDecisionListener(registry)).build()) {
            assertEquals(2,flow.tryAcquire("leaf",RateLimitContext.empty()).remaining()); drain(flow);
            assertEquals(2,gauge(registry,QuotaFlowMetrics.TOKENS_REMAINING,"parent"));
            assertEquals(9,gauge(registry,QuotaFlowMetrics.TOKENS_REMAINING,"leaf"));
            assertEquals(0.1,gauge(registry,QuotaFlowMetrics.UTILIZATION,"leaf"),1e-9);
            assertEquals(1,registry.get(QuotaFlowMetrics.UTILIZATION).tag("policy","leaf").gauge().getId().getTags().size());
        }
    }
    @Test void noMetricCallbackQueriesCurrentPolicyOrTariff() {
        var registry=new SimpleMeterRegistry();var lookups=new AtomicInteger();var value=new AtomicReference<>(new Limit(10,1,Duration.ofHours(1)));
        LimitResolver resolver=(ref,group)->{lookups.incrementAndGet();return Optional.of(value.get());};
        var policies=PolicySet.compile(List.of(RateLimitPolicy.builder("p").scope(Scope.GLOBAL).limitRef("plan").build()));
        var bridge=MicrometerDecisionListener.withLimitResolver(registry,()->{throw new AssertionError("late policy lookup");},(ref,group)->{throw new AssertionError("metric tariff lookup");});
        try(var flow=DefaultQuotaFlow.builder(policies,new LocalRateLimitStore()).limitResolver(resolver).addListener(bridge).build()) {
            flow.tryAcquire("p",RateLimitContext.empty());value.set(new Limit(1,1,Duration.ofHours(1)));drain(flow);
            assertEquals(1,lookups.get());assertEquals(0.1,gauge(registry,QuotaFlowMetrics.UTILIZATION,"p"),1e-9);
        }
    }
    @Test void unavailableLegacyBudgetsDoNotInventGauges() {
        var registry=new SimpleMeterRegistry();var bridge=new MicrometerDecisionListener(registry,(p,g)->{throw new AssertionError("capacity reconstruction");});
        bridge.onDecision(Decision.allowed("p",Scope.GLOBAL,3),"global");
        assertNull(registry.find(QuotaFlowMetrics.UTILIZATION).gauge());assertNull(registry.find(QuotaFlowMetrics.TOKENS_REMAINING).gauge());
        assertEquals(1,registry.get(QuotaFlowMetrics.DECISIONS).counter().count());bridge.close();
    }
    @Test void oldGenerationsAndProviderViewsCannotResurrectGauges() {
        var registry=new SimpleMeterRegistry();var bridge=new MicrometerDecisionListener(registry);
        try(var session=bridge.open(config(0,"p"))) {
            session.onBudget(sample(0,1,1,"p","gold",10,8,false));
            session.onBudget(sample(0,2,1,"p","silver",10,3,false));
            assertEquals(3,gauge(registry,QuotaFlowMetrics.TOKENS_REMAINING,"p"));
            session.onBudget(sample(0,3,2,"p","gold",2,1,false));
            assertEquals(1,gauge(registry,QuotaFlowMetrics.TOKENS_REMAINING,"p"));
            session.onBudget(sample(0,100,1,"p","silver",10,10,false));
            assertEquals(1,gauge(registry,QuotaFlowMetrics.TOKENS_REMAINING,"p"));
            session.onConfiguration(config(1,"q"));assertNull(registry.find(QuotaFlowMetrics.UTILIZATION).gauge());
            session.onBudget(sample(0,101,2,"p","gold",2,2,false));assertNull(registry.find(QuotaFlowMetrics.UTILIZATION).gauge());
            session.onDecision(0,Decision.allowed("p",Scope.GLOBAL,1),"gold");assertEquals(1,registry.get(QuotaFlowMetrics.DECISIONS).counter().count());
            session.onRetired(0);assertNull(registry.find(QuotaFlowMetrics.DECISIONS).counter());
            session.onBudget(sample(1,102,0,"q","global",0,0,true));
            assertEquals(0,gauge(registry,QuotaFlowMetrics.TOKENS_REMAINING,"q"));assertEquals(1,gauge(registry,QuotaFlowMetrics.UTILIZATION,"q"));
        }
        assertTrue(registry.getMeters().isEmpty());
    }
    @Test void retiringOneOwnerPreservesTheOtherOwnerAndForeignMeters() {
        var registry=new SimpleMeterRegistry();var foreign=registry.counter("application.counter");
        var bridge=new MicrometerDecisionListener(registry);
        var first=bridge.open(config(0,"p"));var second=bridge.open(config(0,"p"));
        first.onBudget(sample(0,1,0,"p","global",10,2,false));second.onBudget(sample(0,1,0,"p","global",10,8,false));
        first.onDecision(0,Decision.allowed("p",Scope.GLOBAL,2),"global");second.onDecision(0,Decision.allowed("p",Scope.GLOBAL,8),"global");
        assertEquals(2,gauge(registry,QuotaFlowMetrics.TOKENS_REMAINING,"p")); first.close();
        assertEquals(8,gauge(registry,QuotaFlowMetrics.TOKENS_REMAINING,"p"));assertEquals(2,registry.get(QuotaFlowMetrics.DECISIONS).counter().count());
        second.close();assertEquals(List.of(foreign),registry.getMeters());
    }
    @Test void sharedQueueDepthAndDegradationRetireOnlyTheirOwnContributions() {
        var registry=new SimpleMeterRegistry();var policies=PolicySet.compile(List.of(RateLimitPolicy.builder("p").scope(Scope.GLOBAL).reaction(Reaction.THROTTLE).limit(new Limit(1,1,Duration.ofHours(1))).build()));
        try(var a=DefaultQuotaFlow.builder(policies,new LocalRateLimitStore()).build();var b=DefaultQuotaFlow.builder(policies,new LocalRateLimitStore()).build()) {
            var first=new MicrometerThrottleMetrics(registry,a,a::policySet);var second=new MicrometerThrottleMetrics(registry,b,b::policySet);
            first.close();assertNotNull(registry.find(QuotaFlowMetrics.WAIT_QUEUE_DEPTH).gauge());second.close();assertNull(registry.find(QuotaFlowMetrics.WAIT_QUEUE_DEPTH).gauge());
        }
        var first=new MicrometerDegradationListener(registry);var second=new MicrometerDegradationListener(registry);
        first.onTransition(io.quotaflow.fallback.DegradationState.OPEN,io.quotaflow.fallback.DegradationState.CLOSED,"healthy");
        assertEquals(1,registry.get(QuotaFlowMetrics.DEGRADED).gauge().value());second.close();
        assertEquals(0,registry.get(QuotaFlowMetrics.DEGRADED).gauge().value());first.close();assertNull(registry.find(QuotaFlowMetrics.DEGRADED).gauge());
    }
    @Test void registrationFailureDoesNotPoisonAnotherOwner() {
        var registry=new SimpleMeterRegistry();var fail=new AtomicBoolean(true);
        registry.config().meterFilter(new io.micrometer.core.instrument.config.MeterFilter(){
            @Override public io.micrometer.core.instrument.Meter.Id map(io.micrometer.core.instrument.Meter.Id id){
                if(id.getName().equals(QuotaFlowMetrics.UTILIZATION)&&fail.getAndSet(false))throw new IllegalStateException("injected");return id;
            }
        });
        var bridge=new MicrometerDecisionListener(registry);
        try(var first=bridge.open(config(0,"p"));var second=bridge.open(config(0,"p"))) {
            assertThrows(IllegalStateException.class,()->first.onBudget(sample(0,1,0,"p","global",10,1,false)));
            assertNull(registry.find(QuotaFlowMetrics.TOKENS_REMAINING).gauge());
            second.onBudget(sample(0,2,0,"p","global",10,5,false));assertEquals(5,gauge(registry,QuotaFlowMetrics.TOKENS_REMAINING,"p"));
        }
    }

    @Test void tenThousandRawKeysAndRepeatedReloadsKeepOnlyActiveMeters() {
        for (var algorithm : Algorithm.values()) {
            var registry = new SimpleMeterRegistry();
            var policy = RateLimitPolicy.builder("p").scope(Scope.USER).algorithm(algorithm)
                    .limit(new Limit(10, 1, Duration.ofHours(1))).build();
            var policies = PolicySet.compile(List.of(policy));
            try (var flow = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore())
                    .addListener(new MicrometerDecisionListener(registry)).build()) {
                for (int key = 0; key < 10_000; key++) {
                    assertTrue(flow.tryAcquire("p", RateLimitContext.builder()
                            .put(RateLimitContext.PRINCIPAL, "user-" + key).build()).isAllowed());
                    if (key % 100 == 0) drain(flow);
                }
                drain(flow);
                assertEquals(0, flow.observationFailures());
                int count = registry.getMeters().size();
                assertTrue(count < 10);
                assertEquals(10_000, registry.get(QuotaFlowMetrics.DECISIONS).counter().count());
                for (int generation = 0; generation < 100; generation++) {
                    String id = "policy-" + generation;
                    flow.replacePolicySet(PolicySet.compile(List.of(policy(id, 10))));
                    flow.tryAcquire(id, RateLimitContext.empty());
                    drain(flow);
                    assertEquals(count, registry.getMeters().size());
                    assertTrue(registry.getMeters().stream().allMatch(meter -> id.equals(meter.getId().getTag("policy"))));
                }
                assertEquals(0, flow.observationFailures());
            }
        }
    }
}
