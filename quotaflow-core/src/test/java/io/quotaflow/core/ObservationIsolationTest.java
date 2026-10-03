package io.quotaflow.core;

import io.quotaflow.core.observation.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ObservationIsolationTest {
    static final PolicySet POLICIES=PolicySet.compile(List.of(RateLimitPolicy.builder("p").scope(Scope.GLOBAL).limit(new Limit(100,1,Duration.ofHours(1))).build()));
    @Test void blockedAndThrowingListenersCannotDelayOtherListenersOrAdmission() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var delivered=new AtomicInteger();
        try(var flow=DefaultQuotaFlow.builder(POLICIES,new LocalRateLimitStore()).observationDelivery(64,Duration.ofMillis(100))
                .addListener((d,g)->{entered.countDown();try{release.await();}catch(InterruptedException e){Thread.currentThread().interrupt();}})
                .addListener((d,g)->{throw new IllegalStateException("fixture-secret-never-log");})
                .addListener((d,g)->delivered.incrementAndGet()).build()) {
            try {
                for(int i=0;i<6;i++) assertTrue(flow.tryAcquireAsync("p",RateLimitContext.empty(),1).toCompletableFuture().get(1,TimeUnit.SECONDS).isAllowed());
                assertTrue(entered.await(1,TimeUnit.SECONDS));flow.flushObservations().toCompletableFuture().get(2,TimeUnit.SECONDS);
                assertEquals(6,delivered.get());assertTrue(flow.observationFailures()>=7);
            } finally {release.countDown();}
        }
    }
    @Test void retiringConfigurationFollowsOldTerminalDeliveryAndCloseReleasesSessions() throws Exception {
        var store=new AcquisitionLifecycleTest.PendingStore();var events=new CopyOnWriteArrayList<String>();
        var flow=DefaultQuotaFlow.builder(POLICIES,store).addObservationListener(initial ->new ObservationSession(){
            public void onConfiguration(ObservationConfiguration value){events.add("config:"+value.generation());}
            public void onDecision(long generation,Decision value,String group){events.add("decision:"+generation);}
            public void onRetired(long generation){events.add("retired:"+generation);}
            public void close(){events.add("closed");}
        }).build();
        var result=flow.tryAcquireAsync("p",RateLimitContext.empty(),1).toCompletableFuture();var pending=store.next();
        flow.replacePolicySet(POLICIES);flow.flushObservations().toCompletableFuture().get(2,TimeUnit.SECONDS);
        assertEquals(List.of("config:1"),events);
        pending.complete(ChainResult.acquired(0,99));assertTrue(result.get(2,TimeUnit.SECONDS).isAllowed());
        flow.flushObservations().toCompletableFuture().get(2,TimeUnit.SECONDS);
        assertEquals(List.of("config:1","decision:0","retired:0"),events);
        flow.close();flow.flushObservations().toCompletableFuture().get(2,TimeUnit.SECONDS);
        assertEquals(List.of("config:1","decision:0","retired:0","retired:1","closed"),events);
        assertThrows(IllegalStateException.class,()->flow.tryAcquire("p",RateLimitContext.empty()));
    }
    @Test void observationSessionCreationFailureIsIsolatedAndUnknownBudgetsStayAbsent() throws Exception {
        var decisions=new AtomicInteger();
        try(var flow=DefaultQuotaFlow.builder(POLICIES,new LocalRateLimitStore())
                .addObservationListener(initial->{throw new IllegalArgumentException("hidden-detail");})
                .addListener((d,g)->decisions.incrementAndGet()).build()) {
            assertTrue(flow.tryAcquire("p",RateLimitContext.empty()).isAllowed());
            flow.flushObservations().toCompletableFuture().get(2,TimeUnit.SECONDS);
            assertEquals(1,decisions.get());assertEquals(1,flow.observationFailures());
        }
    }

    @Test void oneMultiInterfaceObserverOwnsOneSessionAndInvalidBoundsFailEarly() throws Exception {
        class Bridge implements DecisionListener, WaitListener, ObservationListener {
            final AtomicInteger opens = new AtomicInteger(), deliveries = new AtomicInteger();
            public void onDecision(Decision decision, String group) { fail("legacy callback must not duplicate a contextual event"); }
            public void onQueued(String policy, String group) { fail("legacy callback must not duplicate a contextual event"); }
            public ObservationSession open(ObservationConfiguration initial) {
                opens.incrementAndGet(); return new ObservationSession() {
                    public void onDecision(long generation, Decision decision, String group) { deliveries.incrementAndGet(); }
                };
            }
        }
        var bridge = new Bridge();
        var builder = DefaultQuotaFlow.builder(POLICIES, new LocalRateLimitStore());
        assertThrows(IllegalArgumentException.class, () -> builder.observationDelivery(0, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> builder.observationDelivery(1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> builder.observationDelivery(1, Duration.ofSeconds(-1)));
        assertThrows(NullPointerException.class, () -> builder.addObservationListener(null));
        var flow = builder.addListener(bridge).addWaitListener(bridge).addObservationListener(bridge).build();
        flow.tryAcquire("p", RateLimitContext.empty());
        flow.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(1, bridge.opens.get()); assertEquals(1, bridge.deliveries.get());
        flow.close(); flow.close();
        assertThrows(IllegalStateException.class, () -> flow.replacePolicySet(POLICIES));
    }

    @Test void unrelatedEventKindsCannotSaturateAListenersLane() throws Exception {
        for (boolean waiting : List.of(false, true)) {
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            Runnable block = () -> { entered.countDown(); try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } };
            var dispatcher = new ObservationDispatcher(waiting ? List.of() : List.of((d,g) -> block.run()),
                    waiting ? List.of((p,g) -> block.run()) : List.of(), List.of(),
                    new ObservationConfiguration(0, Map.of("p", "p"), Set.of()), 1, Duration.ofSeconds(10));
            try {
                if (waiting) dispatcher.queued(0, "p", "global");
                else dispatcher.decision(0, Decision.allowed("p", Scope.GLOBAL, 1), "global");
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                for (int event = 0; event < 1000; event++) {
                    if (waiting) dispatcher.decision(0, Decision.allowed("p", Scope.GLOBAL, 1), "global");
                    else dispatcher.queued(0, "p", "global");
                }
                assertEquals(0, dispatcher.failures());
            } finally {
                release.countDown(); dispatcher.barrier().toCompletableFuture().get(2, TimeUnit.SECONDS);
                dispatcher.close().toCompletableFuture().get(2, TimeUnit.SECONDS);
            }
        }
    }
}
