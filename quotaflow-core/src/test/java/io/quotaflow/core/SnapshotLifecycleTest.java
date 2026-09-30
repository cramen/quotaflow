package io.quotaflow.core;

import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SnapshotLifecycleTest {
    @Test void recoveryRetryCapturesFreshWholeSnapshotAndKeepsOneQueueEntry() throws Exception {
        var root=RateLimitPolicy.builder("root").scope(Scope.GLOBAL).limitRef("plan").build();
        var leaf=RateLimitPolicy.builder("leaf").parentId("root").scope(Scope.USER).defaultKey("u").limitRef("plan").reaction(Reaction.THROTTLE).build();
        var policies=PolicySet.compile(List.of(root,leaf));
        var initial=new LimitSnapshot(1,Map.of(new LimitSnapshot.Key("plan","global"),new Limit(10,1,Duration.ofSeconds(1)),new LimitSnapshot.Key("plan","default"),new Limit(10,1,Duration.ofSeconds(1))));
        var changed=new LimitSnapshot(2,Map.of(new LimitSnapshot.Key("plan","global"),new Limit(2,1,Duration.ofSeconds(1)),new LimitSnapshot.Key("plan","default"),new Limit(2,1,Duration.ofSeconds(1))));
        var published=new AtomicReference<>(initial);var reads=new AtomicInteger();
        VersionedLimitResolver resolver=()->{reads.incrementAndGet();return Optional.of(published.get());};
        var resume=new CompletableFuture<Void>();var chains=new CopyOnWriteArrayList<List<LevelRequest>>();
        var store=new AcquisitionLifecycleTest.PendingStore() {
            @Override public boolean requiresVersionedLimits(){return true;}
            @Override public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain){
                chains.add(chain);
                if(chains.size()==1) return CompletableFuture.completedFuture(ChainResult.pending(0,new RecoveryPending(chain.get(0).storageKey().domain(),1,resume)));
                return CompletableFuture.completedFuture(ChainResult.acquired(1,1));
            }
        };
        var entries=new CopyOnWriteArrayList<String>();var terminalGroups=new CopyOnWriteArrayList<String>();
        var flow=DefaultQuotaFlow.builder(policies,store).limitResolver(resolver).addWaitListener((p,g)->entries.add(p+":"+g))
                .addListener((d,g)->terminalGroups.add(d.policyId()+":"+g)).build();
        var result=flow.acquireAsync("leaf",RateLimitContext.empty(),1,Duration.ofSeconds(5)).toCompletableFuture();
        ContinuationLifecycleTest.until(()->flow.waitQueueDepth("leaf")==1);
        published.set(changed);resume.complete(null);
        assertTrue(result.get(2,TimeUnit.SECONDS).isAllowed());
        flow.flushObservations().toCompletableFuture().orTimeout(2,TimeUnit.SECONDS).join();
        assertEquals(2,reads.get());assertEquals(List.of("leaf:global"),entries);assertEquals(List.of("leaf:default"),terminalGroups);
        for(int i=0;i<2;i++) for(var request:chains.get(i)) assertEquals(i+1,request.resolverRevision());
        assertEquals(chains.get(0).get(0).resolverFingerprint(),chains.get(0).get(1).resolverFingerprint());
    }
    @Test void cancellationDuringSnapshotCaptureCannotDispatchLateQuota() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        var snapshot=new LimitSnapshot(1,Map.of(new LimitSnapshot.Key("plan","global"),new Limit(10,1,Duration.ofSeconds(1))));
        VersionedLimitResolver provider=()->{
            entered.countDown();try{release.await();}catch(InterruptedException e){Thread.currentThread().interrupt();}
            return Optional.of(snapshot);
        };
        var store=new AcquisitionLifecycleTest.PendingStore();
        var policies=PolicySet.compile(List.of(RateLimitPolicy.builder("p").scope(Scope.GLOBAL).limitRef("plan").build()));
        var flow=DefaultQuotaFlow.builder(policies,store).limitResolver(provider).build();
        try {
            var result=flow.tryAcquireAsync("p",RateLimitContext.empty(),1).toCompletableFuture();
            assertTrue(entered.await(2,TimeUnit.SECONDS));assertTrue(result.cancel(true));release.countDown();
            assertNull(store.calls.poll(100,TimeUnit.MILLISECONDS));
        } finally {release.countDown();}
    }
    @Test void parentBlockedQueueOverflowRetainsFiredLevelWithoutAnEntry() throws Exception {
        var parent=RateLimitPolicy.builder("root").scope(Scope.GLOBAL).limit(new Limit(1,1,Duration.ofHours(1))).build();
        var leaf=RateLimitPolicy.builder("leaf").parentId("root").scope(Scope.USER).defaultKey("u")
                .reaction(Reaction.THROTTLE).limit(new Limit(10,1,Duration.ofHours(1))).build();
        var entries=new AtomicInteger();
        var flow=DefaultQuotaFlow.builder(PolicySet.compile(List.of(parent,leaf)),new LocalRateLimitStore())
                .maxWaitersPerPolicy(1).addWaitListener((p,g)->entries.incrementAndGet()).build();
        assertTrue(flow.tryAcquire("leaf",RateLimitContext.empty()).isAllowed());
        var first=flow.acquireAsync("leaf",RateLimitContext.empty(),1,Duration.ofSeconds(5)).toCompletableFuture();
        try {
            ContinuationLifecycleTest.until(()->flow.waitQueueDepth("leaf")==1 && entries.get()==1);
            var overflow=flow.acquireAsync("leaf",RateLimitContext.empty(),1,Duration.ofSeconds(5)).toCompletableFuture().get(2,TimeUnit.SECONDS);
            assertEquals("root",overflow.policyId());assertEquals(Optional.of(ThrottleRejection.QUEUE_OVERFLOW),overflow.throttleRejection());
            assertEquals(1,entries.get());assertEquals(Duration.ZERO,overflow.waitDuration());
        } finally {first.cancel(true);}
    }

}
