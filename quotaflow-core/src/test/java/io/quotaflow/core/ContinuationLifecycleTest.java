package io.quotaflow.core;

import io.quotaflow.core.execution.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ContinuationLifecycleTest {
    static final PolicySet POLICIES = AcquisitionLifecycleTest.POLICIES;
    static void until(BooleanSupplier condition) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < end) Thread.sleep(1);
        assertTrue(condition.getAsBoolean());
    }
    @Test void neverCompletingNonwaitingAttemptIsBoundedAndLateSuccessIsIgnored() throws Exception {
        var store = new AcquisitionLifecycleTest.PendingStore(); var events = new AtomicInteger();
        var flow = DefaultQuotaFlow.builder(POLICIES, store).operationTimeout(Duration.ofMillis(100))
                .addListener((d,g) -> events.incrementAndGet()).build();
        var result = flow.tryAcquireAsync("p", RateLimitContext.empty(), 1).toCompletableFuture();
        var call = store.next(); var decision = result.get(2, TimeUnit.SECONDS);
        assertFalse(decision.isAllowed()); assertTrue(decision.retryAfter().isEmpty());
        assertEquals(Duration.ZERO, decision.waitDuration()); assertEquals(0, flow.waitQueueDepth("p"));
        call.complete(ChainResult.acquired(0, 9)); assertEquals(1, events.get());
    }
    @Test void blockedListenerCannotStopAnotherDeadlineFromWinning() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var flow = DefaultQuotaFlow.builder(POLICIES, new LocalRateLimitStore()).addListener((d,g) -> {
            entered.countDown(); try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }).build();
        try {
            var first = flow.tryAcquireAsync("p", RateLimitContext.empty(), 1).toCompletableFuture();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            var store = new AcquisitionLifecycleTest.PendingStore();
            var secondFlow = DefaultQuotaFlow.builder(POLICIES, store).build();
            var second = secondFlow.acquireAsync("p", RateLimitContext.empty(), 1, Duration.ofMillis(100)).toCompletableFuture();
            var pending = store.next();
            assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), second.get(2, TimeUnit.SECONDS).throttleRejection());
            assertFalse(second.cancel(true)); pending.complete(ChainResult.acquired(0, 1));
            release.countDown(); assertTrue(first.get(2, TimeUnit.SECONDS).isAllowed());
        } finally { release.countDown(); }
    }
    @Test void cancellingSequentialStoreStopsLaterDispatch() throws Exception {
        var parent = RateLimitPolicy.builder("root").scope(Scope.GLOBAL).limit(new Limit(10,1,Duration.ofSeconds(1))).build();
        var leaf = RateLimitPolicy.builder("leaf").parentId("root").scope(Scope.USER).defaultKey("u")
                .limit(new Limit(10,1,Duration.ofSeconds(1))).build();
        var dispatched = new CountDownLatch(1); var call = new CompletableFuture<StoreResult>(); var calls = new AtomicInteger();
        RateLimitStore store = new RateLimitStore() {
            public CompletionStage<Void> registerPolicies(List<PolicyBinding> bindings) { return CompletableFuture.completedFuture(null); }
            public StoreResult tryAcquire(BucketIdentity key,Limit limit,Algorithm algorithm,long weight) { throw new AssertionError(); }
            public CompletionStage<StoreResult> tryAcquireAsync(BucketIdentity key,Limit limit,Algorithm algorithm,long weight) {
                calls.incrementAndGet(); dispatched.countDown(); return call;
            }
        };
        var flow = DefaultQuotaFlow.builder(PolicySet.compile(List.of(parent,leaf)),store).build();
        var result = flow.tryAcquireAsync("leaf",RateLimitContext.empty(),1).toCompletableFuture();
        assertTrue(dispatched.await(2,TimeUnit.SECONDS)); assertTrue(result.cancel(true));
        call.complete(StoreResult.acquired(9));
        Thread.sleep(30); assertEquals(1,calls.get());
    }
    @Test void recoveryReadinessReevaluatesAndCancellationOnlyDetachesOneWaiter() throws Exception {
        var signal = new CompletableFuture<Void>(); var dispatchCount = new AtomicInteger();
        var pending = new RecoveryPending(new QuotaDomain("default","p"),1,signal);
        var store = new AcquisitionLifecycleTest.PendingStore() {
            @Override public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
                dispatchCount.incrementAndGet(); return CompletableFuture.completedFuture(signal.isDone()
                        ? ChainResult.acquired(0,9) : ChainResult.pending(0,pending));
            }
        };
        var entries = new CopyOnWriteArrayList<String>(); var events = new AtomicInteger();
        var flow = DefaultQuotaFlow.builder(POLICIES,store).addWaitListener((p,g)->entries.add(p+":"+g))
                .addListener((d,g)->events.incrementAndGet()).build();
        var cancelled = flow.acquireAsync("p",RateLimitContext.empty(),1,Duration.ofSeconds(5)).toCompletableFuture();
        var live = flow.acquireAsync("p",RateLimitContext.empty(),1,Duration.ofSeconds(5)).toCompletableFuture();
        until(()->flow.waitQueueDepth("p")==2 && entries.size()==2);
        assertTrue(cancelled.cancel(true)); assertFalse(signal.isDone());
        signal.complete(null);
        assertTrue(live.get(2,TimeUnit.SECONDS).isAllowed());
        assertEquals(List.of("p:global","p:global"),entries); assertEquals(1,events.get());
        assertEquals(3,dispatchCount.get()); assertEquals(0,flow.retainedQueues());
    }
    @Test void dispatchSaturationHasNoQueueEntryAndNoCallerWork() throws Exception {
        try(var execution = new BoundedExecution(1,1)) {
            var release = new CountDownLatch(1); var entered = new CountDownLatch(1);
            var occupied = execution.submit(()->{ entered.countDown(); try { release.await(); } catch(InterruptedException e){Thread.currentThread().interrupt();} return 1; },()->true);
            assertTrue(entered.await(2,TimeUnit.SECONDS));
            var queued = execution.submit(()->2,()->true);
            try {
                var entries = new AtomicInteger();
                var flow = DefaultQuotaFlow.builder(POLICIES,new LocalRateLimitStore()).execution(execution)
                        .addWaitListener((p,g)->entries.incrementAndGet()).build();
                var result = flow.acquireAsync("p",RateLimitContext.empty(),1,Duration.ofSeconds(5)).toCompletableFuture().get(2,TimeUnit.SECONDS);
                assertEquals(Optional.of(ThrottleRejection.QUEUE_OVERFLOW),result.throttleRejection());
                assertEquals(0,entries.get()); assertEquals(1,execution.activeWorkers()); assertEquals(1,execution.pendingTasks());
                assertFalse(flow.tryAcquireAsync("p",RateLimitContext.empty(),1).toCompletableFuture().get(2,TimeUnit.SECONDS).isAllowed());
            } finally { release.countDown(); occupied.toCompletableFuture().get(2,TimeUnit.SECONDS); queued.toCompletableFuture().get(2,TimeUnit.SECONDS); }
        }
    }
    @Test void manyWaitersAndPolicyChurnReleaseQueuesAndTimers() throws Exception {
        int baseline = DeadlineScheduler.pendingTimers();
        try(var execution = new BoundedExecution(2,1024)) {
            var local = new LocalRateLimitStore();
            var policy = RateLimitPolicy.builder("p").scope(Scope.GLOBAL).reaction(Reaction.THROTTLE)
                    .limit(new Limit(1,1,Duration.ofHours(1))).build();
            var policies = PolicySet.compile(List.of(policy));
            var flow = DefaultQuotaFlow.builder(policies,local).execution(execution).build();
            assertTrue(flow.tryAcquire("p",RateLimitContext.empty()).isAllowed());
            var waiting = new ArrayList<CompletableFuture<Decision>>();
            for(int i=0;i<200;i++) waiting.add(flow.acquireAsync("p",RateLimitContext.empty(),1,Duration.ofMinutes(1)).toCompletableFuture());
            until(()->flow.waitQueueDepth("p")==200);
            assertTrue(execution.activeWorkers()<=2); assertEquals(0,execution.pendingTasks());
            waiting.forEach(f->assertTrue(f.cancel(true)));
            until(()->flow.retainedQueues()==0 && DeadlineScheduler.pendingTimers()<=baseline);
            for(int i=0;i<20;i++) {
                var call=flow.acquireAsync("p",RateLimitContext.empty(),1,Duration.ofSeconds(5)).toCompletableFuture();
                until(()->flow.waitQueueDepth("p")==1);
                flow.replacePolicySet(PolicySet.compile(List.of(RateLimitPolicy.builder("other").scope(Scope.GLOBAL).limit(new Limit(1,1,Duration.ofHours(1))).build())));
                assertFalse(call.get(2,TimeUnit.SECONDS).isAllowed());
                assertEquals(0,flow.retainedQueues()); flow.replacePolicySet(policies);
            }
        }
    }
    @Test void blockingStoreInvocationAndInlineContinuationExecutorCannotBlockTheCaller() throws Exception {
        try (var execution = new BoundedExecution(1, 2)) {
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var store = new AcquisitionLifecycleTest.PendingStore() {
                @Override public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
                    entered.countDown();
                    try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    return CompletableFuture.completedFuture(ChainResult.acquired(0, 9));
                }
            };
            var flow = DefaultQuotaFlow.builder(POLICIES, store).execution(execution).asyncExecutor(Runnable::run).build();
            try {
                var call = flow.acquireAsync("p", RateLimitContext.empty(), 1, Duration.ofMillis(100)).toCompletableFuture();
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), call.get(2, TimeUnit.SECONDS).throttleRejection());
                assertFalse(call.cancel(true)); assertEquals(1, execution.activeWorkers());
            } finally { release.countDown(); }
        }
    }

}
