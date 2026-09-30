package io.quotaflow.core;

import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AcquisitionLifecycleTest {
    static final Duration BUDGET = Duration.ofSeconds(30);
    static final PolicySet POLICIES = PolicySet.compile(List.of(RateLimitPolicy.builder("p")
            .scope(Scope.GLOBAL).reaction(Reaction.THROTTLE)
            .limit(new Limit(10, 1, Duration.ofMinutes(1))).build()));
    static class PendingStore implements BatchRateLimitStore {
        final BlockingQueue<CompletableFuture<ChainResult>> calls = new LinkedBlockingQueue<>();
        public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
            var result = new CompletableFuture<ChainResult>(); calls.add(result); return result;
        }
        public StoreResult tryAcquire(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) {
            throw new AssertionError("batch expected");
        }
        public CompletionStage<StoreResult> tryAcquireAsync(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) { throw new AssertionError("batch expected"); }
        public CompletionStage<Void> registerPolicies(List<PolicyBinding> bindings) { return CompletableFuture.completedFuture(null); }
        CompletableFuture<ChainResult> next() throws Exception {
            var call = calls.poll(2, TimeUnit.SECONDS); assertNotNull(call); return call;
        }
    }
    static final class ManualExecutor implements Executor {
        final BlockingQueue<Runnable> work = new LinkedBlockingQueue<>();
        public void execute(Runnable task) { work.add(task); }
        void runNext() throws Exception { var task = work.poll(2, TimeUnit.SECONDS); assertNotNull(task); task.run(); }
    }
    @Test void executorDelayConsumesApiEntryBudget() throws Exception {
        var clock = new AtomicLong(); var executor = new ManualExecutor(); var store = new LocalRateLimitStore();
        var flow = DefaultQuotaFlow.builder(POLICIES, store).nanoClock(clock::get).asyncExecutor(executor).build();
        var result = flow.acquireAsync("p", RateLimitContext.empty(), 1, BUDGET).toCompletableFuture();
        clock.set(BUDGET.toNanos()); executor.runNext();
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), result.get(2, TimeUnit.SECONDS).throttleRejection());
    }
    @Test void lateInitialAndQueuedStoreSuccessCannotAllow() throws Exception {
        for (boolean queued : List.of(false, true)) {
            var clock = new AtomicLong(); var store = new PendingStore();
            var flow = DefaultQuotaFlow.builder(POLICIES, store).nanoClock(clock::get).build();
            var result = flow.acquireAsync("p", RateLimitContext.empty(), 1, BUDGET).toCompletableFuture();
            var pending = store.next();
            if (queued) {
                pending.complete(ChainResult.rejected(0, 0, 60_000));
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (flow.waitQueueDepth("p") != 1 && System.nanoTime() < end) Thread.yield();
                assertEquals(1, flow.waitQueueDepth("p"));
                flow.replacePolicySet(POLICIES); pending = store.next();
            }
            clock.set(BUDGET.toNanos()); pending.complete(ChainResult.acquired(0, 9));
            var decision = result.get(2, TimeUnit.SECONDS);
            assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), decision.throttleRejection());
            assertEquals(BUDGET, decision.waitDuration());
        }
    }
    @Test void cancellationAndCompletionHaveOneTerminalOwner() throws Exception {
        for (boolean cancelFirst : List.of(true, false)) {
            var store = new PendingStore(); var events = new CopyOnWriteArrayList<Decision>();
            var event = new CountDownLatch(1);
            var flow = DefaultQuotaFlow.builder(POLICIES, store)
                    .addListener((d,g) -> { events.add(d); event.countDown(); }).build();
            var result = flow.acquireAsync("p", RateLimitContext.empty(), 1, BUDGET).toCompletableFuture();
            var pending = store.next();
            if (cancelFirst) assertTrue(result.cancel(true));
            pending.complete(ChainResult.acquired(0, 9));
            if (cancelFirst) {
                assertFalse(event.await(100, TimeUnit.MILLISECONDS)); assertTrue(result.isCancelled());
            } else {
                assertTrue(result.get(2, TimeUnit.SECONDS).isAllowed()); assertFalse(result.cancel(true));
                assertTrue(event.await(2, TimeUnit.SECONDS)); assertEquals(1, events.size());
            }
        }
    }
}
