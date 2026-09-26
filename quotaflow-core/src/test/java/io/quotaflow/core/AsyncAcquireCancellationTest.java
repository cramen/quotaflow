package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/**
 * Cancellation contract of {@link DefaultQuotaFlow#acquireAsync}: cancelling
 * the returned future removes the waiter from its queue promptly — the slot is
 * freed, no quota is consumed and no listener event fires for the abandoned
 * wait. Normal completion and wait-timeout paths are unaffected.
 */
class AsyncAcquireCancellationTest {

    private static RateLimitPolicy throttlePolicy(String id, long capacity, long refill, Duration period) {
        return RateLimitPolicy.builder(id)
                .limit(new Limit(capacity, refill, period))
                .scope(Scope.GLOBAL)
                .reaction(Reaction.THROTTLE)
                .build();
    }

    private static DefaultQuotaFlow flowFor(RateLimitPolicy policy) {
        return DefaultQuotaFlow.builder(PolicySet.compile(List.of(policy)), new LocalRateLimitStore())
                .build();
    }

    private static void awaitTrue(BooleanSupplier condition, Duration timeout, String description)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline >= 0) {
                fail("timed out waiting for: " + description);
            }
            Thread.sleep(2);
        }
    }

    @Test
    void cancelledAsyncWaiterLeavesQueuePromptly() throws Exception {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        CompletableFuture<Decision> waiting = flow
                .acquireAsync("t", RateLimitContext.empty(), 1, Duration.ofSeconds(60))
                .toCompletableFuture();
        awaitTrue(() -> flow.waitQueueDepth("t") == 1, Duration.ofSeconds(2), "waiter queued");

        long start = System.nanoTime();
        assertTrue(waiting.cancel(true));
        awaitTrue(() -> flow.waitQueueDepth("t") == 0, Duration.ofSeconds(2), "slot freed");
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertTrue(waiting.isCancelled());
        assertTrue(elapsedMillis < 2_000, "cancelled waiter left only after " + elapsedMillis + " ms");
    }

    @Test
    void cancelledAsyncWaiterEmitsNoDecisionAndConsumesNoQuota() throws Exception {
        List<Decision> events = new CopyOnWriteArrayList<>();
        DefaultQuotaFlow flow = DefaultQuotaFlow
                .builder(PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofMillis(60)))),
                        new LocalRateLimitStore())
                .addListener((decision, keyGroup) -> events.add(decision))
                .build();
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        assertEquals(1, events.size(), "the draining acquisition emitted its event");
        CompletableFuture<Decision> waiting = flow
                .acquireAsync("t", RateLimitContext.empty(), 1, Duration.ofSeconds(30))
                .toCompletableFuture();
        awaitTrue(() -> flow.waitQueueDepth("t") == 1, Duration.ofSeconds(2), "waiter queued");

        assertTrue(waiting.cancel(true));

        // several refill periods pass: an uncancelled waiter would have retried,
        // consumed the refilled token and fired an allow event
        Thread.sleep(300);
        assertEquals(1, events.size(), "no decision emitted for the cancelled waiter: " + events);
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed(),
                "the refilled token must survive for a live caller");
    }

    @Test
    void cancelledSlotIsAvailableToSubsequentCaller() throws Exception {
        DefaultQuotaFlow flow = DefaultQuotaFlow
                .builder(PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)))),
                        new LocalRateLimitStore())
                .maxWaitersPerPolicy(1)
                .build();
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        CompletableFuture<Decision> waiting = flow
                .acquireAsync("t", RateLimitContext.empty(), 1, Duration.ofSeconds(60))
                .toCompletableFuture();
        awaitTrue(() -> flow.waitQueueDepth("t") == 1, Duration.ofSeconds(2), "queue at its bound");
        assertTrue(waiting.cancel(true));
        awaitTrue(() -> flow.waitQueueDepth("t") == 0, Duration.ofSeconds(2), "slot freed");

        // with the slot freed, the next caller enqueues and waits out its own
        // timeout instead of being overflow-rejected
        Decision decision = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofMillis(150));
        assertFalse(decision.isAllowed());
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), decision.throttleRejection());
        assertTrue(decision.waitDuration().toMillis() >= 100,
                "the caller really enqueued and waited, got " + decision.waitDuration());
    }

    @Test
    void cancellingCompletedFutureHasNoEffect() throws Exception {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 5, 1, Duration.ofMinutes(1)));
        CompletableFuture<Decision> completed = flow
                .acquireAsync("t", RateLimitContext.empty(), 1, Duration.ofSeconds(5))
                .toCompletableFuture();
        assertTrue(completed.get(10, TimeUnit.SECONDS).isAllowed());
        assertFalse(completed.cancel(true), "an already completed future must not cancel");
        assertEquals(0, flow.waitQueueDepth("t"));
    }

    @Test
    void cancelBeforeWorkerStartsIsHarmless() {
        AtomicReference<Runnable> queuedTask = new AtomicReference<>();
        List<Decision> events = new CopyOnWriteArrayList<>();
        DefaultQuotaFlow flow = DefaultQuotaFlow
                .builder(PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)))),
                        new LocalRateLimitStore())
                .asyncExecutor(queuedTask::set)
                .addListener((decision, keyGroup) -> events.add(decision))
                .build();
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        CompletableFuture<Decision> waiting = flow
                .acquireAsync("t", RateLimitContext.empty(), 1, Duration.ofSeconds(60))
                .toCompletableFuture();

        assertTrue(waiting.cancel(true), "cancellation wins before the worker starts");
        queuedTask.get().run();

        assertTrue(waiting.isCancelled());
        assertEquals(0, flow.waitQueueDepth("t"));
        assertEquals(1, events.size(), "only the draining acquisition emitted an event");
    }

    @Test
    void asyncWaitTimeoutStillCompletesWithDecision() throws Exception {
        List<Decision> events = new CopyOnWriteArrayList<>();
        DefaultQuotaFlow flow = DefaultQuotaFlow
                .builder(PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)))),
                        new LocalRateLimitStore())
                .addListener((decision, keyGroup) -> events.add(decision))
                .build();
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());

        Decision decision = flow
                .acquireAsync("t", RateLimitContext.empty(), 1, Duration.ofMillis(120))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertFalse(decision.isAllowed());
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), decision.throttleRejection());
        assertEquals(2, events.size(), "finalized decisions still emit exactly one event each");
        assertEquals(0, flow.waitQueueDepth("t"));
    }
}
