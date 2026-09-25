package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ThrottleAcquireTest {

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void shutdownExecutor() {
        executor.shutdownNow();
    }

    private static RateLimitPolicy throttlePolicy(String id, long capacity, long refill, Duration period) {
        return RateLimitPolicy.builder(id)
                .limit(new Limit(capacity, refill, period))
                .scope(Scope.GLOBAL)
                .reaction(Reaction.THROTTLE)
                .build();
    }

    private static RateLimitPolicy rejectPolicy(String id, long capacity, long refill, Duration period) {
        return RateLimitPolicy.builder(id)
                .limit(new Limit(capacity, refill, period))
                .scope(Scope.GLOBAL)
                .reaction(Reaction.REJECT)
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
    void instantAllowCarriesZeroWait() {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 5, 1, Duration.ofSeconds(1)));
        Decision decision = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(1));
        assertTrue(decision.isAllowed());
        assertEquals(Duration.ZERO, decision.waitDuration());
        assertTrue(decision.throttleRejection().isEmpty());
    }

    @Test
    void rejectModePolicyNeverWaits() {
        DefaultQuotaFlow flow = flowFor(rejectPolicy("r", 1, 1, Duration.ofHours(1)));
        assertTrue(flow.tryAcquire("r", RateLimitContext.empty()).isAllowed());
        long start = System.nanoTime();
        Decision decision = flow.acquire("r", RateLimitContext.empty(), 1, Duration.ofSeconds(30));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertFalse(decision.isAllowed());
        assertEquals(Duration.ZERO, decision.waitDuration());
        assertTrue(decision.throttleRejection().isEmpty());
        assertTrue(elapsedMillis < 1_000, "reject-mode policy waited " + elapsedMillis + " ms");
    }

    @Test
    void acquireRejectsInvalidArgumentsSynchronously() {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofSeconds(1)));
        Duration timeout = Duration.ofSeconds(1);
        assertThrows(IllegalArgumentException.class,
                () -> flow.acquire("t", RateLimitContext.empty(), 0, timeout));
        assertThrows(IllegalArgumentException.class,
                () -> flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofMillis(-1)));
        assertThrows(NullPointerException.class,
                () -> flow.acquire("t", RateLimitContext.empty(), 1, null));
        assertThrows(IllegalArgumentException.class,
                () -> flow.acquireAsync("t", RateLimitContext.empty(), 0, timeout));
        assertThrows(IllegalArgumentException.class,
                () -> flow.acquireAsync("t", RateLimitContext.empty(), 1, Duration.ofMillis(-1), 3));
        assertThrows(NullPointerException.class,
                () -> flow.acquireAsync("t", RateLimitContext.empty(), 1, null));
    }

    @Test
    void builderRejectsInvalidMaxWaiters() {
        PolicySet set = PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofSeconds(1))));
        DefaultQuotaFlow.Builder builder = DefaultQuotaFlow.builder(set, new LocalRateLimitStore());
        assertThrows(IllegalArgumentException.class, () -> builder.maxWaitersPerPolicy(0));
        assertThrows(NullPointerException.class, () -> builder.asyncExecutor(null));
    }

    @Test
    void burstIsAbsorbedAsLatency() throws Exception {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMillis(50)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        long start = System.nanoTime();
        Decision decision = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(5));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(decision.isAllowed());
        assertTrue(decision.waitDuration().toMillis() >= 20,
                "expected a real wait, got " + decision.waitDuration());
        assertTrue(decision.waitDuration().compareTo(Duration.ofSeconds(5)) < 0,
                "wait duration must never exceed the wait timeout, got " + decision.waitDuration());
        assertTrue(elapsedMillis < 5_000);
        assertTrue(decision.throttleRejection().isEmpty());
    }

    @Test
    void waitTimeoutRejectsWithElapsedWait() {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        long start = System.nanoTime();
        Decision decision = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofMillis(150));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertFalse(decision.isAllowed());
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), decision.throttleRejection());
        assertTrue(decision.waitDuration().toMillis() >= 100,
                "wait duration should reflect the timeout, got " + decision.waitDuration());
        assertTrue(elapsedMillis >= 100, "caller returned after " + elapsedMillis + " ms");
        assertTrue(elapsedMillis < 3_000, "caller held past its deadline: " + elapsedMillis + " ms");
        // the rejection still carries the refill schedule of the fired level
        assertTrue(decision.retryAfter().isPresent());
        assertEquals(0, flow.waitQueueDepth("t"));
    }

    @Test
    void zeroWaitTimeoutRejectsImmediately() {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        Decision decision = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ZERO);
        assertFalse(decision.isAllowed());
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), decision.throttleRejection());
        assertEquals(Duration.ZERO, decision.waitDuration());
    }

    @Test
    void queueOverflowRejectsImmediately() throws Exception {
        DefaultQuotaFlow flow = DefaultQuotaFlow
                .builder(PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)))),
                        new LocalRateLimitStore())
                .maxWaitersPerPolicy(2)
                .build();
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        // fill the queue with two waiters
        Future<Decision> first = executor.submit(
                () -> flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(2)));
        Future<Decision> second = executor.submit(
                () -> flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(2)));
        awaitTrue(() -> flow.waitQueueDepth("t") == 2, Duration.ofSeconds(2), "queue to fill");

        long start = System.nanoTime();
        Decision overflow = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(30));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertFalse(overflow.isAllowed());
        assertEquals(Optional.of(ThrottleRejection.QUEUE_OVERFLOW), overflow.throttleRejection());
        assertEquals(Duration.ZERO, overflow.waitDuration());
        assertTrue(elapsedMillis < 1_000, "overflow did not reject immediately");

        // the queued waiters are unaffected and leave on their own deadlines
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT),
                first.get(5, TimeUnit.SECONDS).throttleRejection());
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT),
                second.get(5, TimeUnit.SECONDS).throttleRejection());
        awaitTrue(() -> flow.waitQueueDepth("t") == 0, Duration.ofSeconds(2), "queue to drain");
    }

    @Test
    void highPriorityWaiterIsServedFirst() throws Exception {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMillis(100)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        List<String> servedOrder = new CopyOnWriteArrayList<>();
        Future<?> low = executor.submit(() -> {
            Decision d = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(10), 0);
            assertTrue(d.isAllowed());
            servedOrder.add("low");
        });
        awaitTrue(() -> flow.waitQueueDepth("t") == 1, Duration.ofSeconds(2), "low waiter queued");
        Future<?> high = executor.submit(() -> {
            Decision d = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(10), 10);
            assertTrue(d.isAllowed());
            servedOrder.add("high");
        });
        high.get(10, TimeUnit.SECONDS);
        low.get(10, TimeUnit.SECONDS);
        assertEquals(List.of("high", "low"), servedOrder);
    }

    @Test
    void equalPriorityIsServedFifo() throws Exception {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMillis(80)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        List<Integer> servedOrder = new CopyOnWriteArrayList<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            int id = i;
            futures.add(executor.submit(() -> {
                Decision d = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(10), 0);
                assertTrue(d.isAllowed());
                servedOrder.add(id);
            }));
            // guarantee enqueue order before submitting the next waiter
            awaitTrue(() -> flow.waitQueueDepth("t") == id + 1, Duration.ofSeconds(2),
                    "waiter " + id + " queued");
        }
        for (Future<?> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
        assertEquals(List.of(0, 1, 2), servedOrder);
    }

    @Test
    void weightedAcquisitionWaitsForFullWeight() throws Exception {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 4, 4, Duration.ofMillis(100)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty(), 4).isAllowed());
        Future<Decision> waiting = executor.submit(
                () -> flow.acquire("t", RateLimitContext.empty(), 3, Duration.ofSeconds(10)));
        awaitTrue(() -> flow.waitQueueDepth("t") == 1, Duration.ofSeconds(2), "waiter queued");
        // one token refills (25 ms per token) while the waiter is queued: the
        // partial quota serves a weight-1 request but must not free the waiter
        awaitTrue(() -> flow.tryAcquire("t", RateLimitContext.empty(), 1).isAllowed(),
                Duration.ofSeconds(2), "partial refill");
        Decision decision = waiting.get(10, TimeUnit.SECONDS);
        assertTrue(decision.isAllowed());
        assertTrue(decision.waitDuration().toMillis() >= 40,
                "weight 3 must wait for the full weight, got " + decision.waitDuration());
    }

    @Test
    void exactlyOneListenerEventPerFinalDecision() throws Exception {
        List<Decision> events = new CopyOnWriteArrayList<>();
        List<String> keyGroups = new CopyOnWriteArrayList<>();
        DefaultQuotaFlow flow = DefaultQuotaFlow
                .builder(PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofMillis(60)))),
                        new LocalRateLimitStore())
                .addListener((decision, keyGroup) -> {
                    events.add(decision);
                    keyGroups.add(keyGroup);
                })
                .build();
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        // two competing waiters force several lost retries before both are served
        Future<Decision> first = executor.submit(
                () -> flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(10)));
        Future<Decision> second = executor.submit(
                () -> flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(10)));
        assertTrue(first.get(10, TimeUnit.SECONDS).isAllowed());
        assertTrue(second.get(10, TimeUnit.SECONDS).isAllowed());

        assertEquals(3, events.size(), "one drain event plus one per waiter: " + events);
        assertEquals(List.of("global", "global", "global"), keyGroups);
        List<Decision> waited = events.stream()
                .filter(d -> !d.waitDuration().isZero())
                .toList();
        assertEquals(2, waited.size(), "each waiter reports exactly one waited decision");
        assertTrue(waited.stream().allMatch(Decision::isAllowed));
    }

    @Test
    void scheduleLessRejectionDoesNotWait() {
        DefaultQuotaFlow flow = DefaultQuotaFlow
                .builder(PolicySet.compile(List.of(RateLimitPolicy.builder("u")
                        .limit(new Limit(1, 1, Duration.ofMillis(1)))
                        .scope(Scope.USER)
                        .reaction(Reaction.THROTTLE)
                        .build())), new LocalRateLimitStore())
                .build();
        // no principal in the context: missing-key rejection has no refill schedule
        long start = System.nanoTime();
        Decision decision = flow.acquire("u", RateLimitContext.empty(), 1, Duration.ofSeconds(30));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertFalse(decision.isAllowed());
        assertTrue(decision.retryAfter().isEmpty());
        assertTrue(decision.throttleRejection().isEmpty());
        assertTrue(elapsedMillis < 1_000, "schedule-less rejection waited " + elapsedMillis + " ms");
    }

    @Test
    void priorityDefaultsToContextAttributeThenPolicyThenZero() throws Exception {
        // policy default priority 5 beats an explicit context attribute of 1
        RateLimitPolicy policy = RateLimitPolicy.builder("t")
                .limit(new Limit(1, 1, Duration.ofMillis(100)))
                .scope(Scope.GLOBAL)
                .reaction(Reaction.THROTTLE)
                .priority(5)
                .build();
        DefaultQuotaFlow flow = flowFor(policy);
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        List<String> servedOrder = new CopyOnWriteArrayList<>();
        RateLimitContext lowContext =
                RateLimitContext.builder().put(RateLimitContext.PRIORITY, 1).build();
        Future<?> low = executor.submit(() -> {
            assertTrue(flow.acquire("t", lowContext, 1, Duration.ofSeconds(10)).isAllowed());
            servedOrder.add("context-1");
        });
        awaitTrue(() -> flow.waitQueueDepth("t") == 1, Duration.ofSeconds(2), "context waiter queued");
        Future<?> policyDefault = executor.submit(() -> {
            assertTrue(flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(10)).isAllowed());
            servedOrder.add("policy-5");
        });
        policyDefault.get(10, TimeUnit.SECONDS);
        low.get(10, TimeUnit.SECONDS);
        assertEquals(List.of("policy-5", "context-1"), servedOrder);
    }

    @Test
    void explicitPriorityArgumentBeatsContextAttribute() throws Exception {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMillis(100)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        List<String> servedOrder = new CopyOnWriteArrayList<>();
        // explicit 0 must win over the context attribute 99
        RateLimitContext highContext =
                RateLimitContext.builder().put(RateLimitContext.PRIORITY, 99).build();
        Future<?> explicitLow = executor.submit(() -> {
            assertTrue(flow.acquire("t", highContext, 1, Duration.ofSeconds(10), 0).isAllowed());
            servedOrder.add("explicit-0");
        });
        awaitTrue(() -> flow.waitQueueDepth("t") == 1, Duration.ofSeconds(2), "explicit waiter queued");
        Future<?> contextHigh = executor.submit(() -> {
            assertTrue(flow.acquire("t", highContext, 1, Duration.ofSeconds(10)).isAllowed());
            servedOrder.add("context-99");
        });
        contextHigh.get(10, TimeUnit.SECONDS);
        explicitLow.get(10, TimeUnit.SECONDS);
        assertEquals(List.of("context-99", "explicit-0"), servedOrder);
    }

    @Test
    void nonNumericContextPriorityIsCallerMisuse() {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        RateLimitContext bad =
                RateLimitContext.builder().put(RateLimitContext.PRIORITY, "high").build();
        assertThrows(IllegalArgumentException.class,
                () -> flow.acquire("t", bad, 1, Duration.ofSeconds(1)));
    }

    @Test
    void policySetSwapFreesWaiterEarly() throws Exception {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        long start = System.nanoTime();
        Future<Decision> waiting = executor.submit(
                () -> flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(30)));
        awaitTrue(() -> flow.waitQueueDepth("t") == 1, Duration.ofSeconds(2), "waiter queued");

        // the quota would not refill for a minute, but the swap switches to a
        // fast-refilling limit
        flow.replacePolicySet(PolicySet.compile(
                List.of(throttlePolicy("t", 1, 1000, Duration.ofSeconds(1)))));

        Decision decision = waiting.get(10, TimeUnit.SECONDS);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(decision.isAllowed());
        assertTrue(elapsedMillis < 10_000, "swap did not free the waiter early");
        assertTrue(decision.waitDuration().compareTo(Duration.ZERO) > 0);
    }

    @Test
    void vanishedPolicyRejectsConfigurationShaped() throws Exception {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        Future<Decision> waiting = executor.submit(
                () -> flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(30)));
        awaitTrue(() -> flow.waitQueueDepth("t") == 1, Duration.ofSeconds(2), "waiter queued");

        flow.replacePolicySet(PolicySet.compile(List.of(throttlePolicy("other", 5, 1, Duration.ofSeconds(1)))));

        Decision decision = waiting.get(10, TimeUnit.SECONDS);
        assertFalse(decision.isAllowed());
        assertEquals("t", decision.policyId());
        assertEquals(Scope.GLOBAL, decision.scope());
        // configuration-shaped: no refill schedule, no throttle rejection reason
        assertTrue(decision.retryAfter().isEmpty());
        assertTrue(decision.throttleRejection().isEmpty());
        assertTrue(decision.waitDuration().compareTo(Duration.ZERO) > 0);
    }

    @Test
    void limitReferenceIsReResolvedPerRetry() throws Exception {
        AtomicReference<Limit> resolved = new AtomicReference<>(new Limit(1, 1, Duration.ofMinutes(1)));
        RateLimitPolicy policy = RateLimitPolicy.builder("dyn")
                .limitRef("plan")
                .scope(Scope.GLOBAL)
                .reaction(Reaction.THROTTLE)
                .build();
        PolicySet set = PolicySet.compile(List.of(policy));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(set, new LocalRateLimitStore())
                .limitResolver((limitRef, keyGroup) -> Optional.ofNullable(resolved.get()))
                .build();
        assertTrue(flow.tryAcquire("dyn", RateLimitContext.empty()).isAllowed());
        Future<Decision> waiting = executor.submit(
                () -> flow.acquire("dyn", RateLimitContext.empty(), 1, Duration.ofSeconds(30)));
        awaitTrue(() -> flow.waitQueueDepth("dyn") == 1, Duration.ofSeconds(2), "waiter queued");

        // tariff upgrade mid-wait; the reload swaps the set, waking the waiter,
        // whose retry resolves the new (fast-refilling) limit
        resolved.set(new Limit(5, 1000, Duration.ofSeconds(1)));
        flow.replacePolicySet(set);

        Decision decision = waiting.get(10, TimeUnit.SECONDS);
        // under the old limit the retry would keep rejecting for a minute;
        // being freed at all proves the retry resolved the new limit
        assertTrue(decision.isAllowed());
        assertTrue(decision.remaining() >= 0 && decision.remaining() <= 4);
    }

    @Test
    void oversubscribedPolicyServesAllWaitersExactly() throws Exception {
        int waiters = 40;
        int capacity = 10;
        AtomicInteger allowed = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicLong listenerEvents = new AtomicLong();
        DefaultQuotaFlow metered = DefaultQuotaFlow
                .builder(PolicySet.compile(List.of(
                        throttlePolicy("m", capacity, capacity, Duration.ofMillis(100)))),
                        new LocalRateLimitStore())
                .addListener((decision, keyGroup) -> listenerEvents.incrementAndGet())
                .build();
        CountDownLatch ready = new CountDownLatch(waiters);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<Decision>> futures = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            int priority = i % 7; // mixed priorities must not lose or duplicate accounting
            futures.add(executor.submit(() -> {
                ready.countDown();
                assertTrue(go.await(10, TimeUnit.SECONDS));
                Decision d = metered.acquire("m", RateLimitContext.empty(), 1, Duration.ofSeconds(30), priority);
                if (d.isAllowed()) {
                    allowed.incrementAndGet();
                } else {
                    rejected.incrementAndGet();
                }
                return d;
            }));
        }
        assertTrue(ready.await(10, TimeUnit.SECONDS));
        go.countDown();
        long totalWaitNanos = 0;
        for (Future<Decision> future : futures) {
            totalWaitNanos += future.get(30, TimeUnit.SECONDS).waitDuration().toNanos();
        }
        assertEquals(waiters, allowed.get(), "every waiter must be served exactly once");
        assertEquals(0, rejected.get(), "zero cascading rejections");
        assertEquals(waiters, listenerEvents.get(), "exactly one listener event per final decision");
        assertTrue(totalWaitNanos > 0, "at least some waiters actually waited");
        awaitTrue(() -> metered.waitQueueDepth("m") == 0, Duration.ofSeconds(2), "queue drained");
    }

    @Test
    void interruptedWaiterStopsWaitingAndKeepsInterruptStatus() throws Exception {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        AtomicReference<Decision> outcome = new AtomicReference<>();
        AtomicReference<Boolean> interruptKept = new AtomicReference<>(false);
        CountDownLatch queued = new CountDownLatch(1);
        Thread waiterThread = new Thread(() -> {
            queued.countDown();
            outcome.set(flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(60)));
            interruptKept.set(Thread.currentThread().isInterrupted());
        });
        waiterThread.start();
        assertTrue(queued.await(5, TimeUnit.SECONDS));
        awaitTrue(() -> flow.waitQueueDepth("t") == 1, Duration.ofSeconds(2), "waiter queued");
        waiterThread.interrupt();
        waiterThread.join(10_000);
        assertFalse(waiterThread.isAlive(), "interrupted waiter kept waiting");
        Decision decision = outcome.get();
        assertFalse(decision.isAllowed());
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), decision.throttleRejection());
        assertTrue(interruptKept.get(), "interrupt status must be preserved");
        assertEquals(0, flow.waitQueueDepth("t"));
    }

    @Test
    void acquireAsyncWaitsAndCompletes() throws Exception {
        DefaultQuotaFlow flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMillis(60)));
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        Decision decision = flow
                .acquireAsync("t", RateLimitContext.empty(), 1, Duration.ofSeconds(5))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertTrue(decision.isAllowed());
        assertTrue(decision.waitDuration().compareTo(Duration.ZERO) > 0);
    }
}
