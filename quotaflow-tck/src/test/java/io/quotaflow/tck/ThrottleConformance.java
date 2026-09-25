package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.quotaflow.core.Decision;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.Limit;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.RateLimitContext;
import io.quotaflow.core.RateLimitPolicy;
import io.quotaflow.core.Reaction;
import io.quotaflow.core.Scope;
import io.quotaflow.core.ThrottleRejection;
import io.quotaflow.core.store.RateLimitStore;
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
import java.util.function.BooleanSupplier;

/**
 * Shared throttle conformance scenarios, run against the in-memory store and
 * the Redis-backed store with per-store timing profiles. Every scenario uses
 * a unique policy id so the stores can be shared without state bleed.
 */
final class ThrottleConformance {

    /** Per-store timing: local runs fast, Redis gets margins for round-trips. */
    record Profile(Duration quickRefill, Duration slowRefill, Duration waitTimeout, Duration pollBound) {
        static Profile local() {
            return new Profile(Duration.ofMillis(100), Duration.ofMinutes(1),
                    Duration.ofSeconds(15), Duration.ofSeconds(3));
        }

        static Profile redis() {
            return new Profile(Duration.ofMillis(300), Duration.ofMinutes(1),
                    Duration.ofSeconds(30), Duration.ofSeconds(10));
        }
    }

    private ThrottleConformance() {
    }

    private static RateLimitPolicy throttlePolicy(String id, long capacity, long refill, Duration period) {
        return RateLimitPolicy.builder(id)
                .limit(new Limit(capacity, refill, period))
                .scope(Scope.GLOBAL)
                .reaction(Reaction.THROTTLE)
                .build();
    }

    private static void awaitTrue(BooleanSupplier condition, Duration timeout, String description)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline >= 0) {
                fail("timed out waiting for: " + description);
            }
            Thread.sleep(5);
        }
    }

    /**
     * Burst absorbed as latency: 4x oversubscription against a throttle policy
     * serves every waiter within its wait timeout, with zero rejections.
     */
    static void oversubscribedPolicyServesAllWaiters(
            RateLimitStore store, String policyId, Profile profile) throws Exception {
        int capacity = 5;
        int waiters = 20;
        PolicySet policies = PolicySet.compile(List.of(
                throttlePolicy(policyId, capacity, capacity, profile.quickRefill())));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, store).build();
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            AtomicInteger allowed = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();
            CountDownLatch ready = new CountDownLatch(waiters);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Decision>> futures = new ArrayList<>();
            for (int i = 0; i < waiters; i++) {
                int priority = i % 4;
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    assertTrue(go.await(30, TimeUnit.SECONDS));
                    Decision decision = flow.acquire(
                            policyId, RateLimitContext.empty(), 1, profile.waitTimeout(), priority);
                    if (decision.isAllowed()) {
                        allowed.incrementAndGet();
                    } else {
                        rejected.incrementAndGet();
                    }
                    return decision;
                }));
            }
            assertTrue(ready.await(30, TimeUnit.SECONDS));
            go.countDown();
            for (Future<Decision> future : futures) {
                Decision decision = future.get(
                        profile.waitTimeout().plus(profile.pollBound()).toMillis(), TimeUnit.MILLISECONDS);
                assertTrue(decision.waitDuration().compareTo(profile.waitTimeout()) < 0,
                        "caller held beyond its own wait timeout");
            }
            assertEquals(waiters, allowed.get(), "every waiter served exactly once");
            assertEquals(0, rejected.get(), "zero cascading failures");
            awaitTrue(() -> flow.waitQueueDepth(policyId) == 0, profile.pollBound(), "queue drained");
        } finally {
            executor.shutdownNow();
        }
    }

    /** A full waiter queue rejects the next arrival immediately. */
    static void queueOverflowRejectsImmediately(
            RateLimitStore store, String policyId, Profile profile) throws Exception {
        PolicySet policies = PolicySet.compile(List.of(
                throttlePolicy(policyId, 1, 1, profile.slowRefill())));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, store)
                .maxWaitersPerPolicy(2)
                .build();
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            assertTrue(flow.tryAcquire(policyId, RateLimitContext.empty()).isAllowed());
            Future<Decision> first = executor.submit(() -> flow.acquire(
                    policyId, RateLimitContext.empty(), 1, Duration.ofSeconds(3)));
            Future<Decision> second = executor.submit(() -> flow.acquire(
                    policyId, RateLimitContext.empty(), 1, Duration.ofSeconds(3)));
            awaitTrue(() -> flow.waitQueueDepth(policyId) == 2, profile.pollBound(), "queue filled");

            long start = System.nanoTime();
            Decision overflow =
                    flow.acquire(policyId, RateLimitContext.empty(), 1, profile.waitTimeout());
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            assertFalse(overflow.isAllowed());
            assertEquals(Optional.of(ThrottleRejection.QUEUE_OVERFLOW), overflow.throttleRejection());
            assertEquals(Duration.ZERO, overflow.waitDuration());
            assertTrue(elapsedMillis < profile.pollBound().toMillis(),
                    "overflow must reject immediately, took " + elapsedMillis + " ms");

            assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT),
                    first.get(30, TimeUnit.SECONDS).throttleRejection());
            assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT),
                    second.get(30, TimeUnit.SECONDS).throttleRejection());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Under contention, the higher-priority waiter is served first. */
    static void highPriorityWaiterServedFirst(
            RateLimitStore store, String policyId, Profile profile) throws Exception {
        PolicySet policies = PolicySet.compile(List.of(
                throttlePolicy(policyId, 1, 1, profile.quickRefill())));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, store).build();
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            assertTrue(flow.tryAcquire(policyId, RateLimitContext.empty()).isAllowed());
            List<String> servedOrder = new CopyOnWriteArrayList<>();
            Future<?> low = executor.submit(() -> {
                assertTrue(flow.acquire(policyId, RateLimitContext.empty(), 1,
                        profile.waitTimeout(), 0).isAllowed());
                servedOrder.add("low");
            });
            awaitTrue(() -> flow.waitQueueDepth(policyId) == 1, profile.pollBound(), "low queued");
            Future<?> high = executor.submit(() -> {
                assertTrue(flow.acquire(policyId, RateLimitContext.empty(), 1,
                        profile.waitTimeout(), 10).isAllowed());
                servedOrder.add("high");
            });
            high.get(profile.waitTimeout().plus(profile.pollBound()).toMillis(), TimeUnit.MILLISECONDS);
            low.get(profile.waitTimeout().plus(profile.pollBound()).toMillis(), TimeUnit.MILLISECONDS);
            assertEquals(List.of("high", "low"), servedOrder);
        } finally {
            executor.shutdownNow();
        }
    }

    /** A waiter that cannot be served in time is rejected with wait-timeout. */
    static void waitTimeoutRejects(RateLimitStore store, String policyId, Profile profile)
            throws Exception {
        PolicySet policies = PolicySet.compile(List.of(
                throttlePolicy(policyId, 1, 1, profile.slowRefill())));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, store).build();
        assertTrue(flow.tryAcquire(policyId, RateLimitContext.empty()).isAllowed());

        Duration waitTimeout = Duration.ofMillis(400);
        long start = System.nanoTime();
        Decision decision = flow.acquire(policyId, RateLimitContext.empty(), 1, waitTimeout);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertFalse(decision.isAllowed());
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), decision.throttleRejection());
        assertTrue(decision.waitDuration().toMillis() >= 200,
                "wait duration should reflect the elapsed wait, got " + decision.waitDuration());
        assertTrue(elapsedMillis < 3_000,
                "caller held past its wait timeout: " + elapsedMillis + " ms");
    }
}
