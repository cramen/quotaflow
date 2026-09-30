package io.quotaflow.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.Limit;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.RateLimitContext;
import io.quotaflow.core.RateLimitPolicy;
import io.quotaflow.core.Reaction;
import io.quotaflow.core.Scope;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class MicrometerWaitMetricsTest {

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
    void waitDurationHistogramRecordsWaitedAndZeroWaitDecisions() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PolicySet policies = PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofMillis(60))));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore())
                .addListener(MicrometerDecisionListener.withStaticLimits(registry, () -> policies))
                .build();

        flow.tryAcquire("t", RateLimitContext.empty());
        assertTrue(flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(5)).isAllowed());

        flow.flushObservations().toCompletableFuture().orTimeout(2, TimeUnit.SECONDS).join();
        Timer timer = registry.get(QuotaFlowMetrics.WAIT_DURATION)
                .tags("policy", "t", "key-group", "global")
                .timer();
        // one instant allow (zero wait) and one waited allow
        assertEquals(2, timer.count());
        assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 20,
                "the waited decision must contribute its actual wait, got "
                        + timer.totalTime(TimeUnit.MILLISECONDS) + " ms");
        assertTrue(timer.max(TimeUnit.MILLISECONDS) >= 20);
    }

    @Test
    void waitTimeoutsAreCountedPerPolicyAndKeyGroup() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PolicySet policies = PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofMinutes(1))));
        var delivered = new java.util.concurrent.CountDownLatch(1);
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore())
                .addListener(MicrometerDecisionListener.withStaticLimits(registry, () -> policies))
                .addListener((decision, group) -> {
                    if (decision.throttleRejection().isPresent()) delivered.countDown();
                })
                .build();

        flow.tryAcquire("t", RateLimitContext.empty());
        flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofMillis(120));

        // Deadline finalization is independent of asynchronous telemetry delivery.
        assertTrue(delivered.await(2, TimeUnit.SECONDS));
        flow.flushObservations().toCompletableFuture().orTimeout(2, TimeUnit.SECONDS).join();
        assertEquals(1.0, registry.get(QuotaFlowMetrics.WAIT_TIMEOUTS)
                .tags("policy", "t", "key-group", "global").counter().count());
        // the timeout is also visible as an ordinary rejected decision
        assertEquals(1.0, registry.get(QuotaFlowMetrics.DECISIONS)
                .tags("result", "reject", "policy", "t", "key-group", "global").counter().count());
    }

    @Test
    void queueDepthGaugeTracksAccumulationAndDrain() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PolicySet policies = PolicySet.compile(List.of(
                throttlePolicy("t", 1, 1, Duration.ofMinutes(1)),
                RateLimitPolicy.builder("r")
                        .limit(new Limit(1, 1, Duration.ofMinutes(1)))
                        .scope(Scope.GLOBAL)
                        .build()));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore())
                .maxWaitersPerPolicy(10)
                .build();
        new MicrometerThrottleMetrics(registry, flow, () -> policies);

        // only throttle policies get a depth gauge
        assertNull(registry.find(QuotaFlowMetrics.WAIT_QUEUE_DEPTH).tags("policy", "r").gauge());
        var gauge = registry.get(QuotaFlowMetrics.WAIT_QUEUE_DEPTH).tags("policy", "t").gauge();
        assertEquals(0.0, gauge.value());

        flow.tryAcquire("t", RateLimitContext.empty());
        Future<?> first = executor.submit(
                () -> flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofMillis(600)));
        Future<?> second = executor.submit(
                () -> flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofMillis(600)));
        awaitTrue(() -> gauge.value() == 2.0, Duration.ofSeconds(2), "gauge tracks accumulation");

        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        awaitTrue(() -> gauge.value() == 0.0, Duration.ofSeconds(2), "gauge tracks the drain");
    }

    @Test void parentBlockedLeafCountsOneWaitAndCancellationHasNoTerminalSample() throws Exception {
        var registry = new SimpleMeterRegistry();
        var policies = PolicySet.compile(List.of(throttlePolicy("parent", 1, 1, Duration.ofHours(1)),
                RateLimitPolicy.builder("leaf").scope(Scope.USER).defaultKey("fixture").parentId("parent").reaction(Reaction.THROTTLE)
                        .limit(new Limit(10, 1, Duration.ofHours(1))).build()));
        try (var flow = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore()).maxWaitersPerPolicy(1)
                .addListener(new MicrometerDecisionListener(registry)).build()) {
            flow.tryAcquire("parent", RateLimitContext.empty());
            var waiter = flow.acquireAsync("leaf", RateLimitContext.empty(), 1, Duration.ofSeconds(5)).toCompletableFuture();
            awaitTrue(() -> flow.waitQueueDepth("leaf") == 1, Duration.ofSeconds(2), "leaf owns queue");
            var overflow = flow.acquire("leaf", RateLimitContext.empty(), 1, Duration.ofSeconds(1));
            assertEquals(io.quotaflow.core.ThrottleRejection.QUEUE_OVERFLOW, overflow.throttleRejection().orElseThrow());
            assertTrue(waiter.cancel(true));
            flow.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(0, flow.waitQueueDepth("leaf"));
            assertEquals(1, registry.get(QuotaFlowMetrics.DECISIONS).tags("result", "wait", "policy", "leaf").counter().count());
            assertEquals(2, registry.find(QuotaFlowMetrics.WAIT_DURATION).timers().stream().mapToLong(Timer::count).sum());
            assertEquals(0, flow.observationFailures());
        }
    }
    @Test void timeoutBeforeQueueEntryUsesTerminalDurationAndIgnoresLateStoreResult() throws Exception {
        var pending = new java.util.concurrent.CompletableFuture<io.quotaflow.core.store.StoreResult>();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var store = new io.quotaflow.core.store.RateLimitStore() {
            public java.util.concurrent.CompletionStage<Void> registerPolicies(List<io.quotaflow.core.store.PolicyBinding> bindings) { return java.util.concurrent.CompletableFuture.completedFuture(null); }
            public io.quotaflow.core.store.StoreResult tryAcquire(io.quotaflow.core.store.BucketIdentity key, Limit limit, io.quotaflow.core.Algorithm algorithm, long weight) { throw new AssertionError(); }
            public java.util.concurrent.CompletionStage<io.quotaflow.core.store.StoreResult> tryAcquireAsync(io.quotaflow.core.store.BucketIdentity key, Limit limit, io.quotaflow.core.Algorithm algorithm, long weight) { entered.countDown(); return pending; }
        };
        var registry = new SimpleMeterRegistry();
        var policies = PolicySet.compile(List.of(throttlePolicy("p", 1, 1, Duration.ofHours(1))));
        try (var flow = DefaultQuotaFlow.builder(policies, store).addListener(new MicrometerDecisionListener(registry)).build()) {
            var request = flow.acquireAsync("p", RateLimitContext.empty(), 1, Duration.ofMillis(300)).toCompletableFuture();
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            var decision = request.get(2, TimeUnit.SECONDS);
            pending.complete(io.quotaflow.core.store.StoreResult.acquired(0));
            flow.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(0, flow.waitQueueDepth("p"));
            assertNull(registry.find(QuotaFlowMetrics.DECISIONS).tag("result", "wait").counter());
            assertNull(registry.find(QuotaFlowMetrics.DECISIONS).tag("result", "allow").counter());
            var timer = registry.get(QuotaFlowMetrics.WAIT_DURATION).timer();
            assertEquals(1, timer.count());
            assertEquals(decision.waitDuration().toNanos(), timer.totalTime(TimeUnit.NANOSECONDS), 1);
            assertEquals(1, registry.get(QuotaFlowMetrics.WAIT_TIMEOUTS).counter().count());
        }
    }
}
