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
    void waitTimeoutsAreCountedPerPolicyAndKeyGroup() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PolicySet policies = PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofMinutes(1))));
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore())
                .addListener(MicrometerDecisionListener.withStaticLimits(registry, () -> policies))
                .build();

        flow.tryAcquire("t", RateLimitContext.empty());
        flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofMillis(120));

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
}
