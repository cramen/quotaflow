package io.quotaflow.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.store.BatchRateLimitStore;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.StateSeeder;
import io.quotaflow.core.store.StoreResult;
import io.quotaflow.fallback.DegradationState;
import io.quotaflow.fallback.FallbackConfig;
import io.quotaflow.fallback.FallbackRateLimitStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MicrometerDegradationListenerTest {

    private static final Limit LIMIT = new Limit(100, 100, Duration.ofSeconds(1));
    private static final String KEY = "policy:global:shared";

    /** Primary store whose health the test flips to simulate an outage. */
    private static final class ControllablePrimary implements BatchRateLimitStore {
        private volatile boolean healthy = true;

        void fail() {
            healthy = false;
        }

        void heal() {
            healthy = true;
        }

        @Override
        public StoreResult tryAcquire(String storageKey, Limit limit, Algorithm algorithm, long weight) {
            if (!healthy) {
                throw new RuntimeException("primary store unavailable");
            }
            return StoreResult.acquired(limit.capacity() - weight);
        }

        @Override
        public CompletionStage<StoreResult> tryAcquireAsync(
                String storageKey, Limit limit, Algorithm algorithm, long weight) {
            return CompletableFuture.completedFuture(tryAcquire(storageKey, limit, algorithm, weight));
        }

        @Override
        public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
            if (!healthy) {
                throw new RuntimeException("primary store unavailable");
            }
            return CompletableFuture.completedFuture(ChainResult.acquired(chain.size() - 1, 50));
        }
    }

    @Test
    void outageAndRecoveryMoveTheGaugeAndCountFallbackDecisions() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerDegradationListener listener = new MicrometerDegradationListener(registry);
        ControllablePrimary primary = new ControllablePrimary();
        AtomicLong clock = new AtomicLong();
        FallbackConfig config =
                new FallbackConfig(1, Duration.ofMillis(100), Duration.ofSeconds(1), 2, 100);
        FallbackRateLimitStore store = new FallbackRateLimitStore(
                primary,
                new LocalRateLimitStore(),
                StateSeeder.noOp(),
                config,
                List.of(listener),
                clock::get,
                key -> "global");

        assertEquals(0.0, degraded(registry));

        // healthy: served by the primary, no fallback decisions
        store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1);
        assertEquals(0.0, degraded(registry));
        assertEquals(0.0, fallbackDecisions(registry));

        // outage: the threshold of 1 trips the breaker on the first failure
        primary.fail();
        store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1);
        assertEquals(DegradationState.OPEN, store.state());
        assertEquals(1.0, degraded(registry));
        assertEquals(1.0, fallbackDecisions(registry));

        store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1);
        assertEquals(2.0, fallbackDecisions(registry));

        // recovery: after the open duration the next request probes the now
        // healthy primary, seeding succeeds and the breaker closes
        primary.heal();
        clock.addAndGet(Duration.ofMillis(150).toNanos());
        store.tryAcquire(KEY, LIMIT, Algorithm.TOKEN_BUCKET, 1);
        assertEquals(DegradationState.CLOSED, store.state());
        assertEquals(0.0, degraded(registry));
        assertEquals(2.0, fallbackDecisions(registry));
    }

    private static double degraded(SimpleMeterRegistry registry) {
        return registry.get(QuotaFlowMetrics.DEGRADED).gauge().value();
    }

    private static double fallbackDecisions(SimpleMeterRegistry registry) {
        var counter = registry.find(QuotaFlowMetrics.FALLBACK_DECISIONS)
                .tags("policy", "policy", "key-group", "global")
                .counter();
        return counter == null ? 0.0 : counter.count();
    }
}
