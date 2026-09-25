package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Decision;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.Limit;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.RateLimitContext;
import io.quotaflow.core.RateLimitPolicy;
import io.quotaflow.core.Reaction;
import io.quotaflow.core.Scope;
import io.quotaflow.core.ThrottleRejection;
import io.quotaflow.core.store.BatchRateLimitStore;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.StateSeeder;
import io.quotaflow.core.store.StoreResult;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/**
 * Throttle mode under degradation: the facade's wait/retry cycle runs
 * unchanged above the engine when the store is the fallback wrapper, deciding
 * against the conservative local limiter while the primary is down. The
 * caller's wait timeout still bounds the total wait.
 */
class FallbackThrottleTest {

    /** Primary that is permanently down: every call fails immediately. */
    private static final class DownPrimary implements BatchRateLimitStore {
        @Override
        public StoreResult tryAcquire(String storageKey, Limit limit, Algorithm algorithm, long weight) {
            throw new IllegalStateException("primary is down");
        }

        @Override
        public CompletionStage<StoreResult> tryAcquireAsync(
                String storageKey, Limit limit, Algorithm algorithm, long weight) {
            throw new IllegalStateException("primary is down");
        }

        @Override
        public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
            throw new IllegalStateException("primary is down");
        }
    }

    private static RateLimitPolicy throttlePolicy(String id, long capacity, long refill, Duration period) {
        return RateLimitPolicy.builder(id)
                .limit(new Limit(capacity, refill, period))
                .scope(Scope.GLOBAL)
                .reaction(Reaction.THROTTLE)
                .build();
    }

    /** Fallback wrapper that trips its breaker on the first primary failure. */
    private static FallbackRateLimitStore degradedStore() {
        FallbackConfig config =
                new FallbackConfig(1, Duration.ofMinutes(5), Duration.ofMinutes(5), 1, 100);
        return new FallbackRateLimitStore(
                new DownPrimary(), StateSeeder.noOp(), config, List.of());
    }

    @Test
    void degradedThrottleWaitsAndIsServedByLocalLimiter() {
        FallbackRateLimitStore store = degradedStore();
        DefaultQuotaFlow flow = DefaultQuotaFlow
                .builder(PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofMillis(50)))), store)
                .build();

        // the first call trips the breaker; the decision is served locally
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        assertEquals(DegradationState.OPEN, store.state());

        long start = System.nanoTime();
        Decision decision = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(5));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(decision.isAllowed(), "degraded local limiter should serve the waiter");
        assertTrue(decision.waitDuration().toMillis() >= 20,
                "the waiter queued against the local limiter, got " + decision.waitDuration());
        assertTrue(elapsedMillis < 5_000);
        assertEquals(DegradationState.OPEN, store.state());
    }

    @Test
    void degradedThrottleWaitTimeoutStillBoundsTotalWait() {
        FallbackRateLimitStore store = degradedStore();
        DefaultQuotaFlow flow = DefaultQuotaFlow
                .builder(PolicySet.compile(List.of(throttlePolicy("t", 1, 1, Duration.ofMinutes(1)))), store)
                .build();
        assertTrue(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
        assertEquals(DegradationState.OPEN, store.state());

        long start = System.nanoTime();
        Decision decision = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofMillis(150));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertFalse(decision.isAllowed());
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), decision.throttleRejection());
        assertTrue(elapsedMillis >= 100, "returned after " + elapsedMillis + " ms");
        assertTrue(elapsedMillis < 3_000,
                "degradation extended the caller beyond its deadline: " + elapsedMillis + " ms");
    }
}
