package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.RecoveryCohort;
import io.quotaflow.testing.RecoveryPrimaryFixture;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Local refill waiting remains bounded; recovery-readiness queue composition belongs to the lifecycle change. */
class FallbackThrottleTest {
    private static PolicySet policy(Duration period) {
        return PolicySet.compile(List.of(RateLimitPolicy.builder("t").scope(Scope.GLOBAL).reaction(Reaction.THROTTLE)
                .limit(new Limit(1, 1, period)).build()));
    }
    private static RecoverySettings settings() {
        return new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 100, 100,
                Duration.ofMillis(20), Duration.ofSeconds(1));
    }
    @Test void enrolledDegradedThrottleWaitsForEarnedLocalCredit() throws Exception {
        var policies = policy(Duration.ofMillis(100)); var primary = new RecoveryPrimaryFixture(policies);
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of())) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false;
            assertFalse(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
            assertFalse(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
            Decision decision = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofSeconds(5));
            assertTrue(decision.isAllowed());
            assertTrue(decision.waitDuration().toMillis() >= 20);
            assertEquals(DegradationState.OPEN, store.state());
        }
    }
    @Test void enrolledDegradedThrottleKeepsTheCallerWaitBound() throws Exception {
        var policies = policy(Duration.ofMinutes(1)); var primary = new RecoveryPrimaryFixture(policies);
        try (var store = new FallbackRateLimitStore(primary, settings(), List.of())) {
            var flow = DefaultQuotaFlow.builder(policies, store).build();
            FallbackRateLimitStoreTest.await(() -> store.state() == DegradationState.CLOSED);
            primary.available = false;
            assertFalse(flow.tryAcquire("t", RateLimitContext.empty()).isAllowed());
            long started = System.nanoTime();
            Decision decision = flow.acquire("t", RateLimitContext.empty(), 1, Duration.ofMillis(150));
            assertFalse(decision.isAllowed());
            assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), decision.throttleRejection());
            long elapsed = (System.nanoTime() - started) / 1_000_000;
            assertTrue(elapsed >= 100 && elapsed < 3_000, "wait duration: " + elapsed);
        }
    }
}
