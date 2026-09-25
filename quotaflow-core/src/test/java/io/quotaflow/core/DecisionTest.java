package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DecisionTest {

    @Test
    void legacyConstructorYieldsInstantDecision() {
        Decision decision = new Decision(
                Verdict.REJECTED, "p", Scope.GLOBAL, 0, Optional.of(Duration.ofMillis(5)));
        assertEquals(Duration.ZERO, decision.waitDuration());
        assertTrue(decision.throttleRejection().isEmpty());
    }

    @Test
    void withWaitAndThrottleRejectionCopyWithoutLosingFields() {
        Decision base = Decision.rejected("p", Scope.TENANT, 2, Duration.ofMillis(50));
        Decision waited = base.withWait(Duration.ofMillis(30));
        assertEquals(Duration.ofMillis(30), waited.waitDuration());
        assertEquals(2, waited.remaining());
        assertEquals(Optional.of(Duration.ofMillis(50)), waited.retryAfter());
        assertTrue(waited.throttleRejection().isEmpty());

        Decision timedOut = waited.withThrottleRejection(ThrottleRejection.WAIT_TIMEOUT);
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), timedOut.throttleRejection());
        assertEquals(Duration.ofMillis(30), timedOut.waitDuration());
        assertEquals(Verdict.REJECTED, timedOut.verdict());
        // the base decision is untouched
        assertEquals(Duration.ZERO, base.waitDuration());
    }

    @Test
    void negativeWaitDurationIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Decision(
                Verdict.ALLOWED, "p", Scope.GLOBAL, 1,
                Optional.empty(), Duration.ofNanos(-1), Optional.empty()));
    }

    @Test
    void nullComponentsAreRejected() {
        assertThrows(NullPointerException.class, () -> new Decision(
                Verdict.ALLOWED, "p", Scope.GLOBAL, 1, Optional.empty(), null, Optional.empty()));
        assertThrows(NullPointerException.class, () -> new Decision(
                Verdict.ALLOWED, "p", Scope.GLOBAL, 1, Optional.empty(), Duration.ZERO, null));
    }
}
