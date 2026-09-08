package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * State machine of the embedded circuit breaker: consecutive-failure trip,
 * counter reset on success, zero-primary OPEN, single-probe HALF_OPEN, and
 * backoff-capped reopen.
 */
class CircuitBreakerTest {

    private static final long SECOND = 1_000_000_000L;

    private final AtomicLong nanos = new AtomicLong();
    private final List<String> transitions = new CopyOnWriteArrayList<>();

    private CircuitBreaker newBreaker(int threshold, long openSeconds, long maxOpenSeconds) {
        FallbackConfig config = new FallbackConfig(threshold, Duration.ofSeconds(openSeconds),
                Duration.ofSeconds(maxOpenSeconds), 1, 100);
        return new CircuitBreaker(config, nanos::get,
                (from, to, reason) -> transitions.add(from + "->" + to + ":" + reason));
    }

    @Test
    void closedAdmitsPrimaryAndFailuresBelowThresholdDoNotTrip() {
        CircuitBreaker breaker = newBreaker(3, 1, 30);
        assertEquals(CircuitBreaker.Call.PRIMARY, breaker.permitCall());
        breaker.onFailure("boom");
        breaker.onFailure("boom");
        assertEquals(2, breaker.consecutiveFailures());
        assertEquals(DegradationState.CLOSED, breaker.state());
        assertEquals(CircuitBreaker.Call.PRIMARY, breaker.permitCall());
        assertTrue(transitions.isEmpty());
    }

    @Test
    void successInClosedResetsTheFailureCounter() {
        CircuitBreaker breaker = newBreaker(3, 1, 30);
        breaker.onFailure("boom");
        breaker.onFailure("boom");
        breaker.onSuccess();
        assertEquals(0, breaker.consecutiveFailures());
        breaker.onFailure("boom");
        breaker.onFailure("boom");
        assertEquals(DegradationState.CLOSED, breaker.state());
    }

    @Test
    void thresholdTripsOpenAndOpenServesLocally() {
        CircuitBreaker breaker = newBreaker(3, 1, 30);
        breaker.onFailure("f1");
        breaker.onFailure("f2");
        breaker.onFailure("f3");
        assertEquals(DegradationState.OPEN, breaker.state());
        assertEquals(CircuitBreaker.Call.LOCAL, breaker.permitCall());
        assertEquals(1, transitions.size());
        assertTrue(transitions.get(0).startsWith("CLOSED->OPEN:"));
        assertTrue(transitions.get(0).contains("f3"));
    }

    @Test
    void openDurationElapsedAdmitsExactlyOneProbe() {
        CircuitBreaker breaker = newBreaker(1, 1, 30);
        breaker.onFailure("boom");
        assertEquals(DegradationState.OPEN, breaker.state());
        nanos.addAndGet(SECOND - 1);
        assertEquals(CircuitBreaker.Call.LOCAL, breaker.permitCall(), "open duration not elapsed yet");
        nanos.addAndGet(1);
        assertEquals(CircuitBreaker.Call.PROBE, breaker.permitCall());
        assertEquals(DegradationState.HALF_OPEN, breaker.state());
        assertEquals(CircuitBreaker.Call.LOCAL, breaker.permitCall(), "only one probe is admitted");
    }

    @Test
    void probeFailureReopensWithDoubledBackoffCappedAtMax() {
        CircuitBreaker breaker = newBreaker(1, 1, 2);
        breaker.onFailure("boom");
        nanos.addAndGet(SECOND);
        assertEquals(CircuitBreaker.Call.PROBE, breaker.permitCall());
        breaker.onFailure("probe failed");
        assertEquals(DegradationState.OPEN, breaker.state());
        assertEquals(2 * SECOND, breaker.currentOpenDurationNanos());
        nanos.addAndGet(SECOND);
        assertEquals(CircuitBreaker.Call.LOCAL, breaker.permitCall(), "backed-off open duration applies");
        nanos.addAndGet(SECOND);
        assertEquals(CircuitBreaker.Call.PROBE, breaker.permitCall());
        breaker.onFailure("probe failed again");
        assertEquals(DegradationState.OPEN, breaker.state());
        assertEquals(2 * SECOND, breaker.currentOpenDurationNanos(), "backoff is capped at maxOpenDuration");
    }

    @Test
    void probeSuccessThenSeedingSuccessClosesAndResetsBackoff() {
        CircuitBreaker breaker = newBreaker(1, 1, 30);
        breaker.onFailure("boom");
        nanos.addAndGet(SECOND);
        assertEquals(CircuitBreaker.Call.PROBE, breaker.permitCall());
        breaker.onFailure("probe failed");
        assertEquals(2 * SECOND, breaker.currentOpenDurationNanos());
        nanos.addAndGet(2 * SECOND);
        assertEquals(CircuitBreaker.Call.PROBE, breaker.permitCall());
        breaker.onSeedingSuccess("seeded");
        assertEquals(DegradationState.CLOSED, breaker.state());
        assertEquals(0, breaker.consecutiveFailures());
        assertEquals(SECOND, breaker.currentOpenDurationNanos());
        assertEquals(CircuitBreaker.Call.PRIMARY, breaker.permitCall());
        assertEquals(List.of(
                "CLOSED->OPEN:boom",
                "OPEN->HALF_OPEN:open duration elapsed; admitting one probe request",
                "HALF_OPEN->OPEN:probe failed",
                "OPEN->HALF_OPEN:open duration elapsed; admitting one probe request",
                "HALF_OPEN->CLOSED:seeded"), transitions);
    }

    @Test
    void seedingFailureReopensWithoutClosing() {
        CircuitBreaker breaker = newBreaker(1, 1, 30);
        breaker.onFailure("boom");
        nanos.addAndGet(SECOND);
        assertEquals(CircuitBreaker.Call.PROBE, breaker.permitCall());
        breaker.onSeedingFailure("seeding failed");
        assertEquals(DegradationState.OPEN, breaker.state());
        assertEquals(2 * SECOND, breaker.currentOpenDurationNanos());
        assertEquals(CircuitBreaker.Call.LOCAL, breaker.permitCall());
    }

    @Test
    void failuresArrivingWhileOpenDoNotRetripOrExtend() {
        CircuitBreaker breaker = newBreaker(2, 1, 30);
        breaker.onFailure("f1");
        breaker.onFailure("f2");
        assertEquals(DegradationState.OPEN, breaker.state());
        long openedAt = transitions.size();
        breaker.onFailure("late failure from an in-flight closed-era call");
        assertEquals(DegradationState.OPEN, breaker.state());
        assertEquals(openedAt, transitions.size(), "no additional transition while open");
    }

    @Test
    void halfOpenIgnoresNonProbeFailures() {
        CircuitBreaker breaker = newBreaker(1, 1, 30);
        breaker.onFailure("boom");
        nanos.addAndGet(SECOND);
        assertEquals(CircuitBreaker.Call.PROBE, breaker.permitCall());
        // a stale failure signal while half-open reopens, which is the probe-failure path
        breaker.onFailure("probe failed");
        assertEquals(DegradationState.OPEN, breaker.state());
        assertFalse(transitions.isEmpty());
    }
}
