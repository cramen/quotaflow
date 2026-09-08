package io.quotaflow.fallback;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Embedded circuit breaker for {@link FallbackRateLimitStore}. Lock-free,
 * driven entirely by caller threads: no threads are spawned, and the probe
 * rides a real request instead of a background timer.
 *
 * <p>CLOSED: calls go to the primary; {@link #onSuccess()} resets the
 * consecutive-failure counter and {@link #onFailure(String)} trips the breaker
 * OPEN at the configured threshold. OPEN: calls are served locally with zero
 * primary calls; once the open duration has elapsed the next caller flips the
 * breaker HALF_OPEN. HALF_OPEN: exactly one caller is admitted as a probe;
 * probe success leaves the breaker HALF_OPEN while the wrapper replays local
 * state ({@link #onSeedingSuccess(String)} closes, {@link #onSeedingFailure}
 * reopens), probe failure reopens with the open duration doubled up to a cap.
 */
final class CircuitBreaker {

    /** Which store a caller may use for its request. */
    enum Call {
        PRIMARY,
        PROBE,
        LOCAL
    }

    interface TransitionListener {
        void onTransition(DegradationState from, DegradationState to, String reason);
    }

    private final int failureThreshold;
    private final long baseOpenDurationNanos;
    private final long maxOpenDurationNanos;
    private final LongSupplier nanoClock;
    private final TransitionListener transitionListener;

    private final AtomicReference<DegradationState> state = new AtomicReference<>(DegradationState.CLOSED);
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private final AtomicLong openedAtNanos = new AtomicLong();
    private final AtomicLong openDurationNanos;
    private final AtomicBoolean probeInFlight = new AtomicBoolean();

    CircuitBreaker(
            FallbackConfig config,
            LongSupplier nanoClock,
            TransitionListener transitionListener) {
        this.failureThreshold = config.failureThreshold();
        this.baseOpenDurationNanos = config.openDuration().toNanos();
        this.maxOpenDurationNanos = config.maxOpenDuration().toNanos();
        this.openDurationNanos = new AtomicLong(baseOpenDurationNanos);
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.transitionListener = Objects.requireNonNull(transitionListener, "transitionListener");
    }

    DegradationState state() {
        return state.get();
    }

    /** Visible for tests. */
    int consecutiveFailures() {
        return consecutiveFailures.get();
    }

    /** Visible for tests: the current (possibly backed-off) open duration in nanoseconds. */
    long currentOpenDurationNanos() {
        return openDurationNanos.get();
    }

    /**
     * Decides which store the caller may use, performing the time-based
     * OPEN -&gt; HALF_OPEN transition when the open duration has elapsed.
     */
    Call permitCall() {
        while (true) {
            switch (state.get()) {
                case CLOSED:
                    return Call.PRIMARY;
                case OPEN:
                    if (nanoClock.getAsLong() - openedAtNanos.get() < openDurationNanos.get()) {
                        return Call.LOCAL;
                    }
                    if (state.compareAndSet(DegradationState.OPEN, DegradationState.HALF_OPEN)) {
                        probeInFlight.set(true);
                        transitionListener.onTransition(
                                DegradationState.OPEN, DegradationState.HALF_OPEN,
                                "open duration elapsed; admitting one probe request");
                        return Call.PROBE;
                    }
                    break;
                case HALF_OPEN:
                    return probeInFlight.compareAndSet(false, true) ? Call.PROBE : Call.LOCAL;
            }
        }
    }

    /** Records a successful primary call; a success in CLOSED resets the failure counter. */
    void onSuccess() {
        if (state.get() == DegradationState.CLOSED) {
            consecutiveFailures.set(0);
        }
    }

    /**
     * Records a failed primary call. A probe failure reopens the breaker with
     * a doubled open duration (capped); a failure in CLOSED increments the
     * consecutive-failure counter and trips the breaker at the threshold.
     */
    void onFailure(String reason) {
        if (state.get() == DegradationState.HALF_OPEN) {
            reopen(reason);
            return;
        }
        if (state.get() != DegradationState.CLOSED) {
            return;
        }
        int failures = consecutiveFailures.incrementAndGet();
        if (failures >= failureThreshold
                && state.compareAndSet(DegradationState.CLOSED, DegradationState.OPEN)) {
            openedAtNanos.set(nanoClock.getAsLong());
            openDurationNanos.set(baseOpenDurationNanos);
            probeInFlight.set(false);
            transitionListener.onTransition(DegradationState.CLOSED, DegradationState.OPEN, reason);
        }
    }

    /** Closes the breaker after the probe succeeded and local state was seeded. */
    void onSeedingSuccess(String reason) {
        if (state.compareAndSet(DegradationState.HALF_OPEN, DegradationState.CLOSED)) {
            consecutiveFailures.set(0);
            openDurationNanos.set(baseOpenDurationNanos);
            probeInFlight.set(false);
            transitionListener.onTransition(DegradationState.HALF_OPEN, DegradationState.CLOSED, reason);
        }
    }

    /** Reopens the breaker when seeding could not complete; recovery is retried later. */
    void onSeedingFailure(String reason) {
        reopen(reason);
    }

    private void reopen(String reason) {
        if (state.compareAndSet(DegradationState.HALF_OPEN, DegradationState.OPEN)) {
            long backedOff = Math.min(maxOpenDurationNanos, openDurationNanos.get() * 2);
            openDurationNanos.set(backedOff);
            openedAtNanos.set(nanoClock.getAsLong());
            probeInFlight.set(false);
            transitionListener.onTransition(DegradationState.HALF_OPEN, DegradationState.OPEN, reason);
        }
    }
}
