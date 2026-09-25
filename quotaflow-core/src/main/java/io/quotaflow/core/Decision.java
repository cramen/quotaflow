package io.quotaflow.core;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable outcome of one rate limit evaluation. Rejections are data, never
 * exceptions.
 *
 * <p>{@code policyId} and {@code scope} identify the fired level: the level
 * that rejected the request, or the leaf level when the request was allowed.
 * {@code remaining} is the minimum remaining capacity across the evaluated
 * chain. {@code retryAfter} is present on store rejections (when the fired
 * level would admit the request) and empty for missing-key rejections, which
 * are not governed by a refill schedule.
 *
 * <p>{@code waitDuration} is the total time the caller spent queued by the
 * throttle machinery before this final decision; it is zero for instant
 * decisions. {@code throttleRejection} distinguishes throttle-specific
 * rejections (wait timeout, queue overflow) from ordinary quota rejections
 * and is empty on every decision that did not come out of a throttle wait.
 */
public record Decision(
        Verdict verdict,
        String policyId,
        Scope scope,
        long remaining,
        Optional<Duration> retryAfter,
        Duration waitDuration,
        Optional<ThrottleRejection> throttleRejection) {

    public Decision {
        Objects.requireNonNull(verdict, "verdict");
        Objects.requireNonNull(policyId, "policyId");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(retryAfter, "retryAfter");
        Objects.requireNonNull(waitDuration, "waitDuration");
        Objects.requireNonNull(throttleRejection, "throttleRejection");
        if (waitDuration.isNegative()) {
            throw new IllegalArgumentException("waitDuration must not be negative, got " + waitDuration);
        }
    }

    /** Instant decision: zero wait and no throttle rejection reason. */
    public Decision(
            Verdict verdict, String policyId, Scope scope, long remaining, Optional<Duration> retryAfter) {
        this(verdict, policyId, scope, remaining, retryAfter, Duration.ZERO, Optional.empty());
    }

    public boolean isAllowed() {
        return verdict == Verdict.ALLOWED;
    }

    /** Copy reporting the given total wait duration. */
    public Decision withWait(Duration waitDuration) {
        return new Decision(
                verdict, policyId, scope, remaining, retryAfter, waitDuration, throttleRejection);
    }

    /** Copy carrying the given throttle rejection reason. */
    public Decision withThrottleRejection(ThrottleRejection reason) {
        return new Decision(
                verdict, policyId, scope, remaining, retryAfter, waitDuration, Optional.of(reason));
    }

    public static Decision allowed(String policyId, Scope scope, long remaining) {
        return new Decision(Verdict.ALLOWED, policyId, scope, remaining, Optional.empty());
    }

    public static Decision rejected(String policyId, Scope scope, long remaining, Duration retryAfter) {
        return new Decision(
                Verdict.REJECTED, policyId, scope, remaining, Optional.of(retryAfter));
    }

    /** Rejection without a refill schedule (missing key). */
    public static Decision rejectedWithoutSchedule(String policyId, Scope scope) {
        return new Decision(Verdict.REJECTED, policyId, scope, 0, Optional.empty());
    }
}
