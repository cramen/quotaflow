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
 */
public record Decision(
        Verdict verdict, String policyId, Scope scope, long remaining, Optional<Duration> retryAfter) {

    public Decision {
        Objects.requireNonNull(verdict, "verdict");
        Objects.requireNonNull(policyId, "policyId");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(retryAfter, "retryAfter");
    }

    public boolean isAllowed() {
        return verdict == Verdict.ALLOWED;
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
