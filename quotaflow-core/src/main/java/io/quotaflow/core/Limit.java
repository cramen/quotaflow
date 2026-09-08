package io.quotaflow.core;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable rate limit: up to {@code capacity} tokens, refilled by
 * {@code refillAmount} every {@code refillPeriod}.
 */
public record Limit(long capacity, long refillAmount, Duration refillPeriod) {

    public Limit {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1, got " + capacity);
        }
        if (refillAmount < 1) {
            throw new IllegalArgumentException("refillAmount must be >= 1, got " + refillAmount);
        }
        Objects.requireNonNull(refillPeriod, "refillPeriod");
        if (refillPeriod.isZero() || refillPeriod.isNegative()) {
            throw new IllegalArgumentException("refillPeriod must be positive, got " + refillPeriod);
        }
    }

    /** Emission interval in nanoseconds: time to regenerate one token. */
    public double emissionIntervalNanos() {
        return (double) refillPeriod.toNanos() / refillAmount;
    }
}
