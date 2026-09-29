package io.quotaflow.core;

import java.time.Duration;
import java.util.Objects;

/** Immutable limit with a conservatively quantized, exactly represented refill schedule. */
public record Limit(long capacity, long refillAmount, Duration refillPeriod) {
    public static final long MAX_TOKENS = 1_000_000_000L;
    public static final long MAX_HORIZON_NANOS = 2_764_800_000_000_000L;
    public static final Duration MAX_PERIOD = Duration.ofDays(32);

    public Limit {
        validateCount(capacity, "capacity");
        validateCount(refillAmount, "refillAmount");
        Objects.requireNonNull(refillPeriod, "refillPeriod");
        if (refillPeriod.compareTo(Duration.ofNanos(1_000)) < 0 || refillPeriod.compareTo(MAX_PERIOD) > 0) {
            throw new IllegalArgumentException("refillPeriod must be between one microsecond and 32 days");
        }
        long nanos = refillPeriod.toNanos();
        if (nanos / refillAmount < 1_000) {
            throw new IllegalArgumentException("nominal emission interval must be at least one microsecond");
        }
        long interval = ceilDivide(nanos, refillAmount);
        if (capacity > MAX_HORIZON_NANOS / interval) {
            throw new IllegalArgumentException("full-refill horizon must not exceed 32 days");
        }
    }

    /** Whole nanoseconds per token, rounded upward by less than one nanosecond. */
    public long emissionIntervalNanos() {
        return ceilDivide(refillPeriod.toNanos(), refillAmount);
    }

    public static void validateWeight(long weight) {
        validateCount(weight, "weight");
    }

    private static void validateCount(long value, String name) {
        if (value < 1 || value > MAX_TOKENS) {
            throw new IllegalArgumentException(name + " must be between 1 and " + MAX_TOKENS + ", got " + value);
        }
    }

    private static long ceilDivide(long value, long divisor) {
        return value / divisor + (value % divisor == 0 ? 0 : 1);
    }
}
