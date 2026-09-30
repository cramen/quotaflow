package io.quotaflow.core.store;

import io.quotaflow.core.Limit;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/** Conservative share of one validated distributed schedule, identical for both algorithms. */
public final class DegradedShare {
    private final long capacity;
    private final Limit localLimit;

    private DegradedShare(long capacity, Limit localLimit) {
        this.capacity = capacity;
        this.localLimit = localLimit;
    }

    public static DegradedShare of(Limit distributed, int instances) {
        Objects.requireNonNull(distributed, "distributed");
        if (instances < 1) throw new IllegalArgumentException("expected instances must be positive");
        long capacity = distributed.capacity() / instances;
        if (capacity == 0) return new DegradedShare(0, null);
        // Positive burst implies N <= C, so T*N <= T*C <= the validated horizon.
        long interval = Math.multiplyExact(distributed.emissionIntervalNanos(), instances);
        return new DegradedShare(capacity, new Limit(capacity, 1, Duration.ofNanos(interval)));
    }

    public long capacity() { return capacity; }
    public boolean canFit(long weight) {
        Limit.validateWeight(weight);
        return weight <= capacity;
    }
    public Optional<Limit> localLimit() { return Optional.ofNullable(localLimit); }
}
