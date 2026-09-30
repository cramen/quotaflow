package io.quotaflow.core.store;

/** Immutable budget actually evaluated by a store; absent metadata means unknown, not zero. */
public record StoreBudget(long capacity, long remaining, boolean degraded) {
    public StoreBudget {
        if (capacity < 0 || remaining < 0 || remaining > capacity)
            throw new IllegalArgumentException("invalid observed budget");
    }
    public StoreBudget asDegraded() { return degraded ? this : new StoreBudget(capacity, remaining, true); }
}
