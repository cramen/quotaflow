package io.quotaflow.core.store;

/**
 * Outcome of one atomic store acquisition. On rejection
 * {@code retryAfterMillis} is the positive delay after which the same request
 * could fit after refill; zero on rejection means no refill schedule exists.
 * It is also zero when {@code acquired} is true.
 */
public record StoreResult(boolean acquired, long remaining, long retryAfterMillis, RecoveryPending recoveryPending, StoreBudget budget) {

    public StoreResult(boolean acquired, long remaining, long retryAfterMillis, RecoveryPending pending) {
        this(acquired, remaining, retryAfterMillis, pending, null);
    }

    public StoreResult(boolean acquired, long remaining, long retryAfterMillis) {
        this(acquired, remaining, retryAfterMillis, null);
    }

    public StoreResult {
        if (recoveryPending != null && (acquired || remaining != 0 || retryAfterMillis != 0 || budget != null)) {
            throw new IllegalArgumentException("recovery-pending outcome cannot grant quota or a refill schedule");
        }
    }

    public static StoreResult pending(RecoveryPending pending) {
        return new StoreResult(false, 0, 0, java.util.Objects.requireNonNull(pending, "pending"));
    }

    public static StoreResult acquired(long remaining) {
        return new StoreResult(true, remaining, 0);
    }

    public static StoreResult rejected(long remaining, long retryAfterMillis) {
        return new StoreResult(false, remaining, retryAfterMillis);
    }
}
