package io.quotaflow.core.store;

/**
 * Outcome of one atomic store acquisition. On rejection
 * {@code retryAfterMillis} is the positive delay after which the same request
 * would be admitted; it is undefined (zero) when {@code acquired} is true.
 */
public record StoreResult(boolean acquired, long remaining, long retryAfterMillis) {

    public static StoreResult acquired(long remaining) {
        return new StoreResult(true, remaining, 0);
    }

    public static StoreResult rejected(long remaining, long retryAfterMillis) {
        return new StoreResult(false, remaining, retryAfterMillis);
    }
}
