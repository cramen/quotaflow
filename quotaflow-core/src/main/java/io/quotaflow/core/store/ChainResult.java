package io.quotaflow.core.store;

/**
 * Outcome of one atomic chain evaluation by a {@link BatchRateLimitStore}.
 *
 * <p>{@code firedLevelIndex} is the index into the evaluated level list of the
 * level whose verdict the result reports: the first rejecting level, or the
 * last (leaf) level when the chain allowed the request.
 *
 * <p>{@code remaining} mirrors the per-level {@link StoreResult} contract so
 * decisions read identically on both evaluation paths: on rejection it is the
 * fired level's remaining capacity; when the chain allowed the request it is
 * the minimum remaining capacity across all levels.
 *
 * <p>{@code retryAfterMillis} is the positive delay after which the fired
 * level would admit the request; it is undefined (zero) when {@code acquired}
 * is true.
 */
public record ChainResult(boolean acquired, int firedLevelIndex, long remaining, long retryAfterMillis) {

    public static ChainResult acquired(int firedLevelIndex, long minRemaining) {
        return new ChainResult(true, firedLevelIndex, minRemaining, 0);
    }

    public static ChainResult rejected(int firedLevelIndex, long remaining, long retryAfterMillis) {
        return new ChainResult(false, firedLevelIndex, remaining, retryAfterMillis);
    }
}
