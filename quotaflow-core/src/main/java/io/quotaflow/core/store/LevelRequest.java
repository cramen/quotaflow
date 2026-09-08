package io.quotaflow.core.store;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.util.Objects;

/**
 * One level of a policy chain as evaluated by a {@link BatchRateLimitStore}:
 * the engine-resolved storage key, the level's limit and algorithm, and the
 * requested weight. Levels are ordered root-to-leaf (broadest scope first).
 */
public record LevelRequest(String storageKey, Limit limit, Algorithm algorithm, long weight) {

    public LevelRequest {
        Objects.requireNonNull(storageKey, "storageKey");
        Objects.requireNonNull(limit, "limit");
        Objects.requireNonNull(algorithm, "algorithm");
        if (weight < 1) {
            throw new IllegalArgumentException("weight must be >= 1, got " + weight);
        }
    }
}
