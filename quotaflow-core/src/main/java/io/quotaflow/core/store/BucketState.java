package io.quotaflow.core.store;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.util.Objects;

/**
 * A snapshot of one locally tracked bucket: the engine storage key, the limit
 * and algorithm the bucket was last evaluated with, and the whole tokens
 * remaining at snapshot time. Used to replay degraded-mode consumption into a
 * recovered distributed store via {@link StateSeeder}.
 */
public record BucketState(String storageKey, Limit limit, Algorithm algorithm, long remaining) {

    public BucketState {
        Objects.requireNonNull(storageKey, "storageKey");
        Objects.requireNonNull(limit, "limit");
        Objects.requireNonNull(algorithm, "algorithm");
        if (remaining < 0) {
            throw new IllegalArgumentException("remaining must be >= 0, got " + remaining);
        }
    }
}
