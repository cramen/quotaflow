package io.quotaflow.core.store;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.util.concurrent.CompletionStage;

/**
 * Atomic counter storage for rate limit decisions. All atomicity lives behind
 * this SPI: a single {@code tryAcquire} call checks and consumes in one atomic
 * step, so no read-modify-write sequence is expressible through this surface.
 *
 * <p>Storage keys are namespaced as {@code <policyId>:<scope>:<rawKey>} by the
 * engine; implementations must treat them as opaque.
 */
public interface RateLimitStore {

    /**
     * Atomically consumes {@code weight} tokens from the bucket identified by
     * {@code storageKey} if enough remain; consumes nothing otherwise.
     *
     * @throws IllegalArgumentException if {@code weight} is less than 1
     */
    StoreResult tryAcquire(String storageKey, Limit limit, Algorithm algorithm, long weight);

    /** Asynchronous variant of {@link #tryAcquire}. */
    CompletionStage<StoreResult> tryAcquireAsync(String storageKey, Limit limit, Algorithm algorithm, long weight);
}
