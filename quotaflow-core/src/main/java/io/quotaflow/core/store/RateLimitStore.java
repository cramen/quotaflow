package io.quotaflow.core.store;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.util.concurrent.CompletionStage;
import java.util.List;

/**
 * Atomic counter storage for rate limit decisions. All atomicity lives behind
 * this SPI: a single {@code tryAcquire} call checks and consumes in one atomic
 * step, so no read-modify-write sequence is expressible through this surface.
 *
 * <p>Storage identities explicitly carry their namespace and root domain.
 * Implementations must preserve this identity across direct, chain and seed calls.
 */
public interface RateLimitStore {

    /**
     * Atomically validates/registers the complete candidate before policy publication.
     * Implementations retain removed bindings and reject scope/domain reassignment.
     * This is a control-plane operation, not an acquisition or a quota debit.
     */
    CompletionStage<Void> registerPolicies(List<PolicyBinding> bindings);


    /**
     * Atomically consumes {@code weight} tokens from the bucket identified by
     * {@code storageKey} if enough remain; consumes nothing otherwise.
     *
     * @throws IllegalArgumentException if {@code weight} is less than 1
     */
    StoreResult tryAcquire(BucketIdentity storageKey, Limit limit, Algorithm algorithm, long weight);

    /** Asynchronous variant of {@link #tryAcquire}. */
    CompletionStage<StoreResult> tryAcquireAsync(BucketIdentity storageKey, Limit limit, Algorithm algorithm, long weight);
}
