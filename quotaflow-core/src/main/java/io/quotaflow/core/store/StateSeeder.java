package io.quotaflow.core.store;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Best-effort replay of locally accumulated bucket state into a distributed
 * store after an outage. Implementations merge conservatively: seeding must
 * never increase the remaining capacity a store already holds for a key.
 *
 * <p>{@code chainLeafStorageKey} identifies the chain the buckets were
 * evaluated under (the storage key of the chain's leaf level), so
 * implementations whose key layout depends on chain identity can address the
 * same state the chain evaluation writes. Buckets evaluated outside a chain
 * use their own storage key as the chain leaf.
 */
@FunctionalInterface
public interface StateSeeder {

    /**
     * Seeds {@code buckets} into the distributed store. The returned stage
     * completes when the seed writes have been flushed; an exceptional
     * completion signals that recovery must not proceed yet.
     */
    CompletionStage<Void> seed(String chainLeafStorageKey, List<BucketState> buckets);

    /** A seeder that does nothing, for primaries without seedable state. */
    static StateSeeder noOp() {
        return (chainLeafStorageKey, buckets) ->
                CompletableFuture.completedFuture(null);
    }
}
