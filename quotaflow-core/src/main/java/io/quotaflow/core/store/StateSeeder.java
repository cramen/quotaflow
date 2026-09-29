package io.quotaflow.core.store;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Best-effort replay of locally accumulated bucket state into a distributed
 * store after an outage. Implementations merge conservatively: seeding must
 * never increase the remaining capacity a store already holds for a key.
 *
 * <p>The explicit domain groups canonical buckets for replay. No requesting
 * leaf participates in storage placement.
 */
@FunctionalInterface
public interface StateSeeder {

    /**
     * Seeds {@code buckets} into the distributed store. The returned stage
     * completes when the seed writes have been flushed; an exceptional
     * completion signals that recovery must not proceed yet.
     */
    CompletionStage<Void> seed(QuotaDomain domain, List<BucketState> buckets);

    /** A seeder that does nothing, for primaries without seedable state. */
    static StateSeeder noOp() {
        return (domain, buckets) ->
                CompletableFuture.completedFuture(null);
    }
}
