package io.quotaflow.core;

import java.util.concurrent.CompletionStage;

/**
 * Stable programmatic entry point of the library. Rejections are returned as
 * {@link Decision} values, never thrown; exceptions are reserved for caller
 * misuse (unknown policy id, invalid weight).
 */
public interface QuotaFlow {

    /** Acquires one token against the chain containing {@code policyId}. */
    Decision tryAcquire(String policyId, RateLimitContext context);

    /** Acquires {@code weight} tokens against the chain containing {@code policyId}. */
    Decision tryAcquire(String policyId, RateLimitContext context, long weight);

    /** Asynchronous variant of {@link #tryAcquire(String, RateLimitContext, long)}. */
    CompletionStage<Decision> tryAcquireAsync(String policyId, RateLimitContext context, long weight);
}
