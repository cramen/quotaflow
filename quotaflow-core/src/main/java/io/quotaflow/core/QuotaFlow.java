package io.quotaflow.core;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * Stable programmatic entry point of the library. Rejections are returned as
 * {@link Decision} values, never thrown; exceptions are reserved for caller
 * misuse (unknown policy id, invalid weight).
 *
 * <p>{@code tryAcquire} decides instantly. {@code acquire} adds the throttle
 * reaction: when the quota of a throttle-mode policy is exhausted, the caller
 * waits (virtual-thread-friendly parking, never beyond its own wait timeout)
 * and retries as quota refills, instead of being rejected immediately. The
 * final decision — allowed after waiting, or rejected with a
 * {@link ThrottleRejection} reason — is data, carries the total wait duration
 * and fires exactly one {@link DecisionListener} event.
 */
public interface QuotaFlow {

    /** Acquires one token against the chain containing {@code policyId}. */
    Decision tryAcquire(String policyId, RateLimitContext context);

    /** Acquires {@code weight} tokens against the chain containing {@code policyId}. */
    Decision tryAcquire(String policyId, RateLimitContext context, long weight);

    /** Asynchronous variant of {@link #tryAcquire(String, RateLimitContext, long)}. */
    CompletionStage<Decision> tryAcquireAsync(String policyId, RateLimitContext context, long weight);

    /**
     * Acquires {@code weight} tokens, waiting up to {@code waitTimeout} when a
     * throttle-mode policy rejects. Priority resolves by the default chain:
     * context attribute {@link RateLimitContext#PRIORITY}, then the policy's
     * configured priority, then zero.
     */
    Decision acquire(String policyId, RateLimitContext context, long weight, Duration waitTimeout);

    /**
     * Acquires {@code weight} tokens, waiting up to {@code waitTimeout} when a
     * throttle-mode policy rejects, queued with the given explicit priority
     * (higher is served sooner; FIFO within equal priority).
     */
    Decision acquire(String policyId, RateLimitContext context, long weight, Duration waitTimeout, int priority);

    /** Asynchronous variant of {@link #acquire(String, RateLimitContext, long, Duration)}. */
    CompletionStage<Decision> acquireAsync(
            String policyId, RateLimitContext context, long weight, Duration waitTimeout);

    /** Asynchronous variant of {@link #acquire(String, RateLimitContext, long, Duration, int)}. */
    CompletionStage<Decision> acquireAsync(
            String policyId, RateLimitContext context, long weight, Duration waitTimeout, int priority);
}
