package io.quotaflow.kotlin

import io.quotaflow.core.Decision
import io.quotaflow.core.DecisionListener
import io.quotaflow.core.DefaultQuotaFlow
import io.quotaflow.core.KeyResolver
import io.quotaflow.core.LimitResolver
import io.quotaflow.core.PolicySet
import io.quotaflow.core.RateLimitContext
import io.quotaflow.core.store.RateLimitStore
import java.util.concurrent.Executor
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.future.await

/**
 * Coroutine-native facade over [DefaultQuotaFlow]. Every acquisition is a plain
 * `suspend` call built on the facade's asynchronous (`CompletionStage`) paths
 * via `await()`: the calling coroutine suspends and resumes, it never blocks or
 * parks a thread — including while waiting in throttle mode, where the actual
 * parked workers live in the facade's own daemon pool, invisible to coroutine
 * callers. The semantics (chain evaluation, degradation, throttle, decision
 * shape) are exactly those of the Java facade; rejections remain data, never
 * exceptions.
 *
 * Cancelling a waiting coroutine abandons the wait immediately: the coroutine
 * resumes with `CancellationException` without waiting for the throttle loop.
 *
 * Create instances through [builder] so the decision stream is wired into the
 * facade's listener pipeline.
 */
public class CoroutineQuotaFlow private constructor(
    /** The underlying Java facade, for configuration swaps and queue depth inspection. */
    public val delegate: DefaultQuotaFlow,
    internal val eventBus: DecisionEventBus,
) {

    /** Acquires one token against the chain containing [policyId]. */
    public suspend fun tryAcquire(policyId: String, context: RateLimitContext): Decision =
        tryAcquire(policyId, context, 1)

    /** Acquires [weight] tokens against the chain containing [policyId]. */
    public suspend fun tryAcquire(policyId: String, context: RateLimitContext, weight: Long): Decision =
        delegate.tryAcquireAsync(policyId, context, weight).await()

    /**
     * Acquires [weight] tokens, waiting up to [waitTimeout] when a throttle-mode
     * policy rejects. Priority resolves by the default chain: context attribute
     * [RateLimitContext.PRIORITY], then the policy's configured priority, then zero.
     */
    public suspend fun acquire(
        policyId: String,
        context: RateLimitContext,
        weight: Long,
        waitTimeout: Duration,
    ): Decision =
        delegate.acquireAsync(policyId, context, weight, waitTimeout.toJavaDuration()).await()

    /**
     * Acquires [weight] tokens, waiting up to [waitTimeout] when a throttle-mode
     * policy rejects, queued with the given explicit [priority] (higher is
     * served sooner; FIFO within equal priority).
     */
    public suspend fun acquire(
        policyId: String,
        context: RateLimitContext,
        weight: Long,
        waitTimeout: Duration,
        priority: Int,
    ): Decision =
        delegate.acquireAsync(policyId, context, weight, waitTimeout.toJavaDuration(), priority).await()

    /**
     * Stream of finalized limiter decisions, exactly one event per final
     * decision (an acquisition allowed after several throttle retries emits a
     * single event carrying the total wait duration). Identities are key-group
     * only; raw keys never appear on this stream.
     *
     * This is a convenience telemetry stream, not an audit log: it never
     * backpressures the limiter's decision path. A collector slower than the
     * decision rate loses intermediate events (oldest dropped first, bounded
     * buffer) instead of delaying decisions; the counters in the observability
     * module remain the lossless source. Cancelling collection unregisters the
     * underlying listener subscription.
     */
    public fun decisions(): Flow<DecisionEvent> = callbackFlow {
        val unsubscribe = eventBus.subscribe { event -> trySend(event) }
        awaitClose { unsubscribe() }
    }.buffer(capacity = Channel.BUFFERED, onBufferOverflow = BufferOverflow.DROP_OLDEST)

    public companion object {

        /** Starts a builder over the given compiled policy set and counter storage. */
        public fun builder(policySet: PolicySet, store: RateLimitStore): Builder =
            Builder(policySet, store)
    }

    /**
     * Mirrors [DefaultQuotaFlow.Builder]; [build] additionally wires the
     * decision stream listener, so [decisions] observes every final decision.
     */
    public class Builder internal constructor(policySet: PolicySet, store: RateLimitStore) {
        private val eventBus = DecisionEventBus()
        private val delegate = DefaultQuotaFlow.builder(policySet, store)

        /** Default resolver used for policies without a `keyResolverId`. */
        public fun defaultResolver(resolver: KeyResolver): Builder = apply {
            delegate.defaultResolver(resolver)
        }

        /** Registers a resolver under the id policies reference via `keyResolverId`. */
        public fun addResolver(id: String, resolver: KeyResolver): Builder = apply {
            delegate.addResolver(id, resolver)
        }

        /** Resolver for policies declaring a dynamic `limitRef`. */
        public fun limitResolver(limitResolver: LimitResolver): Builder = apply {
            delegate.limitResolver(limitResolver)
        }

        /** Adds an extra decision listener alongside the decision stream. */
        public fun addListener(listener: DecisionListener): Builder = apply {
            delegate.addListener(listener)
        }

        /** Bound of each throttle policy's waiter queue (default 1000). */
        public fun maxWaitersPerPolicy(maxWaitersPerPolicy: Int): Builder = apply {
            delegate.maxWaitersPerPolicy(maxWaitersPerPolicy)
        }

        /** Executor running the facade's asynchronous throttle waits. */
        public fun asyncExecutor(executor: Executor): Builder = apply {
            delegate.asyncExecutor(executor)
        }

        public fun build(): CoroutineQuotaFlow {
            delegate.addListener(eventBus)
            return CoroutineQuotaFlow(delegate.build(), eventBus)
        }
    }
}

/**
 * One finalized limiter decision as observed on the [CoroutineQuotaFlow.decisions]
 * stream: the [Decision] itself plus the [keyGroup] of the fired level (never
 * the raw key).
 */
public data class DecisionEvent(
    public val decision: Decision,
    public val keyGroup: String,
)
