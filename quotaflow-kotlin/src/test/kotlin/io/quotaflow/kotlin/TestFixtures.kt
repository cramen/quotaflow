package io.quotaflow.kotlin

import io.quotaflow.core.Limit
import io.quotaflow.core.PolicySet
import io.quotaflow.core.RateLimitPolicy
import io.quotaflow.core.Reaction
import io.quotaflow.core.Scope
import io.quotaflow.core.store.LocalRateLimitStore
import java.time.Duration
import kotlinx.coroutines.delay

internal fun throttlePolicy(id: String, capacity: Long, refill: Long, period: Duration): RateLimitPolicy =
    policy(id, capacity, refill, period, Reaction.THROTTLE)

internal fun rejectPolicy(id: String, capacity: Long, refill: Long, period: Duration): RateLimitPolicy =
    policy(id, capacity, refill, period, Reaction.REJECT)

private fun policy(
    id: String,
    capacity: Long,
    refill: Long,
    period: Duration,
    reaction: Reaction,
): RateLimitPolicy =
    RateLimitPolicy.builder(id)
        .limit(Limit(capacity, refill, period))
        .scope(Scope.GLOBAL)
        .reaction(reaction)
        .build()

internal fun flowFor(policy: RateLimitPolicy, maxWaiters: Int = 1000): CoroutineQuotaFlow =
    CoroutineQuotaFlow.builder(PolicySet.compile(listOf(policy)), LocalRateLimitStore())
        .maxWaitersPerPolicy(maxWaiters)
        .build()

internal suspend fun awaitCondition(description: String, timeoutMillis: Long = 5_000, condition: () -> Boolean) {
    val deadline = System.nanoTime() + timeoutMillis * 1_000_000
    while (!condition()) {
        if (System.nanoTime() - deadline >= 0) {
            throw AssertionError("timed out waiting for: $description")
        }
        delay(2)
    }
}
