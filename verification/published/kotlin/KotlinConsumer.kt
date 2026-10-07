package io.quotaflow.verification.published

import io.quotaflow.core.RateLimitContext
import io.quotaflow.core.Scope
import io.quotaflow.core.store.LocalRateLimitStore
import io.quotaflow.kotlin.CoroutineQuotaFlow
import io.quotaflow.kotlin.every
import io.quotaflow.kotlin.quotaFlow
import kotlin.time.Duration.Companion.hours
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

fun main() = runBlocking {
    check(Runtime.version().feature() == Integer.getInteger("verification.jdk"))
    val policies = quotaFlow {
        policy("kotlin") { scope = Scope.GLOBAL; limit(capacity = 2, refill = 1 every 1.hours) }
        policy("subscription-probe") { scope = Scope.GLOBAL; limit(capacity = 1, refill = 1 every 1.hours) }
    }
    val flow = CoroutineQuotaFlow.builder(policies, LocalRateLimitStore()).build()
    try {
        val subscribed = CompletableDeferred<Unit>()
        val events = async(start = CoroutineStart.UNDISPATCHED) {
            flow.decisions()
                .onEach { if (it.decision.policyId() == "subscription-probe") subscribed.complete(Unit) }
                .filter { it.decision.policyId() == "kotlin" }
                .take(3).toList()
        }
        withTimeout(5_000) {
            while (!subscribed.isCompleted) {
                flow.tryAcquire("subscription-probe", RateLimitContext.empty())
                yield()
            }
        }
        println("KOTLIN SUBSCRIPTION VERIFIED")
        check(flow.tryAcquire("kotlin", RateLimitContext.empty()).isAllowed)
        check(flow.tryAcquire("kotlin", RateLimitContext.empty()).isAllowed)
        check(!flow.tryAcquire("kotlin", RateLimitContext.empty()).isAllowed)
        withTimeout(5_000) { check(events.await().size == 3) }
    } finally { flow.delegate.close() }
    println("PUBLISHED CONSUMERS VERIFIED case=" + System.getProperty("verification.case"))
}
