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
    val policies = quotaFlow { policy("kotlin") { scope = Scope.GLOBAL; limit(capacity = 2, refill = 1 every 1.hours) } }
    val flow = CoroutineQuotaFlow.builder(policies, LocalRateLimitStore()).build()
    try {
        val events = async(start = CoroutineStart.UNDISPATCHED) { flow.decisions().take(3).toList() }
        yield()
        check(flow.tryAcquire("kotlin", RateLimitContext.empty()).isAllowed)
        check(flow.tryAcquire("kotlin", RateLimitContext.empty()).isAllowed)
        check(!flow.tryAcquire("kotlin", RateLimitContext.empty()).isAllowed)
        withTimeout(5_000) { check(events.await().size == 3) }
    } finally { flow.delegate.close() }
    println("PUBLISHED CONSUMERS VERIFIED case=" + System.getProperty("verification.case"))
}
