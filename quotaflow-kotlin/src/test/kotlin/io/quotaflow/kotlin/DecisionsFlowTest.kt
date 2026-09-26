package io.quotaflow.kotlin

import io.quotaflow.core.Limit
import io.quotaflow.core.PolicySet
import io.quotaflow.core.RateLimitContext
import io.quotaflow.core.RateLimitPolicy
import io.quotaflow.core.Reaction
import io.quotaflow.core.Scope
import io.quotaflow.core.store.LocalRateLimitStore
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class DecisionsFlowTest {

    private val context = RateLimitContext.empty()

    @Test
    fun `exactly one event per final decision including throttled retries`() = runBlocking {
        val flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMillis(40)))
        val collected = CopyOnWriteArrayList<DecisionEvent>()
        val collector = launch(Dispatchers.Default) {
            flow.decisions().collect { collected += it }
        }
        awaitCondition("subscription registered") { flow.eventBus.subscriberCount == 1 }
        try {
            val instantAllow = flow.tryAcquire("t", context)
            val waitedAllow = flow.acquire("t", context, 1, 5.seconds)
            val instantRejection = flow.tryAcquire("t", context)

            awaitCondition("all decisions streamed") { collected.size == 3 }

            assertTrue(instantAllow.isAllowed)
            assertTrue(waitedAllow.isAllowed)
            assertTrue(waitedAllow.waitDuration().toMillis() > 0)
            assertFalse(instantRejection.isAllowed)

            // One event per final decision — the throttled acquisition retried
            // against the store at least once yet emitted exactly one event.
            assertEquals(3, collected.size)
            val waitedEvent = collected.single { it.decision.waitDuration().toMillis() > 0 }
            assertEquals(waitedAllow, waitedEvent.decision)
            assertTrue(collected.all { it.keyGroup == "global" })
        } finally {
            collector.cancelAndJoin()
        }
    }

    @Test
    fun `events carry key-group-only identities`() = runBlocking {
        val tenantPolicy = RateLimitPolicy.builder("tenant-plan")
            .limit(Limit(1, 1, Duration.ofHours(1)))
            .scope(Scope.TENANT)
            .reaction(Reaction.REJECT)
            .build()
        val flow = CoroutineQuotaFlow
            .builder(PolicySet.compile(listOf(tenantPolicy)), LocalRateLimitStore())
            .build()
        val collected = CopyOnWriteArrayList<DecisionEvent>()
        val collector = launch(Dispatchers.Default) {
            flow.decisions().collect { collected += it }
        }
        awaitCondition("subscription registered") { flow.eventBus.subscriberCount == 1 }
        try {
            val rawKey = "tenant-secret-42"
            val tenantContext = RateLimitContext.builder()
                .put(RateLimitContext.TENANT_ID, rawKey)
                .build()
            flow.tryAcquire("tenant-plan", tenantContext)

            awaitCondition("decision streamed") { collected.size == 1 }
            assertEquals("tenant", collected[0].keyGroup)
            assertFalse(collected[0].keyGroup.contains(rawKey))
        } finally {
            collector.cancelAndJoin()
        }
    }

    @Test
    fun `slow collector drops events instead of delaying decisions`() = runBlocking {
        val flow = flowFor(rejectPolicy("r", 1, 1, Duration.ofHours(1)))
        val received = AtomicInteger()
        val collector: Job = launch(Dispatchers.Default) {
            flow.decisions().collect {
                received.incrementAndGet()
                Thread.sleep(50) // deliberately slow consumer
            }
        }
        awaitCondition("subscription registered") { flow.eventBus.subscriberCount == 1 }
        try {
            flow.tryAcquire("r", context) // allowed; policy now exhausted
            val start = System.nanoTime()
            repeat(100) { flow.tryAcquire("r", context) }
            val elapsedMillis = (System.nanoTime() - start) / 1_000_000

            // 100 decisions while the collector needs ~50ms per event: any
            // backpressure on the decision path would take seconds.
            assertTrue(
                elapsedMillis < 1_000,
                "decisions were delayed by the slow collector (${elapsedMillis}ms)")
            awaitCondition("collector observed the first events", timeoutMillis = 2_000) {
                received.get() >= 1
            }
            assertTrue(
                received.get() < 100,
                "slow collector should have dropped events, yet observed ${received.get()}")
        } finally {
            collector.cancelAndJoin()
        }
    }

    @Test
    fun `cancelling collection unregisters the subscription`() = runBlocking {
        val flow = flowFor(rejectPolicy("r", 10, 1, Duration.ofHours(1)))
        val collector = launch(Dispatchers.Default) {
            flow.decisions().collect { }
        }
        awaitCondition("subscription registered") { flow.eventBus.subscriberCount == 1 }

        collector.cancelAndJoin()

        awaitCondition("subscription removed") { flow.eventBus.subscriberCount == 0 }
    }
}
