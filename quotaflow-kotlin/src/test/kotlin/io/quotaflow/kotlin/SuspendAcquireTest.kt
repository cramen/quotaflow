package io.quotaflow.kotlin

import io.quotaflow.core.PolicyConfigurationException
import io.quotaflow.core.PolicySet
import io.quotaflow.core.RateLimitContext
import io.quotaflow.core.store.LocalRateLimitStore
import java.time.Duration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test

class SuspendAcquireTest {

    private val context = RateLimitContext.empty()

    @Test
    fun `suspend try-acquire returns the same decisions as the Java facade`() = runTest {
        val javaFlow = flowFor(rejectPolicy("p", 2, 1, Duration.ofHours(1)))
        val coroutineFlow = flowFor(rejectPolicy("p", 2, 1, Duration.ofHours(1)))

        repeat(3) { attempt ->
            val expected = javaFlow.delegate.tryAcquire("p", context, 1)
            val actual = coroutineFlow.tryAcquire("p", context)
            assertEquals(expected, actual, "decision #$attempt diverged from the Java facade")
        }
        assertFalse(javaFlow.delegate.tryAcquire("p", context).isAllowed)
    }

    @Test
    fun `suspend try-acquire with weight matches the weighted Java decision`() = runTest {
        val javaFlow = flowFor(rejectPolicy("p", 10, 1, Duration.ofHours(1)))
        val coroutineFlow = flowFor(rejectPolicy("p", 10, 1, Duration.ofHours(1)))

        assertEquals(
            javaFlow.delegate.tryAcquire("p", context, 4),
            coroutineFlow.tryAcquire("p", context, 4))
    }

    @Test
    fun `caller misuse surfaces as the same exception as the Java facade`() = runTest {
        val flow = flowFor(rejectPolicy("p", 1, 1, Duration.ofSeconds(1)))

        assertThrows<PolicyConfigurationException> { flow.tryAcquire("unknown", context) }
        assertThrows<IllegalArgumentException> { flow.tryAcquire("p", context, 0) }
        assertThrows<IllegalArgumentException> { flow.acquire("p", context, 0, 1.seconds) }
        assertThrows<IllegalArgumentException> { flow.acquire("p", context, 1, (-1).seconds) }
    }

    @Test
    fun `throttle wait suspends the coroutine and resumes with the final decision`() = runBlocking {
        val flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMillis(50)))
        assertTrue(flow.tryAcquire("t", context).isAllowed, "first acquisition exhausts the policy")

        val start = System.nanoTime()
        val decision = flow.acquire("t", context, 1, 5.seconds)
        val elapsedMillis = (System.nanoTime() - start) / 1_000_000

        assertTrue(decision.isAllowed)
        assertTrue(decision.waitDuration().toMillis() > 0, "a waited decision carries its wait")
        assertTrue(
            elapsedMillis >= 30,
            "expected a real wait around the 50ms refill period, was ${elapsedMillis}ms")
    }

    @Test
    fun `explicit priority overload delegates to the prioritized async path`() = runBlocking {
        val flow = flowFor(throttlePolicy("t", 5, 1, Duration.ofMillis(10)))
        val decision = flow.acquire("t", context, 1, 1.seconds, priority = 7)
        assertTrue(decision.isAllowed)
    }

    @Test
    fun `concurrent throttled acquisitions on a single-threaded dispatcher all proceed`() = runBlocking {
        val flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofMillis(10)))
        assertTrue(flow.tryAcquire("t", context).isAllowed, "first acquisition exhausts the policy")
        val singleThread = Dispatchers.Default.limitedParallelism(1)

        // If any acquisition blocked the dispatcher's only thread instead of
        // suspending, the remaining coroutines could never run and this test
        // would deadlock into the timeout.
        val decisions = withTimeout(15_000) {
            (1..20)
                .map { async(singleThread) { flow.acquire("t", context, 1, 10.seconds) } }
                .awaitAll()
        }

        assertEquals(20, decisions.size)
        assertTrue(decisions.all { it.isAllowed }, "every queued acquisition eventually passed")
    }

    @Test
    fun `rejection without wait stays instant in throttle mode`() = runBlocking {
        val policies = PolicySet.compile(listOf(rejectPolicy("r", 1, 1, Duration.ofHours(1))))
        val flow = CoroutineQuotaFlow.builder(policies, LocalRateLimitStore()).build()
        assertTrue(flow.tryAcquire("r", context).isAllowed)

        val start = System.nanoTime()
        val decision = flow.acquire("r", context, 1, 30.seconds)
        val elapsedMillis = (System.nanoTime() - start) / 1_000_000

        assertFalse(decision.isAllowed)
        assertTrue(elapsedMillis < 1_000, "reject-mode policy waited ${elapsedMillis}ms")
    }
}
