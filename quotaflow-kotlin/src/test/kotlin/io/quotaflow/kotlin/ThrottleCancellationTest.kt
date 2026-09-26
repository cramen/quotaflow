package io.quotaflow.kotlin

import io.quotaflow.core.RateLimitContext
import io.quotaflow.core.ThrottleRejection
import java.time.Duration
import java.util.Optional
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class ThrottleCancellationTest {

    private val context = RateLimitContext.empty()

    @Test
    fun `cancelling a waiting coroutine resumes it promptly`() = runBlocking {
        val flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofHours(1)), maxWaiters = 1)
        assertTrue(flow.tryAcquire("t", context).isAllowed, "first acquisition exhausts the policy")

        val waiter = launch { flow.acquire("t", context, 1, 60.seconds) }
        awaitCondition("waiter enqueued") { flow.delegate.waitQueueDepth("t") == 1 }

        val start = System.nanoTime()
        waiter.cancelAndJoin()
        val elapsedMillis = (System.nanoTime() - start) / 1_000_000

        assertTrue(
            elapsedMillis < 5_000,
            "cancelled coroutine took ${elapsedMillis}ms to resume; expected a prompt cancellation")
    }

    @Test
    fun `cancelled waiter frees its queue slot for a subsequent caller`() = runBlocking {
        val flow = flowFor(throttlePolicy("t", 1, 1, Duration.ofHours(1)), maxWaiters = 1)
        assertTrue(flow.tryAcquire("t", context).isAllowed, "first acquisition exhausts the policy")

        // Fill the single queue slot, then abandon the waiter.
        val waiter = launch { flow.acquire("t", context, 1, 60.seconds) }
        awaitCondition("waiter enqueued") { flow.delegate.waitQueueDepth("t") == 1 }
        waiter.cancelAndJoin()
        awaitCondition("cancelled waiter left the queue") { flow.delegate.waitQueueDepth("t") == 0 }

        // With the slot freed, a subsequent caller enqueues and waits out its
        // own timeout instead of being overflow-rejected.
        val decision = flow.acquire("t", context, 1, 300.milliseconds)
        assertEquals(Optional.of(ThrottleRejection.WAIT_TIMEOUT), decision.throttleRejection())
    }
}
