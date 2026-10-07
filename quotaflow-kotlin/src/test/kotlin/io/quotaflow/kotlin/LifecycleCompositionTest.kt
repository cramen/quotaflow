package io.quotaflow.kotlin

import io.quotaflow.config.CachingLimitResolver
import io.quotaflow.core.*
import io.quotaflow.core.store.*
import java.time.Duration
import java.util.Optional
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.milliseconds
import io.quotaflow.core.execution.BoundedExecution
import kotlinx.coroutines.*
import kotlinx.coroutines.test.StandardTestDispatcher
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class LifecycleCompositionTest {
    private val policies = PolicySet.compile(listOf(throttlePolicy("p", 10, 1, Duration.ofSeconds(1))))
    private class PendingStore : BatchRateLimitStore {
        val calls = LinkedBlockingQueue<CompletableFuture<ChainResult>>()
        override fun registerPolicies(bindings: List<PolicyBinding>): CompletionStage<Void> = CompletableFuture.completedFuture(null)
        override fun tryAcquire(key: BucketIdentity, limit: Limit, algorithm: Algorithm, weight: Long): StoreResult = error("batch expected")
        override fun tryAcquireAsync(key: BucketIdentity, limit: Limit, algorithm: Algorithm, weight: Long): CompletionStage<StoreResult> = error("batch expected")
        override fun tryAcquireAll(chain: List<LevelRequest>): CompletionStage<ChainResult> = CompletableFuture<ChainResult>().also { calls.add(it) }
        fun next(): CompletableFuture<ChainResult> = calls.poll(2, TimeUnit.SECONDS) ?: error("store not invoked")
    }

    @Test fun `cancellation during a pending retry suppresses its terminal event`() = runBlocking {
        val store = PendingStore()
        val flow = CoroutineQuotaFlow.builder(policies, store).build()
        val events = AtomicInteger()
        val unsubscribe = flow.eventBus.subscribe { events.incrementAndGet() }
        val waiter = launch { flow.acquire("p", RateLimitContext.empty(), 1, 10.seconds) }
        yield()
        store.next().complete(ChainResult.rejected(0, 0, 1))
        val retry = withContext(Dispatchers.IO) { store.next() }
        waiter.cancelAndJoin()
        retry.complete(ChainResult.acquired(0, 9))
        delay(30)
        assertEquals(0, events.get())
        assertEquals(0, flow.delegate.waitQueueDepth("p"))
        unsubscribe()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test fun `prompt cancellation after Java finalization retains the winning event`() = runBlocking {
        val dispatcher = StandardTestDispatcher()
        val store = PendingStore()
        val flow = CoroutineQuotaFlow.builder(policies, store).build()
        val event = CompletableFuture<Decision>()
        val unsubscribe = flow.eventBus.subscribe { event.complete(it.decision) }
        var delivered = false
        val waiter = launch(dispatcher) { flow.tryAcquire("p", RateLimitContext.empty()); delivered = true }
        try {
            dispatcher.scheduler.runCurrent()
            store.next().complete(ChainResult.acquired(0, 9))
            assertTrue(event.get(2, TimeUnit.SECONDS).isAllowed)
            waiter.cancel()
            dispatcher.scheduler.runCurrent()
            waiter.join()
            assertFalse(delivered)
            assertTrue(event.join().isAllowed)
        } finally {
            // Cancellation itself is queued on this manually driven dispatcher.
            // Drain it even when an earlier assertion fails, or runBlocking can
            // wait forever for its child instead of reporting the failure.
            waiter.cancel()
            dispatcher.scheduler.runCurrent()
            unsubscribe()
            flow.delegate.close()
        }
    }

    @Test fun `blocking shared resolver leaves single thread progress and surviving joiner intact`() = runBlocking {
        Executors.newSingleThreadExecutor().asCoroutineDispatcher().use { dispatcher ->
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val calls = AtomicInteger()
            val cache = CachingLimitResolver.wrap({ _, _ ->
                calls.incrementAndGet(); entered.countDown(); release.await()
                Optional.of(Limit(10, 1, Duration.ofSeconds(1)))
            })
            val dynamic = PolicySet.compile(listOf(RateLimitPolicy.builder("p").scope(Scope.GLOBAL).limitRef("plan").build()))
            val flow = CoroutineQuotaFlow.builder(dynamic, LocalRateLimitStore()).limitResolver(cache).build()
            val cancelled = async(dispatcher) { flow.tryAcquire("p", RateLimitContext.empty()) }
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS))
                val live = async(dispatcher) { flow.tryAcquire("p", RateLimitContext.empty()) }
                withTimeout(1000) { withContext(dispatcher) { assertTrue(true) } }
                cancelled.cancelAndJoin()
                release.countDown()
                assertTrue(live.await().isAllowed)
                assertEquals(1, calls.get())
            } finally { release.countDown() }
        }
    }

    @Test fun `many suspended waiters leave single thread available`() = runBlocking {
        Executors.newSingleThreadExecutor().asCoroutineDispatcher().use { dispatcher ->
            val flow = flowFor(throttlePolicy("p", 1, 1, Duration.ofHours(1)))
            assertTrue(flow.tryAcquire("p", RateLimitContext.empty()).isAllowed)
            val waiters = (1..100).map { launch(dispatcher) { flow.acquire("p", RateLimitContext.empty(), 1, 60.seconds) } }
            awaitCondition("all queued") { flow.delegate.waitQueueDepth("p") == 100 }
            withTimeout(1000) { withContext(dispatcher) { assertEquals(100, flow.delegate.waitQueueDepth("p")) } }
            waiters.forEach { it.cancelAndJoin() }
            assertEquals(0, flow.delegate.waitQueueDepth("p"))
        }
    }
    @Test fun `Kotlin builder exposes bounded operation configuration`() = runBlocking {
        BoundedExecution(1, 2).use { execution ->
            val store = PendingStore()
            val entries = AtomicInteger()
            val flow = CoroutineQuotaFlow.builder(policies, store)
                .execution(execution).operationTimeout(100.milliseconds)
                .addWaitListener { _, _ -> entries.incrementAndGet() }.build()
            val result = async { flow.tryAcquire("p", RateLimitContext.empty()) }
            yield()
            val pending = store.next()
            assertFalse(result.await().isAllowed)
            assertEquals(0, entries.get())
            pending.complete(ChainResult.acquired(0, 9))
        }
    }

}
