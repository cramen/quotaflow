package io.quotaflow.config;

import io.quotaflow.core.*;
import io.quotaflow.core.execution.BoundedExecution;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ResolverExecutionTest {
    static Limit limit(long capacity) { return new Limit(capacity,1,Duration.ofSeconds(1)); }
    static class AsyncProvider implements AsyncLimitResolver {
        final BlockingQueue<CompletableFuture<Optional<Limit>>> calls = new LinkedBlockingQueue<>();
        public Optional<Limit> resolve(String ref,String group) { throw new AssertionError("async expected"); }
        public CompletionStage<Optional<Limit>> resolveAsync(String ref,String group) {
            var result=new CompletableFuture<Optional<Limit>>(); calls.add(result); return result;
        }
        CompletableFuture<Optional<Limit>> next() throws Exception { var call=calls.poll(2,TimeUnit.SECONDS);assertNotNull(call);return call; }
    }
    @Test void singleFlightDetachedCancellationAndCompletionBasedTtl() throws Exception {
        var provider=new AsyncProvider();var nanos=new AtomicLong();
        var cache=new CachingLimitResolver(provider,Duration.ofNanos(100),Duration.ofSeconds(1),BoundedExecution.shared(),nanos::get);
        var cancelled=cache.resolveAsync("plan","tenant").toCompletableFuture();
        var physical=provider.next();
        var live=cache.resolveAsync("plan","tenant").toCompletableFuture();
        assertTrue(cancelled.cancel(true));assertFalse(physical.isCancelled()); assertTrue(provider.calls.isEmpty());
        nanos.set(90);physical.complete(Optional.of(limit(10)));assertEquals(10,live.get().orElseThrow().capacity());
        nanos.set(150);assertEquals(10,cache.resolveAsync("plan","tenant").toCompletableFuture().get().orElseThrow().capacity());
        assertTrue(provider.calls.isEmpty());
        nanos.set(190);var fresh=cache.resolveAsync("plan","tenant").toCompletableFuture();provider.next().complete(Optional.empty());
        assertTrue(fresh.get().isEmpty()); assertTrue(cache.resolveAsync("plan","tenant").toCompletableFuture().get().isEmpty());assertTrue(provider.calls.isEmpty());
    }
    @Test void timeoutTombstoneSurvivesUntilPhysicalExitAndDoesNotCacheTheLateValue() throws Exception {
        var provider=new AsyncProvider();
        var cache=CachingLimitResolver.wrap(provider,Duration.ofMinutes(1),Duration.ofMillis(100),BoundedExecution.shared());
        var first=cache.resolveAsync("plan","tenant").toCompletableFuture();var physical=provider.next();
        assertTrue(first.get(2,TimeUnit.SECONDS).isEmpty());
        for(int i=0;i<20;i++) assertTrue(cache.resolveAsync("plan","tenant").toCompletableFuture().get().isEmpty());
        assertTrue(provider.calls.isEmpty());physical.complete(Optional.of(limit(100)));
        var fresh=cache.resolveAsync("plan","tenant").toCompletableFuture();provider.next().complete(Optional.of(limit(1)));
        assertEquals(1,fresh.get().orElseThrow().capacity());
    }
    @Test void failuresAreNotNegativeCacheEntriesAndUnrelatedKeysProgress() throws Exception {
        var provider=new AsyncProvider();var cache=CachingLimitResolver.wrap(provider);
        var failed=cache.resolveAsync("plan","tenant").toCompletableFuture();var blocked=provider.next();
        var other=cache.resolveAsync("other","tenant").toCompletableFuture();provider.next().complete(Optional.of(limit(2)));
        assertEquals(2,other.get().orElseThrow().capacity());
        blocked.completeExceptionally(new IllegalStateException("provider unavailable"));assertThrows(ExecutionException.class,failed::get);
        var retried=cache.resolveAsync("plan","tenant").toCompletableFuture();provider.next().complete(Optional.of(limit(1)));assertEquals(1,retried.get().orElseThrow().capacity());
    }
    @Test void actualReloadCompletesOnlyAfterPublishingFreshCacheGeneration() throws Exception {
        var provider=new AsyncProvider();var cache=CachingLimitResolver.wrap(provider);
        var old=cache.resolveAsync("plan","global").toCompletableFuture();var physical=provider.next();
        var config=Map.of("quotaflow.policies.p.scope","global","quotaflow.policies.p.limit-ref","plan");
        var source=new MapConfigSource(config);
        var flow=DefaultQuotaFlow.builder(ConfigurationParser.parse(config).policySet(),new LocalRateLimitStore()).limitResolver(cache).build();
        var reloader=ConfigReloader.builder(source,flow).onApplied(cache::clear).build();
        assertTrue(reloader.reload().applied());
        var fresh=cache.resolveAsync("plan","global").toCompletableFuture();provider.next().complete(Optional.of(limit(1)));
        assertEquals(1,fresh.get().orElseThrow().capacity());physical.complete(Optional.of(limit(100)));assertEquals(100,old.get().orElseThrow().capacity());
        assertEquals(1,cache.resolve("plan","global").orElseThrow().capacity());
    }
    @Test void repeatedInvalidationCannotReplaceStuckWorkersWithoutBound() throws Exception {
        try(var execution=new BoundedExecution(1,2)) {
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var calls=new AtomicInteger();
            var cache=CachingLimitResolver.wrap((ref,group)->{
                calls.incrementAndGet();entered.countDown();
                while(release.getCount()!=0) { try { release.await(); } catch(InterruptedException ignored) { } }
                return Optional.of(limit(100));
            },Duration.ofMinutes(1),Duration.ofMillis(50),execution);
            try {
                var first=cache.resolveAsync("plan","tenant").toCompletableFuture();assertTrue(entered.await(2,TimeUnit.SECONDS));
                assertTrue(first.get(2,TimeUnit.SECONDS).isEmpty());
                for(int i=0;i<30;i++) { cache.clear(); assertTrue(cache.resolveAsync("plan","tenant").toCompletableFuture().get(2,TimeUnit.SECONDS).isEmpty()); }
                assertEquals(1,calls.get());assertEquals(1,execution.activeWorkers());assertTrue(execution.pendingTasks()<=2);
            } finally { release.countDown(); }
            long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);
            while((execution.activeWorkers()!=0 || execution.pendingTasks()!=0)&&System.nanoTime()<end) Thread.sleep(1);
            assertEquals(0,execution.activeWorkers());assertEquals(0,execution.pendingTasks());
        }
    }
}
