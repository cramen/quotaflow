package io.quotaflow.config;

import io.quotaflow.core.Limit;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ResolverGenerationTest {
    private static Limit limit(long capacity) { return new Limit(capacity, 1, Duration.ofSeconds(1)); }

    @Test void oldCompletionCannotResurrectAnInvalidatedTariff() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var cache = CachingLimitResolver.wrap((ref, group) -> {
            if (calls.incrementAndGet() == 1) {
                entered.countDown();
                await(release);
                return Optional.of(limit(100));
            }
            return Optional.of(limit(1));
        });
        var pool = Executors.newSingleThreadExecutor();
        try {
            var old = pool.submit(() -> cache.resolve("plan", "tenant"));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            cache.clear();
            assertEquals(1, cache.resolve("plan", "tenant").orElseThrow().capacity());
            release.countDown();
            assertEquals(100, old.get(2, TimeUnit.SECONDS).orElseThrow().capacity());
            assertEquals(1, cache.resolve("plan", "tenant").orElseThrow().capacity());
        } finally { release.countDown(); pool.shutdownNow(); }
    }

    @Test void concurrentMissesShareOneDelegate() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var cache = CachingLimitResolver.wrap((ref, group) -> {
            calls.incrementAndGet(); entered.countDown(); await(release);
            return Optional.of(limit(1));
        });
        var pool = Executors.newFixedThreadPool(2);
        try {
            var first = pool.submit(() -> cache.resolve("plan", "tenant"));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            var secondEntered = new CountDownLatch(1);
            var second = pool.submit(() -> { secondEntered.countDown(); return cache.resolve("plan", "tenant"); });
            assertTrue(secondEntered.await(2, TimeUnit.SECONDS));
            release.countDown();
            assertEquals(first.get(2, TimeUnit.SECONDS), second.get(2, TimeUnit.SECONDS));
            assertEquals(1, calls.get());
        } finally { release.countDown(); pool.shutdownNow(); }
    }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(2, TimeUnit.SECONDS)) throw new AssertionError("barrier timed out"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
    }
}
