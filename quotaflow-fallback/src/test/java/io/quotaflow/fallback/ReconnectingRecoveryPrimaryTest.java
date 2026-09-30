package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.store.*;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class ReconnectingRecoveryPrimaryTest {
    private static RecoveryPrimary healthy() {
        return (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(), new Class<?>[]{RecoveryPrimary.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("probe")) return CompletableFuture.completedFuture(null);
                    throw new AssertionError("unexpected operation " + method.getName());
                });
    }
    @Test void failedStartupRetriesAndClosesOneOwnedConnection() {
        AtomicInteger attempts = new AtomicInteger(), closes = new AtomicInteger();
        var holder = new ReconnectingRecoveryPrimary(() -> {
            if (attempts.incrementAndGet() == 1) throw new IllegalStateException("unreachable");
            return new ReconnectingRecoveryPrimary.Connection(healthy(), closes::incrementAndGet);
        }, Duration.ofSeconds(2));
        assertThrows(CompletionException.class, () -> holder.probe().toCompletableFuture().join());
        holder.probe().toCompletableFuture().join();
        holder.probe().toCompletableFuture().join();
        assertEquals(2, attempts.get());
        holder.close(); holder.close();
        assertEquals(1, closes.get());
        assertThrows(CompletionException.class, () -> holder.probe().toCompletableFuture().join());
    }
    @Test void shutdownClosesLateResourceWithoutPublishingIt() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), disposed = new CountDownLatch(1);
        var holder = new ReconnectingRecoveryPrimary(() -> {
            entered.countDown();
            try { assertTrue(release.await(3, TimeUnit.SECONDS)); }
            catch (InterruptedException failure) { throw new AssertionError(failure); }
            return new ReconnectingRecoveryPrimary.Connection(healthy(), disposed::countDown);
        }, Duration.ofSeconds(2));
        var pending = holder.probe().toCompletableFuture();
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        holder.close(); release.countDown();
        assertTrue(disposed.await(2, TimeUnit.SECONDS));
        assertTrue(pending.isCompletedExceptionally());
    }
    @Test void aTimedOutOpeningCannotReplaceANewerConnection() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), lateClosed = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger(), currentClosed = new AtomicInteger();
        try (var holder = new ReconnectingRecoveryPrimary(() -> {
            if (attempts.incrementAndGet() == 1) {
                entered.countDown();
                try { assertTrue(release.await(3, TimeUnit.SECONDS)); }
                catch (InterruptedException failure) { throw new AssertionError(failure); }
                return new ReconnectingRecoveryPrimary.Connection(healthy(), lateClosed::countDown);
            }
            return new ReconnectingRecoveryPrimary.Connection(healthy(), currentClosed::incrementAndGet);
        }, Duration.ofMillis(100))) {
            var old = holder.probe().toCompletableFuture();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> old.get(2, TimeUnit.SECONDS));
            holder.probe().toCompletableFuture().get(2, TimeUnit.SECONDS);
            release.countDown();
            assertTrue(lateClosed.await(2, TimeUnit.SECONDS));
            holder.probe().toCompletableFuture().join();
            assertEquals(0, currentClosed.get());
        } finally { release.countDown(); }
        assertEquals(1, currentClosed.get());
    }
}
