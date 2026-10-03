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

    @Test void forwardsEveryRecoveryCapabilityWithoutChangingArgumentsOrOutcomes() throws Exception {
        var cohort = RecoveryCohort.single();
        var session = new RecoverySession("deployment", cohort.digest(), "single", 0, 1, "token");
        var domain = new QuotaDomain("default", "quota");
        var context = new RecoveryContext(domain, session, 2, 3, 4, "a".repeat(64), RecoveryPhase.GATHER);
        var configuration = new RecoveryConfiguration("a".repeat(64), 4);
        var pending = new RecoveryPending(domain, 3, new CompletableFuture<>());
        var observed = new AtomicReference<java.lang.reflect.Method>();
        var arguments = new AtomicReference<Object[]>();
        var response = new AtomicReference<CompletableFuture<?>>();
        RecoveryPrimary delegate = (RecoveryPrimary) Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(),
                new Class<?>[]{RecoveryPrimary.class}, (proxy, method, args) -> {
                    if (method.getName().equals("probe")) return CompletableFuture.completedFuture(null);
                    observed.set(method); arguments.set(args); return response.get();
                });
        try (var holder = new ReconnectingRecoveryPrimary(
                () -> new ReconnectingRecoveryPrimary.Connection(delegate, () -> {}), Duration.ofSeconds(2))) {
            for (var method : RecoveryPrimary.class.getMethods()) {
                if (method.getName().equals("probe") || method.isDefault()) continue;
                Object[] args = java.util.Arrays.stream(method.getParameterTypes()).map(type -> {
                    if (type == RecoveryCohort.class) return cohort;
                    if (type == RecoverySession.class) return session;
                    if (type == QuotaDomain.class) return domain;
                    if (type == RecoveryContext.class) return context;
                    if (type == RecoveryConfiguration.class) return configuration;
                    if (type == RecoveryPending.class) return pending;
                    if (type == String.class) return "single";
                    if (type == boolean.class) return Boolean.TRUE;
                    if (type == java.util.List.class) return java.util.List.of();
                    throw new AssertionError("Uncovered capability parameter: " + type);
                }).toArray();
                var absent = (CompletionStage<?>) method.invoke(holder, args);
                assertEquals(PrimaryDispatchException.Outcome.NOT_DISPATCHED,
                        assertInstanceOf(PrimaryDispatchException.class,
                            assertThrows(CompletionException.class, () -> absent.toCompletableFuture().join()).getCause()).outcome());
            }
            holder.probe().toCompletableFuture().get(3, TimeUnit.SECONDS);
            for (var method : RecoveryPrimary.class.getMethods()) {
                if (method.getName().equals("probe") || method.isDefault()) continue;
                Object[] args = java.util.Arrays.stream(method.getParameterTypes()).map(type -> {
                    if (type == RecoveryCohort.class) return cohort;
                    if (type == RecoverySession.class) return session;
                    if (type == QuotaDomain.class) return domain;
                    if (type == RecoveryContext.class) return context;
                    if (type == RecoveryConfiguration.class) return configuration;
                    if (type == RecoveryPending.class) return pending;
                    if (type == String.class) return "single";
                    if (type == boolean.class) return Boolean.TRUE;
                    if (type == java.util.List.class) return java.util.List.of();
                    throw new AssertionError("Uncovered capability parameter: " + type);
                }).toArray();
                var nativeStage = new CompletableFuture<>(); response.set(nativeStage);
                assertSame(nativeStage, method.invoke(holder, args), method.getName());
                assertEquals(method, observed.get());
                assertArrayEquals(args, arguments.get(), method.getName());
            }
        }
    }
    @Test void cancellingOneOpeningViewDoesNotCancelAnotherOrDuplicateResources() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var opened = new AtomicInteger(); var closed = new AtomicInteger();
        try (var holder = new ReconnectingRecoveryPrimary(() -> {
            opened.incrementAndGet(); entered.countDown();
            try { assertTrue(release.await(3, TimeUnit.SECONDS)); }
            catch (InterruptedException failure) { throw new AssertionError(failure); }
            return new ReconnectingRecoveryPrimary.Connection(healthy(), closed::incrementAndGet);
        }, Duration.ofSeconds(2))) {
            var first = holder.probe().toCompletableFuture();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            var second = holder.probe().toCompletableFuture();
            first.cancel(false); release.countDown();
            second.get(3, TimeUnit.SECONDS);
            assertEquals(1, opened.get()); assertEquals(0, closed.get());
        } finally { release.countDown(); }
        assertEquals(1, closed.get());
    }
}
