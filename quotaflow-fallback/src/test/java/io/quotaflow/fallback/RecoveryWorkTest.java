package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.store.PrimaryDispatchException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class RecoveryWorkTest {
    private static PrimaryDispatchException failure(CompletableFuture<?> result) throws Exception {
        return assertInstanceOf(PrimaryDispatchException.class,
                assertThrows(ExecutionException.class, () -> result.get(3, TimeUnit.SECONDS)).getCause());
    }

    @Test void expiredAndRetiredBudgetsNeverDispatch() throws Exception {
        var called = new AtomicBoolean();
        assertEquals(PrimaryDispatchException.Outcome.NOT_DISPATCHED, failure(
                new RecoveryWork.Budget(Duration.ZERO).call(() -> {
                    called.set(true); return CompletableFuture.completedFuture(1);
                })).outcome());
        assertEquals(PrimaryDispatchException.Outcome.NOT_DISPATCHED, failure(
                new RecoveryWork.Budget(Duration.ofSeconds(2), () -> false).call(() -> {
                    called.set(true); return CompletableFuture.completedFuture(1);
                })).outcome());
        assertFalse(called.get());
    }

    @Test void enteredTimeoutIsUncertainAndDoesNotCancelTheNativeOperation() throws Exception {
        var entered = new CountDownLatch(1);
        var nativeResult = new CompletableFuture<Integer>();
        var result = RecoveryWork.call(() -> { entered.countDown(); return nativeResult; }, Duration.ofMillis(100));
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        assertEquals(PrimaryDispatchException.Outcome.UNCERTAIN, failure(result).outcome());
        assertFalse(nativeResult.isDone());
        nativeResult.complete(42);
        assertTrue(result.isCompletedExceptionally(), "late completion cannot refund or change an uncertain outcome");
    }

    @Test void preservesValuesAndBothKindsOfFailure() throws Exception {
        assertEquals(42, RecoveryWork.call(() -> CompletableFuture.completedFuture(42), Duration.ofSeconds(2))
                .get(3, TimeUnit.SECONDS));
        var cause = new IllegalArgumentException("test failure");
        assertSame(cause, assertThrows(ExecutionException.class, () -> RecoveryWork.call(() -> {
            throw cause;
        }, Duration.ofSeconds(2)).get(3, TimeUnit.SECONDS)).getCause());
        assertSame(cause, assertThrows(ExecutionException.class, () -> RecoveryWork.call(
                () -> CompletableFuture.failedFuture(cause), Duration.ofSeconds(2)).get(3, TimeUnit.SECONDS)).getCause());
    }

    @Test void successiveStepsShareOneDeadline() throws Exception {
        var budget = new RecoveryWork.Budget(Duration.ofMillis(150));
        long initial = budget.remaining();
        var nativeResult = new CompletableFuture<Integer>();
        assertEquals(PrimaryDispatchException.Outcome.UNCERTAIN, failure(budget.call(() -> nativeResult)).outcome());
        assertTrue(initial > 0);
        assertEquals(0, budget.remaining());
        assertEquals(PrimaryDispatchException.Outcome.NOT_DISPATCHED,
                failure(budget.call(() -> { fail("expired continuation dispatched"); return nativeResult; })).outcome());
        assertFalse(nativeResult.isCancelled());
    }
}
