package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class RecoveryPendingTest {
    @Test void cancellationDoesNotCancelSharedReadiness() {
        var source = new CompletableFuture<Void>();
        var pending = new RecoveryPending(new QuotaDomain("n", "r"), 1, source);
        var first = pending.readiness().toCompletableFuture();
        var second = pending.readiness().toCompletableFuture();
        assertTrue(first.cancel(false));
        assertFalse(source.isCancelled());
        assertFalse(second.isDone());
        source.complete(null);
        assertTrue(second.isDone());
        assertEquals(1, pending.generation());
        assertEquals("r", pending.domain().rootPolicyId());
        assertFalse(StoreResult.pending(pending).acquired());
        assertSame(pending, ChainResult.pending(0, pending).recoveryPending());
        assertThrows(IllegalArgumentException.class, () -> new StoreResult(true, 0, 0, pending));
    }
    @Test void pendingOutcomesCannotAdvertiseCreditOrRefill() {
        var pending = new RecoveryPending(new QuotaDomain("n", "r"), 1, new CompletableFuture<>());
        assertThrows(IllegalArgumentException.class, () -> new RecoveryPending(pending.domain(), -1, new CompletableFuture<>()));
        assertThrows(IllegalArgumentException.class, () -> new StoreResult(false, 1, 0, pending));
        assertThrows(IllegalArgumentException.class, () -> new StoreResult(false, 0, 1, pending));
        assertThrows(IllegalArgumentException.class, () -> new ChainResult(true, 0, 0, 0, pending));
        assertThrows(IllegalArgumentException.class, () -> new ChainResult(false, 0, 1, 0, pending));
        assertThrows(IllegalArgumentException.class, () -> new ChainResult(false, 0, 0, 1, pending));
        assertEquals(0, StoreResult.pending(pending).retryAfterMillis());
        assertEquals(0, ChainResult.pending(0, pending).remaining());
        for (var outcome : PrimaryDispatchException.Outcome.values()) {
            var error = new PrimaryDispatchException(outcome);
            assertEquals(outcome, error.outcome()); assertNull(error.getCause());
            assertNotNull(error.getMessage());
        }
        assertThrows(NullPointerException.class, () -> new PrimaryDispatchException(null));
    }
}
