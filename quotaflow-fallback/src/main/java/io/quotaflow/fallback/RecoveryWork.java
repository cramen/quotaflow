package io.quotaflow.fallback;

import io.quotaflow.core.store.PrimaryDispatchException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Shared bounded compatibility execution. Timeouts never cancel native commands or imply rollback. */
final class RecoveryWork {
    private static final ThreadPoolExecutor WORKERS = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1024), runnable -> {
                Thread thread = new Thread(runnable, "quotaflow-recovery-work"); thread.setDaemon(true); return thread;
            }, new ThreadPoolExecutor.AbortPolicy());
    static final ScheduledThreadPoolExecutor TIMER = new ScheduledThreadPoolExecutor(2, runnable -> {
        Thread thread = new Thread(runnable, "quotaflow-recovery-control"); thread.setDaemon(true); return thread;
    });
    static { TIMER.setRemoveOnCancelPolicy(true); }
    private RecoveryWork() { }

    /** One monotonic wall-clock budget shared by every step of a control attempt. */
    static final class Budget {
        private final long started = System.nanoTime();
        private final long nanos;
        private final java.util.function.BooleanSupplier active;
        Budget(Duration timeout) { this(timeout, () -> true); }
        Budget(Duration timeout, java.util.function.BooleanSupplier active) {
            nanos = timeout.toNanos(); this.active = active;
        }
        long remaining() { return Math.max(0, nanos - (System.nanoTime() - started)); }
        <T> CompletableFuture<T> call(Supplier<? extends CompletionStage<T>> work) {
            long remaining = remaining();
            if (remaining == 0 || !active.getAsBoolean()) return CompletableFuture.failedFuture(
                    new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED));
            return RecoveryWork.call(() -> active.getAsBoolean() ? work.get() : CompletableFuture.failedFuture(
                    new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED)), Duration.ofNanos(remaining));
        }
    }

    static <T> CompletableFuture<T> call(Supplier<? extends CompletionStage<T>> work, Duration timeout) {
        CompletableFuture<T> result = new CompletableFuture<>();
        AtomicInteger dispatch = new AtomicInteger(); // queued=0, entered=1, expired-before-dispatch=2
        ScheduledFuture<?> timer = TIMER.schedule(() -> {
            boolean notDispatched = dispatch.compareAndSet(0, 2);
            result.completeExceptionally(new PrimaryDispatchException(notDispatched
                    ? PrimaryDispatchException.Outcome.NOT_DISPATCHED : PrimaryDispatchException.Outcome.UNCERTAIN));
        }, timeout.toNanos(), TimeUnit.NANOSECONDS);
        result.whenComplete((value, failure) -> timer.cancel(false));
        try {
            WORKERS.execute(() -> {
                if (!dispatch.compareAndSet(0, 1)) return;
                try {
                    work.get().whenComplete((value, failure) -> {
                        if (failure == null) result.complete(value); else result.completeExceptionally(failure);
                    });
                } catch (Throwable failure) { result.completeExceptionally(failure); }
            });
        } catch (RejectedExecutionException saturated) {
            dispatch.compareAndSet(0, 2);
            result.completeExceptionally(new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED));
        }
        return result;
    }
}
