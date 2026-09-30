package io.quotaflow.core.execution;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** Finite compatibility execution. Pending asynchronous operations retain admission until physical completion. */
public final class BoundedExecution implements AutoCloseable {
    private static final AtomicLong IDS = new AtomicLong();
    private static final BoundedExecution SHARED = new BoundedExecution(4, 1024);
    private final ThreadPoolExecutor workers;
    private final Semaphore admission;

    public BoundedExecution(int workerCount, int pendingCapacity) {
        if (workerCount < 1 || pendingCapacity < 1) throw new IllegalArgumentException("execution bounds must be positive");
        admission = new Semaphore(Math.addExact(workerCount, pendingCapacity));
        workers = new ThreadPoolExecutor(workerCount, workerCount, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(pendingCapacity), task -> {
                    Thread thread = new Thread(task, "quotaflow-dispatch-" + IDS.incrementAndGet());
                    thread.setDaemon(true); return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }
    /** Process-wide default: four workers and 1024 pending tasks. Do not close this shared service. */
    public static BoundedExecution shared() { return SHARED; }

    public <T> CompletionStage<T> submit(Supplier<T> work, BooleanSupplier active) {
        return submitStage(() -> CompletableFuture.completedFuture(work.get()), active);
    }

    public <T> CompletionStage<T> submitStage(Supplier<? extends CompletionStage<T>> work, BooleanSupplier active) {
        Objects.requireNonNull(work, "work"); Objects.requireNonNull(active, "active");
        var result = new CompletableFuture<T>();
        if (!admission.tryAcquire()) return CompletableFuture.failedFuture(new RejectedExecutionException("quota execution capacity exhausted"));
        try {
            workers.execute(() -> {
                try {
                    if (!active.getAsBoolean()) {
                        admission.release(); result.completeExceptionally(new CancellationException()); return;
                    }
                    Objects.requireNonNull(work.get(), "operation stage").whenComplete((value, failure) -> {
                        admission.release();
                        if (failure == null) result.complete(value); else result.completeExceptionally(failure);
                    });
                } catch (Throwable failure) { admission.release(); result.completeExceptionally(failure); }
            });
        } catch (RejectedExecutionException failure) { admission.release(); result.completeExceptionally(failure); }
        return result;
    }

    public int activeWorkers() { return workers.getActiveCount(); }
    public int pendingTasks() { return workers.getQueue().size(); }
    /** Stops owned execution; running third-party code may ignore interruption. */
    @Override public void close() {
        if (this == SHARED) throw new IllegalStateException("shared execution cannot be closed");
        workers.shutdown();
    }
}
