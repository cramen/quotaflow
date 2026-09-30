package io.quotaflow.core.execution;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Ordered bounded callbacks. Timeout or saturation disables only this lane and is explicitly diagnosed. */
public final class CallbackDispatcher implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CallbackDispatcher.class);
    private static final AtomicLong IDS = new AtomicLong();
    private static void failed(RuntimeException failure) { LOG.warn("quota listener failed ({})", failure.getClass().getSimpleName()); }
    public long failures() { return failures.sum(); }
    @Override public void close() { closeAsync(); }
    final Runnable closeAction;
    final ThreadPoolExecutor executor;
    final long timeoutNanos;
    final java.util.concurrent.atomic.LongAdder failures;
    final java.util.concurrent.locks.ReentrantLock admission = new java.util.concurrent.locks.ReentrantLock();
    CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
    final AtomicBoolean disabled = new AtomicBoolean();
    final AtomicBoolean closed = new AtomicBoolean();
    final AtomicBoolean sessionClosed = new AtomicBoolean();
    public CallbackDispatcher(int capacity, Duration timeout, Runnable closeAction) {
        if (capacity < 1 || timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("callback delivery bounds must be positive");
        this.failures = new java.util.concurrent.atomic.LongAdder();
        this.closeAction = java.util.Objects.requireNonNull(closeAction, "closeAction"); timeoutNanos = timeout.toNanos();
        String name = "quotaflow-observer-" + IDS.incrementAndGet();
        executor = new ThreadPoolExecutor(0, 1, 100, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(capacity), task -> {
            var thread = new Thread(task, name); thread.setDaemon(true); return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
    }
    public CompletableFuture<Void> submit(Runnable action) {
        var work = new Work(action);
        admission.lock();
        try {
            if (closed.get() || disabled.get()) { work.done.complete(null); return work.done.copy(); }
            try { executor.execute(work); tail = work.done; }
            catch (RejectedExecutionException full) { disable("delivery-capacity"); work.done.complete(null); }
            return work.done.copy();
        } finally { admission.unlock(); }
    }
    public CompletableFuture<Void> barrier() {
        admission.lock(); try { return tail.copy(); } finally { admission.unlock(); }
    }
    void disable(String reason) {
        admission.lock();
        try {
            if (!disabled.compareAndSet(false, true)) return;
            failures.increment();
            CompletableFuture.runAsync(() -> LOG.warn("quota listener disabled ({})", reason), ForkJoinPool.commonPool());
            var abandoned = new ArrayList<Runnable>(); executor.getQueue().drainTo(abandoned);
            for (Runnable task : abandoned) ((Work)task).done.complete(null);
            // Physical third-party work may ignore interruption; never create a replacement lane.
            executor.shutdown();
        } finally { admission.unlock(); }
    }
    void closeSession() {
        if (sessionClosed.compareAndSet(false, true)) {
            try { closeAction.run(); } catch (RuntimeException failure) { failures.increment(); failed(failure); }
        }
    }
    public CompletableFuture<Void> closeAsync() {
        admission.lock();
        try {
            if (!closed.compareAndSet(false, true)) return tail.copy();
            if (disabled.get()) return CompletableFuture.completedFuture(null);
            var work = new Work(this::closeSession);
            try { executor.execute(work); tail = work.done; }
            catch (RejectedExecutionException full) { disable("close-capacity"); work.done.complete(null); }
            executor.shutdown(); return work.done.copy();
        } finally { admission.unlock(); }
    }
    private final class Work implements Runnable {
        final Runnable action;
        final CompletableFuture<Void> done = new CompletableFuture<>();
        Work(Runnable action) { this.action = action; }
        @Override public void run() {
            if (disabled.get()) { closeSession(); done.complete(null); return; }
            var timeout = DeadlineScheduler.schedule(() -> { disable("callback-timeout"); done.complete(null); }, timeoutNanos);
            try { action.run(); }
            catch (RuntimeException failure) { failures.increment(); failed(failure); }
            finally {
                timeout.cancel(false); done.complete(null);
                if (disabled.get()) closeSession();
            }
        }
    }
}
