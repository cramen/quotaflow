package io.quotaflow.core.execution;

import java.util.concurrent.*;

/** Shared timer signals. Callbacks must perform only bounded coordination, never user work. */
public final class DeadlineScheduler {
    private static final ScheduledThreadPoolExecutor TIMER = new ScheduledThreadPoolExecutor(2, task -> {
        Thread thread = new Thread(task, "quotaflow-deadline"); thread.setDaemon(true); return thread;
    });
    static { TIMER.setRemoveOnCancelPolicy(true); }
    private DeadlineScheduler() { }
    public static ScheduledFuture<?> schedule(Runnable signal, long delayNanos) {
        return TIMER.schedule(signal, Math.max(0, delayNanos), TimeUnit.NANOSECONDS);
    }
    public static int pendingTimers() { return TIMER.getQueue().size(); }
}
