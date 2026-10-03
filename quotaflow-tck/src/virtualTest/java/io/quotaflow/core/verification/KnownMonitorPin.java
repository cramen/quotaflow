package io.quotaflow.core.verification;

/** Deliberately bad build-only code to prove JDK 21 pinning attribution is active. */
public final class KnownMonitorPin {
    private static final Object MONITOR = new Object();
    private KnownMonitorPin() { }
    public static void park() {
        synchronized (MONITOR) {
            java.util.concurrent.locks.LockSupport.parkNanos(20_000_000L);
        }
    }
}
