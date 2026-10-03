package io.quotaflow.core.verification;

/** Deliberately bad initializer: cold-start classification must not excuse its blocking. */
public final class KnownBlockingInitialization {
    static { java.util.concurrent.locks.LockSupport.parkNanos(20_000_000L); }
    private KnownBlockingInitialization() { }
    public static void touch() { }
}
