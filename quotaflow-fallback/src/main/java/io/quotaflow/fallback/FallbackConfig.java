package io.quotaflow.fallback;

import java.time.Duration;
import java.util.Objects;

/**
 * Legacy breaker configuration retained for source migration. The old store/seeder constructors
 * reject this wiring; use {@link RecoverySettings} with a recovery-capable primary.
 * In particular, a tracking bound never permits skipping consumed buckets during recovery.
 */
public record FallbackConfig(
        int failureThreshold,
        Duration openDuration,
        Duration maxOpenDuration,
        int expectedInstances,
        int maxSeedEntries) {

    public FallbackConfig {
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be >= 1, got " + failureThreshold);
        }
        Objects.requireNonNull(openDuration, "openDuration");
        if (openDuration.isZero() || openDuration.isNegative()) {
            throw new IllegalArgumentException("openDuration must be positive, got " + openDuration);
        }
        Objects.requireNonNull(maxOpenDuration, "maxOpenDuration");
        if (maxOpenDuration.compareTo(openDuration) < 0) {
            throw new IllegalArgumentException("maxOpenDuration (" + maxOpenDuration
                    + ") must be >= openDuration (" + openDuration + ")");
        }
        if (expectedInstances < 1) {
            throw new IllegalArgumentException("expectedInstances must be >= 1, got " + expectedInstances);
        }
        if (maxSeedEntries < 1) {
            throw new IllegalArgumentException("maxSeedEntries must be >= 1, got " + maxSeedEntries);
        }
    }

    /** Starting point: trip after 3 failures, probe after 1 s, cap backoff at 30 s. */
    public static FallbackConfig defaults() {
        return new FallbackConfig(3, Duration.ofSeconds(1), Duration.ofSeconds(30), 1, 10_000);
    }
}
