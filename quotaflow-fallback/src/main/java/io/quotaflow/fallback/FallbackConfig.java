package io.quotaflow.fallback;

import java.time.Duration;
import java.util.Objects;

/**
 * Tuning for {@link FallbackRateLimitStore}.
 *
 * @param failureThreshold consecutive primary store failures that trip the
 *                         breaker open
 * @param openDuration how long the breaker stays open before the first probe;
 *                     should be at least the primary store's command timeout,
 *                     so a stalled store does not eat a probe immediately
 * @param maxOpenDuration cap for the backoff that doubles the open duration
 *                        after each failed probe or seeding attempt
 * @param expectedInstances expected number of running instances; degraded
 *                          limits are divided by it so the summed degraded
 *                          flow never exceeds the global limit. The default 1
 *                          disables division and triggers a startup warning.
 * @param maxSeedEntries hard cap on bucket entries replayed into the
 *                       recovered store; overflow is logged and skipped
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
