package io.quotaflow.fallback;

import io.quotaflow.core.store.RecoveryCohort;
import java.time.Duration;
import java.util.Objects;

/** Startup-only ownership and bounded recovery work settings. */
public record RecoverySettings(String namespace, String deploymentId, String instanceId, RecoveryCohort cohort, int maximumTrackedBuckets,
                               int maximumInFlight, Duration controlInterval, Duration attemptTimeout) {
    public RecoverySettings {
        new io.quotaflow.core.store.QuotaDomain(namespace, deploymentId);
        Objects.requireNonNull(cohort, "cohort"); cohort.slot(instanceId);
        if (maximumTrackedBuckets < 1 || maximumInFlight < 1) throw new IllegalArgumentException("recovery bounds must be positive");
        requireDuration(controlInterval); requireDuration(attemptTimeout);
    }
    private static void requireDuration(Duration value) {
        Objects.requireNonNull(value, "duration");
        if (value.isZero() || value.isNegative() || value.compareTo(Duration.ofHours(1)) > 0)
            throw new IllegalArgumentException("recovery duration must be positive and at most one hour");
    }
    public static RecoverySettings single() {
        return new RecoverySettings("default", "default", RecoveryCohort.DEFAULT_INSTANCE_ID, RecoveryCohort.single(), 10_000, 4096,
                Duration.ofSeconds(1), Duration.ofSeconds(10));
    }
}
