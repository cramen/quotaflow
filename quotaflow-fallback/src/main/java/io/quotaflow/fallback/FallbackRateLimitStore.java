package io.quotaflow.fallback;

import io.quotaflow.core.store.*;
import java.util.List;
import java.util.function.Function;
import java.util.function.LongSupplier;

/** Conservative fixed-cohort fallback. Recovery requires context-fenced primary capabilities. */
public final class FallbackRateLimitStore extends CoordinatedFallbackStore {
    public FallbackRateLimitStore(RecoveryPrimary primary, RecoverySettings settings, List<DegradationListener> listeners) {
        super(primary, settings, listeners);
    }
    public FallbackRateLimitStore(RecoveryPrimary primary, RecoverySettings settings,
                                 List<DegradationListener> listeners, LongSupplier clock) {
        super(primary, settings, listeners, clock);
    }

    /** @deprecated A store/seeder pair cannot prove session ownership, fencing or non-replay. */
    @Deprecated(forRemoval = true)
    public FallbackRateLimitStore(BatchRateLimitStore primary, StateSeeder seeder, FallbackConfig config,
                                 List<DegradationListener> listeners) {
        super(unsupported(), RecoverySettings.single(), listeners);
    }
    /** @deprecated Supply a RecoveryPrimary and RecoverySettings; guards are private, cold and epoch-owned. */
    @Deprecated(forRemoval = true)
    public FallbackRateLimitStore(BatchRateLimitStore primary, LocalRateLimitStore local, StateSeeder seeder,
                                 FallbackConfig config, List<DegradationListener> listeners, LongSupplier clock,
                                 Function<BucketIdentity, String> keyGroupExtractor) {
        super(unsupported(), RecoverySettings.single(), listeners, clock);
    }
    private static RecoveryPrimary unsupported() {
        throw new IllegalArgumentException("legacy store/seeder fallback wiring is unsupported; supply RecoveryPrimary and fixed-cohort RecoverySettings");
    }
}
