package io.quotaflow.core.store;

import io.quotaflow.core.LimitResolver;
import io.quotaflow.core.PolicySet;
import java.util.concurrent.CompletionStage;

/** Control-plane configuration activation used by recovery-aware stores. */
public interface RecoveryConfigurationAware extends BatchRateLimitStore {
    @Override default boolean requiresVersionedLimits() { return true; }
    /** Capability/target validation before immutable bindings or serving policies are published. */
    default void validateRecoveryConfiguration(PolicySet policies, String namespace, LimitResolver resolver) { }

    CompletionStage<Void> configureRecovery(PolicySet policies, String namespace, LimitResolver resolver);
}
