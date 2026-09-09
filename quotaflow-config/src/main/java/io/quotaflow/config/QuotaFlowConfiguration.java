package io.quotaflow.config;

import io.quotaflow.core.PolicySet;
import java.util.Optional;

/**
 * Result of parsing and compiling a configuration payload: the validated,
 * immutable {@link PolicySet} plus the fallback tuning knob
 * {@code expectedInstances} when declared under {@code quotaflow.defaults.}.
 * The policy set is what a reload swaps atomically; {@code expectedInstances}
 * is consumed by degradation wiring (fallback store configuration).
 */
public record QuotaFlowConfiguration(PolicySet policySet, Optional<Integer> expectedInstances) {

    public QuotaFlowConfiguration {
        if (policySet == null) {
            throw new NullPointerException("policySet");
        }
        if (expectedInstances == null) {
            throw new NullPointerException("expectedInstances");
        }
    }
}
