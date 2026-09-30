package io.quotaflow.config;

import io.quotaflow.core.PolicySet;
import java.util.Optional;

/** Validated policies and normalized startup accounting, captured before any reload publication. */
public record QuotaFlowConfiguration(PolicySet policySet, Optional<Integer> expectedInstances, StartupAccounting accounting) {

    public QuotaFlowConfiguration(PolicySet policySet, Optional<Integer> expectedInstances) {
        this(policySet, expectedInstances, StartupAccounting.defaults());
    }

    public QuotaFlowConfiguration {
        if (accounting == null) throw new NullPointerException("accounting");
        if (expectedInstances != null && expectedInstances.orElse(1) != accounting.expectedInstances())
            throw new IllegalArgumentException("expected instances differ from startup accounting");
        if (policySet == null) {
            throw new NullPointerException("policySet");
        }
        if (expectedInstances == null) {
            throw new NullPointerException("expectedInstances");
        }
    }
}
