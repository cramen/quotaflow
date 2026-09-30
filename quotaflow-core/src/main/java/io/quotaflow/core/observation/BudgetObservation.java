package io.quotaflow.core.observation;

import java.util.List;
import java.util.Objects;

/** A captured evaluation, ordered by observation sequence within one limiter owner. */
public record BudgetObservation(long generation, long sequence, String rootPolicyId,
                                long resolverRevision, List<BudgetSample> samples) {
    public BudgetObservation {
        if (generation < 0 || sequence < 0 || resolverRevision < 0) throw new IllegalArgumentException("negative observation version");
        Objects.requireNonNull(rootPolicyId, "rootPolicyId"); samples = List.copyOf(samples);
    }
}
