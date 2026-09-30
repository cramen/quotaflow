package io.quotaflow.core.observation;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.store.StoreBudget;
import java.util.Objects;

/** An evaluated level's immutable budget; no raw identity is carried. */
public record BudgetSample(String policyId, String keyGroup, Algorithm algorithm, StoreBudget budget) {
    public BudgetSample {
        Objects.requireNonNull(policyId, "policyId"); Objects.requireNonNull(keyGroup, "keyGroup");
        Objects.requireNonNull(algorithm, "algorithm"); Objects.requireNonNull(budget, "budget");
    }
}
