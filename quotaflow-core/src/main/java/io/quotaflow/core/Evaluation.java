package io.quotaflow.core;

/** Engine-internal evaluation outcome: the decision plus the fired level's
 * cardinality-safe key group for observability hooks. */
final class Evaluation {

    private final java.util.List<io.quotaflow.core.observation.BudgetSample> budgets;
    private final String rootPolicyId;
    private final long resolverRevision;
    private final Decision decision;
    private final String keyGroup;
    private final io.quotaflow.core.store.RecoveryPending recoveryPending;

    Evaluation(Decision decision, String keyGroup) {
        this(decision, keyGroup, null);
    }

    Evaluation(Decision decision, String keyGroup, io.quotaflow.core.store.RecoveryPending recoveryPending) {
        this(decision, keyGroup, recoveryPending, java.util.List.of(), null, 0);
    }
    private Evaluation(Decision decision, String keyGroup, io.quotaflow.core.store.RecoveryPending recoveryPending,
            java.util.List<io.quotaflow.core.observation.BudgetSample> budgets, String rootPolicyId, long resolverRevision) {
        this.budgets = java.util.List.copyOf(budgets); this.rootPolicyId = rootPolicyId; this.resolverRevision = resolverRevision;
        this.recoveryPending = recoveryPending;
        this.decision = decision;
        this.keyGroup = keyGroup;
    }

    Evaluation observed(java.util.List<io.quotaflow.core.observation.BudgetSample> budgets, String root, long revision) {
        return new Evaluation(decision, keyGroup, recoveryPending, budgets, root, revision);
    }
    java.util.List<io.quotaflow.core.observation.BudgetSample> budgets() { return budgets; }
    String rootPolicyId() { return rootPolicyId; }
    long resolverRevision() { return resolverRevision; }

    io.quotaflow.core.store.RecoveryPending recoveryPending() { return recoveryPending; }

    Decision decision() {
        return decision;
    }

    String keyGroup() {
        return keyGroup;
    }
}
