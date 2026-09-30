package io.quotaflow.core;

/** Engine-internal evaluation outcome: the decision plus the fired level's
 * cardinality-safe key group for observability hooks. */
final class Evaluation {

    private final Decision decision;
    private final String keyGroup;
    private final io.quotaflow.core.store.RecoveryPending recoveryPending;

    Evaluation(Decision decision, String keyGroup) {
        this(decision, keyGroup, null);
    }

    Evaluation(Decision decision, String keyGroup, io.quotaflow.core.store.RecoveryPending recoveryPending) {
        this.recoveryPending = recoveryPending;
        this.decision = decision;
        this.keyGroup = keyGroup;
    }

    io.quotaflow.core.store.RecoveryPending recoveryPending() { return recoveryPending; }

    Decision decision() {
        return decision;
    }

    String keyGroup() {
        return keyGroup;
    }
}
