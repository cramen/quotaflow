package io.quotaflow.core;

/** Engine-internal evaluation outcome: the decision plus the fired level's
 * cardinality-safe key group for observability hooks. */
final class Evaluation {

    private final Decision decision;
    private final String keyGroup;

    Evaluation(Decision decision, String keyGroup) {
        this.decision = decision;
        this.keyGroup = keyGroup;
    }

    Decision decision() {
        return decision;
    }

    String keyGroup() {
        return keyGroup;
    }
}
