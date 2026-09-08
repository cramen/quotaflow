package io.quotaflow.core;

/**
 * Observability hook fired for every decision, allowed or rejected.
 * Implementations receive the {@code keyGroup} of the fired level only —
 * the raw key is not part of this signature, keeping metric cardinality
 * bounded by construction. The Micrometer bridge is a separate module.
 */
@FunctionalInterface
public interface DecisionListener {

    void onDecision(Decision decision, String keyGroup);
}
