package io.quotaflow.fallback;

import io.quotaflow.core.Verdict;

/**
 * Observability hook for degradation: every state transition (with the reason)
 * and every decision made in fallback mode.
 *
 * <p>The signatures carry the aggregated key-group identity only — raw limit
 * keys are never part of this contract, keeping metric cardinality bounded by
 * construction (same rule as the decision listener in core). The metrics
 * module bridges these events to the degraded-state gauge and the fallback
 * decisions counter.
 */
public interface DegradationListener {

    /**
     * Fired on every state transition. {@code reason} is a short machine-safe
     * description (for example "3 consecutive store failures") and never
     * contains raw limit keys.
     */
    void onTransition(DegradationState from, DegradationState to, String reason);

    /**
     * Fired for every decision served by the local fallback while degraded.
     *
     * @param policyId the fired level's policy id (leaf when the chain allowed)
     * @param keyGroup the aggregated key-group identity, never the raw key
     */
    void onFallbackDecision(String policyId, String keyGroup, Verdict verdict);

    /** Published configuration membership; revisions are scoped to this store owner. */
    default void onConfiguration(long revision, java.util.Set<String> policies) { }
    /** Contextual form. Non-current requests can be counted without retaining obsolete series. */
    default void onFallbackDecision(long revision, boolean currentTarget, String policyId, String keyGroup, Verdict verdict) {
        onFallbackDecision(policyId, keyGroup, verdict);
    }
    /** All operations admitted under the retired revision have completed their observations. */
    default void onRetired(long revision) { }
    /** This store owner has stopped producing events. */
    default void onClosed() { }
}
