package io.quotaflow.core.observation;

import io.quotaflow.core.Decision;

/** Ordered, nonblocking callbacks scoped to one limiter; the facade owns session closure. */
public interface ObservationSession extends AutoCloseable {
    default void onConfiguration(ObservationConfiguration configuration) { }
    default void onBudget(BudgetObservation observation) { }
    default void onQueued(long generation, String queuePolicyId, String keyGroup) { }
    default void onDecision(long generation, Decision decision, String keyGroup) { }
    /** All prior events and in-flight owners of this generation have drained. */
    default void onRetired(long generation) { }
    @Override default void close() { }
}
