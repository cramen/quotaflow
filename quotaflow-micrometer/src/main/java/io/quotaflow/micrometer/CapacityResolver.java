package io.quotaflow.micrometer;

import java.util.OptionalLong;

/**
 * Legacy capacity callback retained for source compatibility.
 * Budget gauges now consume paired effective capacity/remaining observations;
 * metric adapters never invoke this callback to reconstruct a decision.
 * @deprecated Use observation metadata from the evaluated store result.
 */
@Deprecated
@FunctionalInterface
public interface CapacityResolver {

    OptionalLong capacity(String policyId, String keyGroup);
}
