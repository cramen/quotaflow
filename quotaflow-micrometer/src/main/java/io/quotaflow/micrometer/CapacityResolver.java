package io.quotaflow.micrometer;

import java.util.OptionalLong;

/**
 * Supplies the capacity used as the denominator of the
 * {@code quotaflow.utilization} gauge for one (policy, key-group) pair. An
 * empty result means the capacity is unknown (for example an unresolvable
 * dynamic limit); the gauge then keeps its last value.
 */
@FunctionalInterface
public interface CapacityResolver {

    OptionalLong capacity(String policyId, String keyGroup);
}
