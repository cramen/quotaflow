package io.quotaflow.core.observation;

import java.util.Map;
import java.util.Set;

/** Generation membership used to retire policy and key-group observations safely. */
public record ObservationConfiguration(long generation, Map<String, String> policyRoots, Set<String> throttlePolicies) {
    public ObservationConfiguration {
        if (generation < 0) throw new IllegalArgumentException("negative observation generation");
        policyRoots = Map.copyOf(policyRoots); throttlePolicies = Set.copyOf(throttlePolicies);
    }
}
