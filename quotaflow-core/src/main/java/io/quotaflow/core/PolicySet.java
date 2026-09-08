package io.quotaflow.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable, validated set of policies. Compilation fails fast with an
 * actionable {@link PolicyConfigurationException} on cycles, unknown parents,
 * invalid limits and scope-order violations. Configuration updates take effect
 * by atomically replacing the whole compiled set.
 */
public final class PolicySet {

    private final Map<String, RateLimitPolicy> policies;

    private PolicySet(Map<String, RateLimitPolicy> policies) {
        this.policies = policies;
    }

    /**
     * Validates and compiles the given policies into an immutable set.
     *
     * @throws PolicyConfigurationException on any validation failure
     */
    public static PolicySet compile(Collection<RateLimitPolicy> policies) {
        Objects.requireNonNull(policies, "policies");
        Map<String, RateLimitPolicy> byId = new LinkedHashMap<>();
        for (RateLimitPolicy policy : policies) {
            RateLimitPolicy previous = byId.putIfAbsent(policy.id(), policy);
            if (previous != null) {
                throw new PolicyConfigurationException(
                        "duplicate policy id '" + policy.id() + "'");
            }
        }
        if (byId.isEmpty()) {
            throw new PolicyConfigurationException("policy set must contain at least one policy");
        }
        for (RateLimitPolicy policy : byId.values()) {
            validateChain(policy, byId);
        }
        return new PolicySet(Map.copyOf(byId));
    }

    /** Returns the policy or throws if the id is unknown (caller misuse). */
    public RateLimitPolicy policy(String id) {
        RateLimitPolicy policy = policies.get(id);
        if (policy == null) {
            throw new PolicyConfigurationException(
                    "unknown policy id '" + id + "'; known ids: " + policies.keySet());
        }
        return policy;
    }

    /**
     * Returns the chain containing the given leaf, ordered root-to-leaf
     * (broadest scope first).
     */
    public List<RateLimitPolicy> chainFromLeaf(String leafId) {
        Deque<RateLimitPolicy> chain = new ArrayDeque<>();
        RateLimitPolicy current = policy(leafId);
        while (current != null) {
            chain.addFirst(current);
            current = current.parentId().map(policies::get).orElse(null);
        }
        return List.copyOf(chain);
    }

    public int size() {
        return policies.size();
    }

    public Collection<RateLimitPolicy> policies() {
        return policies.values();
    }

    private static void validateChain(RateLimitPolicy start, Map<String, RateLimitPolicy> byId) {
        List<String> path = new ArrayList<>();
        RateLimitPolicy current = start;
        while (true) {
            path.add(current.id());
            String parentId = current.parentId().orElse(null);
            if (parentId == null) {
                return;
            }
            RateLimitPolicy parent = byId.get(parentId);
            if (parent == null) {
                throw new PolicyConfigurationException(
                        "policy '" + current.id() + "' declares unknown parent '" + parentId + "'");
            }
            int cycleAt = path.indexOf(parentId);
            if (cycleAt >= 0) {
                List<String> cycle = new ArrayList<>(path.subList(cycleAt, path.size()));
                cycle.add(parentId);
                throw new PolicyConfigurationException(
                        "cycle in parent references: " + String.join(" -> ", cycle));
            }
            if (!parent.scope().isBroaderThan(current.scope())) {
                throw new PolicyConfigurationException(
                        "policy '" + current.id() + "' has scope " + current.scope()
                                + " but its parent '" + parent.id() + "' has scope " + parent.scope()
                                + "; a child scope must be strictly narrower than its parent's scope");
            }
            current = parent;
        }
    }
}
