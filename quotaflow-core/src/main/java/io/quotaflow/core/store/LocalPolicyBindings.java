package io.quotaflow.core.store;

import io.quotaflow.core.PolicyConfigurationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/** Atomic, bounded policy history. Removing a serving policy never erases its binding. */
public final class LocalPolicyBindings {
    public static final int DEFAULT_MAX_REGISTERED_POLICIES = 4096;
    private record Key(String namespace, String policyId) { }
    private final int maximum;
    private final ReentrantLock registration = new ReentrantLock();
    private volatile Map<Key, PolicyBinding> bindings = Map.of();

    public LocalPolicyBindings() {
        this(DEFAULT_MAX_REGISTERED_POLICIES);
    }

    public LocalPolicyBindings(int maximum) {
        if (maximum < 1) throw new IllegalArgumentException("maximum registrations must be positive");
        this.maximum = maximum;
    }

    public void register(List<PolicyBinding> candidate) {
        List<PolicyBinding> copy = List.copyOf(candidate);
        Map<Key, PolicyBinding> current = bindings;
        if (copy.stream().allMatch(binding -> binding.equals(current.get(key(binding))))) return;
        registration.lock();
        try {
            Map<Key, PolicyBinding> next = new HashMap<>(bindings);
            Map<String, Integer> sizes = new HashMap<>();
            next.keySet().forEach(key -> sizes.merge(key.namespace(), 1, Integer::sum));
            for (PolicyBinding binding : copy) {
                PolicyBinding previous = next.putIfAbsent(key(binding), binding);
                if (previous != null && !previous.equals(binding)) {
                    throw new PolicyConfigurationException("policy scope or root domain conflicts with registered identity");
                }
                if (previous == null && sizes.merge(binding.domain().namespace(), 1, Integer::sum) > maximum) {
                    throw new PolicyConfigurationException("policy registration budget is exhausted; identity history cannot be evicted");
                }
            }
            bindings = Map.copyOf(next);
        } finally {
            registration.unlock();
        }
    }

    public void register(BucketIdentity bucket) {
        PolicyBinding binding = PolicyBinding.of(bucket);
        if (!binding.equals(bindings.get(key(binding)))) register(List.of(binding));
    }

    public int size(String namespace) {
        return (int) bindings.keySet().stream().filter(key -> key.namespace().equals(namespace)).count();
    }

    private static Key key(PolicyBinding binding) {
        return new Key(binding.domain().namespace(), binding.policyId());
    }
}
