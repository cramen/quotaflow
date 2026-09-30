package io.quotaflow.spring;

import io.quotaflow.core.PolicySet;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Shared read view of the currently serving {@link PolicySet}: set once at
 * startup and refreshed after every applied reload, so consumers outside the
 * facade (the annotation interceptor, the micrometer bridges) observe the same
 * configuration the engine enforces without a back-channel into it.
 */
final class PolicySetReference implements Supplier<PolicySet> {

    private final AtomicReference<PolicySet> reference = new AtomicReference<>();

    void set(PolicySet policySet) {
        reference.set(Objects.requireNonNull(policySet, "policySet"));
    }

    void initialize(PolicySet policySet) {
        reference.compareAndSet(null, Objects.requireNonNull(policySet, "policySet"));
    }

    @Override
    public PolicySet get() {
        return reference.get();
    }
}
