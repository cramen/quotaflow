package io.quotaflow.spring;

import io.quotaflow.config.ConfigSource;
import io.quotaflow.config.ConfigurationParser;
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

    /** Re-parses the source payload; only invoked after the reloader applied it successfully. */
    void refreshFrom(ConfigSource source) {
        set(ConfigurationParser.parse(source.load()).policySet());
    }

    @Override
    public PolicySet get() {
        return reference.get();
    }
}
