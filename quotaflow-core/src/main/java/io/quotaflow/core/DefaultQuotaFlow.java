package io.quotaflow.core;

import io.quotaflow.core.store.RateLimitStore;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default {@link QuotaFlow} composing a policy set, key resolvers, a
 * {@link RateLimitStore} and {@link DecisionListener}s.
 *
 * <p>The compiled {@link PolicySet} is held in an {@link AtomicReference} and
 * read exactly once per decision, so {@link #replacePolicySet(PolicySet)}
 * swaps configuration atomically: every decision uses one consistent set,
 * never a mix of old and new.
 */
public final class DefaultQuotaFlow implements QuotaFlow {

    private final AtomicReference<PolicySet> policySets;
    private final PolicyEngine engine;
    private final List<DecisionListener> listeners;

    private DefaultQuotaFlow(Builder builder) {
        this.policySets = new AtomicReference<>(builder.policySet);
        this.engine = new PolicyEngine(
                builder.store, builder.defaultResolver, builder.namedResolvers, builder.limitResolver);
        this.listeners = List.copyOf(builder.listeners);
    }

    public static Builder builder(PolicySet policySet, RateLimitStore store) {
        return new Builder(policySet, store);
    }

    /** Atomically replaces the compiled policy set for subsequent decisions. */
    public void replacePolicySet(PolicySet policySet) {
        policySets.set(Objects.requireNonNull(policySet, "policySet"));
    }

    @Override
    public Decision tryAcquire(String policyId, RateLimitContext context) {
        return tryAcquire(policyId, context, 1);
    }

    @Override
    public Decision tryAcquire(String policyId, RateLimitContext context, long weight) {
        Evaluation evaluation = engine.evaluateInternal(policySets.get(), policyId, context, weight);
        notifyListeners(evaluation);
        return evaluation.decision();
    }

    @Override
    public CompletionStage<Decision> tryAcquireAsync(String policyId, RateLimitContext context, long weight) {
        return engine
                .evaluateInternalAsync(policySets.get(), policyId, context, weight)
                .thenApply(evaluation -> {
                    notifyListeners(evaluation);
                    return evaluation.decision();
                });
    }

    private void notifyListeners(Evaluation evaluation) {
        for (DecisionListener listener : listeners) {
            listener.onDecision(evaluation.decision(), evaluation.keyGroup());
        }
    }

    public static final class Builder {
        private final PolicySet policySet;
        private final RateLimitStore store;
        private KeyResolver defaultResolver = KeyResolvers.scopeBased();
        private final Map<String, KeyResolver> namedResolvers = new LinkedHashMap<>();
        private final List<DecisionListener> listeners = new ArrayList<>();
        private LimitResolver limitResolver;

        private Builder(PolicySet policySet, RateLimitStore store) {
            this.policySet = Objects.requireNonNull(policySet, "policySet");
            this.store = Objects.requireNonNull(store, "store");
        }

        /** Default resolver used for policies without a {@code keyResolverId}. */
        public Builder defaultResolver(KeyResolver resolver) {
            this.defaultResolver = Objects.requireNonNull(resolver, "resolver");
            return this;
        }

        /** Registers a resolver under the id policies reference via {@code keyResolverId}. */
        public Builder addResolver(String id, KeyResolver resolver) {
            namedResolvers.put(Objects.requireNonNull(id, "id"), Objects.requireNonNull(resolver, "resolver"));
            return this;
        }

        /**
         * Resolver for policies declaring a dynamic {@code limitRef}. Expected
         * to be a caching wrapper; the engine consults it once per resolution.
         */
        public Builder limitResolver(LimitResolver limitResolver) {
            this.limitResolver = Objects.requireNonNull(limitResolver, "limitResolver");
            return this;
        }

        public Builder addListener(DecisionListener listener) {
            listeners.add(Objects.requireNonNull(listener, "listener"));
            return this;
        }

        public DefaultQuotaFlow build() {
            return new DefaultQuotaFlow(this);
        }
    }
}
