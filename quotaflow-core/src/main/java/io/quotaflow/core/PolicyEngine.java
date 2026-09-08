package io.quotaflow.core;

import io.quotaflow.core.store.RateLimitStore;
import io.quotaflow.core.store.StoreResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Evaluates requests against hierarchical policy chains. The entry point
 * addresses a leaf policy; the engine walks parents up to the root and
 * evaluates the chain root-to-leaf (global → tenant → user → key),
 * short-circuiting on the first rejection.
 *
 * <p>Parent levels consume tokens even when a child level later rejects —
 * cross-level atomicity is intentionally not provided (accepted trade-off:
 * slight over-accounting at parent levels under contention).
 *
 * <p>{@link Reaction#THROTTLE} is reserved; throttle policies are currently
 * evaluated exactly like {@link Reaction#REJECT}.
 */
public final class PolicyEngine {

    private final RateLimitStore store;
    private final KeyResolver defaultResolver;
    private final Map<String, KeyResolver> namedResolvers;

    /** Engine with the default scope-based resolver and no named resolvers. */
    public PolicyEngine(RateLimitStore store) {
        this(store, KeyResolvers.scopeBased(), Map.of());
    }

    public PolicyEngine(
            RateLimitStore store, KeyResolver defaultResolver, Map<String, KeyResolver> namedResolvers) {
        this.store = Objects.requireNonNull(store, "store");
        this.defaultResolver = Objects.requireNonNull(defaultResolver, "defaultResolver");
        this.namedResolvers = Map.copyOf(Objects.requireNonNull(namedResolvers, "namedResolvers"));
    }

    /**
     * Evaluates {@code weight} tokens against the chain containing
     * {@code leafPolicyId}.
     *
     * @throws PolicyConfigurationException if the policy id is unknown or a
     *         policy references an unregistered key resolver
     * @throws IllegalArgumentException if {@code weight} is less than 1
     */
    public Decision evaluate(PolicySet policies, String leafPolicyId, RateLimitContext context, long weight) {
        return evaluateInternal(policies, leafPolicyId, context, weight).decision();
    }

    /** Asynchronous variant of {@link #evaluate}. */
    public CompletionStage<Decision> evaluateAsync(
            PolicySet policies, String leafPolicyId, RateLimitContext context, long weight) {
        return evaluateInternalAsync(policies, leafPolicyId, context, weight)
                .thenApply(Evaluation::decision);
    }

    Evaluation evaluateInternal(PolicySet policies, String leafPolicyId, RateLimitContext context, long weight) {
        List<PendingLevel> levels = preflight(policies, leafPolicyId, context, weight);
        long minRemaining = Long.MAX_VALUE;
        for (PendingLevel level : levels) {
            if (level.missingKey()) {
                return missingKeyRejection(level);
            }
            StoreResult result =
                    store.tryAcquire(level.storageKey(), level.policy().limit(), level.policy().algorithm(), weight);
            if (!result.acquired()) {
                return rejection(level, result);
            }
            minRemaining = Math.min(minRemaining, result.remaining());
        }
        return allowed(levels.get(levels.size() - 1), minRemaining);
    }

    CompletionStage<Evaluation> evaluateInternalAsync(
            PolicySet policies, String leafPolicyId, RateLimitContext context, long weight) {
        List<PendingLevel> levels;
        try {
            levels = preflight(policies, leafPolicyId, context, weight);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        return acquireChain(levels, 0, weight, Long.MAX_VALUE);
    }

    private CompletionStage<Evaluation> acquireChain(
            List<PendingLevel> levels, int index, long weight, long minRemaining) {
        if (index == levels.size()) {
            return CompletableFuture.completedFuture(allowed(levels.get(levels.size() - 1), minRemaining));
        }
        PendingLevel level = levels.get(index);
        if (level.missingKey()) {
            return CompletableFuture.completedFuture(missingKeyRejection(level));
        }
        return store
                .tryAcquireAsync(level.storageKey(), level.policy().limit(), level.policy().algorithm(), weight)
                .thenCompose(result -> {
                    if (!result.acquired()) {
                        return CompletableFuture.completedFuture(rejection(level, result));
                    }
                    return acquireChain(levels, index + 1, weight, Math.min(minRemaining, result.remaining()));
                });
    }

    /**
     * Resolves the chain and a storage key per level. A missing key with no
     * configured default short-circuits to a synthetic rejection level that
     * never reaches the store.
     */
    private List<PendingLevel> preflight(PolicySet policies, String leafPolicyId, RateLimitContext context, long weight) {
        if (weight < 1) {
            throw new IllegalArgumentException("weight must be >= 1, got " + weight);
        }
        Objects.requireNonNull(context, "context");
        List<RateLimitPolicy> chain = policies.chainFromLeaf(leafPolicyId);
        List<PendingLevel> levels = new ArrayList<>(chain.size());
        for (RateLimitPolicy policy : chain) {
            KeyResolver resolver = resolverFor(policy);
            Optional<LimitKey> resolved = resolver.resolve(context, policy);
            if (resolved.isEmpty()) {
                if (policy.defaultKey().isPresent()) {
                    resolved = Optional.of(new LimitKey(policy.defaultKey().get(), "default"));
                } else {
                    levels.add(PendingLevel.missingKey(policy));
                    return levels;
                }
            }
            LimitKey key = resolved.get();
            String storageKey = policy.id() + ':' + policy.scope().wireName() + ':' + key.rawKey();
            levels.add(PendingLevel.resolved(policy, storageKey, key.keyGroup()));
        }
        return levels;
    }

    private KeyResolver resolverFor(RateLimitPolicy policy) {
        if (policy.keyResolverId().isPresent()) {
            String resolverId = policy.keyResolverId().get();
            KeyResolver resolver = namedResolvers.get(resolverId);
            if (resolver == null) {
                throw new PolicyConfigurationException(
                        "policy '" + policy.id() + "' references unknown key resolver '" + resolverId + "'");
            }
            return resolver;
        }
        return defaultResolver;
    }

    private Evaluation rejection(PendingLevel level, StoreResult result) {
        Decision decision = Decision.rejected(
                level.policy().id(),
                level.policy().scope(),
                result.remaining(),
                Duration.ofMillis(result.retryAfterMillis()));
        return new Evaluation(decision, level.keyGroup());
    }

    private Evaluation missingKeyRejection(PendingLevel level) {
        Decision decision =
                Decision.rejectedWithoutSchedule(level.policy().id(), level.policy().scope());
        return new Evaluation(decision, level.keyGroup());
    }

    private Evaluation allowed(PendingLevel leaf, long minRemaining) {
        RateLimitPolicy policy = leaf.policy();
        return new Evaluation(Decision.allowed(policy.id(), policy.scope(), minRemaining), leaf.keyGroup());
    }

    private static final class PendingLevel {

        private final RateLimitPolicy policy;
        private final String storageKey;
        private final String keyGroup;
        private final boolean missingKey;

        private PendingLevel(RateLimitPolicy policy, String storageKey, String keyGroup, boolean missingKey) {
            this.policy = policy;
            this.storageKey = storageKey;
            this.keyGroup = keyGroup;
            this.missingKey = missingKey;
        }

        static PendingLevel resolved(RateLimitPolicy policy, String storageKey, String keyGroup) {
            return new PendingLevel(policy, storageKey, keyGroup, false);
        }

        static PendingLevel missingKey(RateLimitPolicy policy) {
            return new PendingLevel(policy, null, "unresolvable", true);
        }

        RateLimitPolicy policy() {
            return policy;
        }

        String storageKey() {
            return storageKey;
        }

        String keyGroup() {
            return keyGroup;
        }

        boolean missingKey() {
            return missingKey;
        }
    }
}
