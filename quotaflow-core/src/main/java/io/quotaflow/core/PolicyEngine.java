package io.quotaflow.core;

import io.quotaflow.core.store.BatchRateLimitStore;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates requests against hierarchical policy chains. The entry point
 * addresses a leaf policy; the engine walks parents up to the root and
 * evaluates the chain root-to-leaf (global → tenant → user → key),
 * short-circuiting on the first rejection.
 *
 * <p>When the store implements {@link BatchRateLimitStore}, the whole chain is
 * evaluated atomically in one store-side execution: a rejection at any level
 * consumes no tokens at any level. Otherwise the chain is evaluated level by
 * level and parent levels consume tokens even when a child level later rejects
 * (accepted trade-off: bounded over-accounting at parent levels under
 * contention). Decision semantics — fired level, minimum remaining, retry-after
 * — are identical on both paths.
 *
 * <p>{@link Reaction#THROTTLE} is reserved; throttle policies are currently
 * evaluated exactly like {@link Reaction#REJECT}.
 *
 * <p>A policy declaring a dynamic {@code limitRef} has its effective limit
 * resolved through the configured {@link LimitResolver} during preflight,
 * before any store evaluation, on both the synchronous and the asynchronous
 * path; the resolved limit then flows through the unchanged store semantics.
 * An unresolvable reference rejects the request without a refill schedule,
 * exactly like an unresolvable key. Policies with a static limit take the
 * zero-cost path and never touch the resolver.
 */
public final class PolicyEngine {

    private static final Logger log = LoggerFactory.getLogger(PolicyEngine.class);

    private final RateLimitStore store;
    private final KeyResolver defaultResolver;
    private final Map<String, KeyResolver> namedResolvers;
    private final LimitResolver limitResolver;

    /** Engine with the default scope-based resolver and no named resolvers. */
    public PolicyEngine(RateLimitStore store) {
        this(store, KeyResolvers.scopeBased(), Map.of());
    }

    public PolicyEngine(
            RateLimitStore store, KeyResolver defaultResolver, Map<String, KeyResolver> namedResolvers) {
        this(store, defaultResolver, namedResolvers, null);
    }

    /**
     * Full engine configuration. {@code limitResolver} may be {@code null};
     * policies declaring a {@code limitRef} then fail evaluation with a
     * {@link PolicyConfigurationException}.
     */
    public PolicyEngine(
            RateLimitStore store,
            KeyResolver defaultResolver,
            Map<String, KeyResolver> namedResolvers,
            LimitResolver limitResolver) {
        this.store = Objects.requireNonNull(store, "store");
        this.defaultResolver = Objects.requireNonNull(defaultResolver, "defaultResolver");
        this.namedResolvers = Map.copyOf(Objects.requireNonNull(namedResolvers, "namedResolvers"));
        this.limitResolver = limitResolver;
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
        if (store instanceof BatchRateLimitStore batchStore) {
            List<LevelRequest> requests = levelRequests(levels, weight);
            if (requests.isEmpty()) {
                return rejectedBeforeStore(levels.get(0));
            }
            ChainResult result = batchStore.tryAcquireAll(requests).toCompletableFuture().join();
            return batchEvaluation(levels, requests.size(), result);
        }
        long minRemaining = Long.MAX_VALUE;
        for (PendingLevel level : levels) {
            if (!level.reachesStore()) {
                return rejectedBeforeStore(level);
            }
            StoreResult result =
                    store.tryAcquire(level.storageKey(), level.limit(), level.policy().algorithm(), weight);
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
        if (store instanceof BatchRateLimitStore batchStore) {
            List<LevelRequest> requests = levelRequests(levels, weight);
            if (requests.isEmpty()) {
                return CompletableFuture.completedFuture(rejectedBeforeStore(levels.get(0)));
            }
            return batchStore.tryAcquireAll(requests)
                    .thenApply(result -> batchEvaluation(levels, requests.size(), result));
        }
        return acquireChain(levels, 0, weight, Long.MAX_VALUE);
    }

    /**
     * Store-facing view of the resolved prefix of {@code levels}: a trailing
     * pre-store rejection level (missing key or unresolvable limit) never
     * reaches the store.
     */
    private static List<LevelRequest> levelRequests(List<PendingLevel> levels, long weight) {
        List<LevelRequest> requests = new ArrayList<>(levels.size());
        for (PendingLevel level : levels) {
            if (!level.reachesStore()) {
                break;
            }
            requests.add(new LevelRequest(
                    level.storageKey(), level.limit(), level.policy().algorithm(), weight));
        }
        return requests;
    }

    /**
     * Maps an atomic chain outcome onto the same decision the per-level path
     * would produce. {@code resolvedCount} is the number of levels that were
     * sent to the store; a level beyond it is the trailing pre-store rejection
     * level, which rejects without a refill schedule after the allowed store
     * levels.
     */
    private static Evaluation batchEvaluation(
            List<PendingLevel> levels, int resolvedCount, ChainResult result) {
        if (!result.acquired()) {
            PendingLevel fired = levels.get(result.firedLevelIndex());
            return rejection(fired, result.remaining(), result.retryAfterMillis());
        }
        if (resolvedCount < levels.size()) {
            return rejectedBeforeStore(levels.get(resolvedCount));
        }
        return allowed(levels.get(result.firedLevelIndex()), result.remaining());
    }

    private CompletionStage<Evaluation> acquireChain(
            List<PendingLevel> levels, int index, long weight, long minRemaining) {
        if (index == levels.size()) {
            return CompletableFuture.completedFuture(allowed(levels.get(levels.size() - 1), minRemaining));
        }
        PendingLevel level = levels.get(index);
        if (!level.reachesStore()) {
            return CompletableFuture.completedFuture(rejectedBeforeStore(level));
        }
        return store
                .tryAcquireAsync(level.storageKey(), level.limit(), level.policy().algorithm(), weight)
                .thenCompose(result -> {
                    if (!result.acquired()) {
                        return CompletableFuture.completedFuture(rejection(level, result));
                    }
                    return acquireChain(levels, index + 1, weight, Math.min(minRemaining, result.remaining()));
                });
    }

    /**
     * Resolves the chain, a storage key and an effective limit per level. A
     * missing key with no configured default, or an unresolvable dynamic limit
     * reference, short-circuits to a synthetic rejection level that never
     * reaches the store.
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
            Limit limit = effectiveLimit(policy, key.keyGroup());
            if (limit == null) {
                log.warn(
                        "policy '{}' limit reference '{}' is unresolvable for key group '{}'; rejecting request",
                        policy.id(), policy.limitRef().orElseThrow(), key.keyGroup());
                levels.add(PendingLevel.unresolvableLimit(policy, key.keyGroup()));
                return levels;
            }
            String storageKey = policy.id() + ':' + policy.scope().wireName() + ':' + key.rawKey();
            levels.add(PendingLevel.resolved(policy, storageKey, key.keyGroup(), limit));
        }
        return levels;
    }

    /**
     * Effective limit of one level: the static limit at zero cost, or the
     * {@link LimitResolver} result for a dynamic reference. Returns
     * {@code null} when the reference cannot be resolved for the key group.
     */
    private Limit effectiveLimit(RateLimitPolicy policy, String keyGroup) {
        if (policy.limitRef().isEmpty()) {
            return policy.limit().orElseThrow();
        }
        if (limitResolver == null) {
            throw new PolicyConfigurationException("policy '" + policy.id() + "' declares limit reference '"
                    + policy.limitRef().orElseThrow() + "' but no LimitResolver is configured");
        }
        return limitResolver.resolve(policy.limitRef().orElseThrow(), keyGroup).orElse(null);
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

    private static Evaluation rejection(PendingLevel level, StoreResult result) {
        return rejection(level, result.remaining(), result.retryAfterMillis());
    }

    private static Evaluation rejection(PendingLevel level, long remaining, long retryAfterMillis) {
        Decision decision = Decision.rejected(
                level.policy().id(),
                level.policy().scope(),
                remaining,
                Duration.ofMillis(retryAfterMillis));
        return new Evaluation(decision, level.keyGroup());
    }

    private static Evaluation rejectedBeforeStore(PendingLevel level) {
        Decision decision =
                Decision.rejectedWithoutSchedule(level.policy().id(), level.policy().scope());
        return new Evaluation(decision, level.keyGroup());
    }

    private static Evaluation allowed(PendingLevel leaf, long minRemaining) {
        RateLimitPolicy policy = leaf.policy();
        return new Evaluation(Decision.allowed(policy.id(), policy.scope(), minRemaining), leaf.keyGroup());
    }

    private static final class PendingLevel {

        private final RateLimitPolicy policy;
        private final String storageKey;
        private final String keyGroup;
        private final Limit limit;

        private PendingLevel(RateLimitPolicy policy, String storageKey, String keyGroup, Limit limit) {
            this.policy = policy;
            this.storageKey = storageKey;
            this.keyGroup = keyGroup;
            this.limit = limit;
        }

        static PendingLevel resolved(RateLimitPolicy policy, String storageKey, String keyGroup, Limit limit) {
            return new PendingLevel(policy, storageKey, keyGroup, limit);
        }

        static PendingLevel missingKey(RateLimitPolicy policy) {
            return new PendingLevel(policy, null, "unresolvable", null);
        }

        static PendingLevel unresolvableLimit(RateLimitPolicy policy, String keyGroup) {
            return new PendingLevel(policy, null, keyGroup, null);
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

        Limit limit() {
            return limit;
        }

        /** Levels rejected before store evaluation (missing key, unresolvable limit). */
        boolean reachesStore() {
            return limit != null;
        }
    }
}
