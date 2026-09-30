package io.quotaflow.core;

import io.quotaflow.core.store.BatchRateLimitStore;
import io.quotaflow.core.observation.BudgetSample;
import io.quotaflow.core.store.BucketIdentity;
import io.quotaflow.core.store.QuotaDomain;
import io.quotaflow.core.store.PolicyBinding;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.RateLimitStore;
import io.quotaflow.core.store.StoreResult;
import java.time.Duration;
import io.quotaflow.core.execution.BoundedExecution;
import java.util.function.BooleanSupplier;
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
 * <p>The engine returns pure decisions regardless of {@link Reaction}: a
 * rejected throttle-mode policy rejects exactly like {@link Reaction#REJECT}
 * here. Throttle waiting (queueing, retry, wait timeouts) is implemented by
 * the {@code DefaultQuotaFlow} facade above the engine.
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
    private final String namespace;
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
        this(store, defaultResolver, namedResolvers, limitResolver, QuotaDomain.DEFAULT_NAMESPACE);
    }

    public PolicyEngine(RateLimitStore store, KeyResolver defaultResolver,
            Map<String, KeyResolver> namedResolvers, LimitResolver limitResolver, String namespace) {
        this.namespace = new QuotaDomain(namespace, "validation").namespace();
        this.store = Objects.requireNonNull(store, "store");
        this.defaultResolver = Objects.requireNonNull(defaultResolver, "defaultResolver");
        this.namedResolvers = Map.copyOf(Objects.requireNonNull(namedResolvers, "namedResolvers"));
        this.limitResolver = limitResolver;
    }

    /** Validates a complete candidate before the facade publishes it. */
    public CompletionStage<Void> registerPolicies(PolicySet policies) {
        if (store.requiresVersionedLimits() && policies.policies().stream().anyMatch(policy -> policy.limitRef().isPresent())
                && !(limitResolver instanceof VersionedLimitResolver))
            throw new PolicyConfigurationException("fenced dynamic policies require a VersionedLimitResolver");
        if (store instanceof io.quotaflow.core.store.RecoveryConfigurationAware aware)
            aware.validateRecoveryConfiguration(policies, namespace, limitResolver);
        return store.registerPolicies(policies.policies().stream().map(policy -> new PolicyBinding(
                new QuotaDomain(namespace, policies.rootPolicyId(policy.id())),
                policy.id(), policy.scope(), policy.algorithm())).toList())
                .thenCompose(ignored -> store instanceof io.quotaflow.core.store.RecoveryConfigurationAware aware
                        ? aware.configureRecovery(policies, namespace, limitResolver) : CompletableFuture.completedFuture(null));
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
        for (PendingLevel level : levels) {
            if (level.limit() != null && weight > level.limit().capacity()) {
                return rejectedBeforeStore(level);
            }
        }
        if (store instanceof BatchRateLimitStore batchStore) {
            List<LevelRequest> requests = levelRequests(levels, weight, policies);
            if (requests.isEmpty()) {
                return rejectedBeforeStore(levels.get(0));
            }
            ChainResult result = batchStore.tryAcquireAll(requests).toCompletableFuture().join();
            return batchEvaluation(levels, requests.size(), result);
        }
        long minRemaining = Long.MAX_VALUE;
        var budgets = new ArrayList<BudgetSample>();
        for (PendingLevel level : levels) {
            if (!level.reachesStore()) {
                return observed(rejectedBeforeStore(level), levels, budgets);
            }
            StoreResult result =
                    store.tryAcquire(level.storageKey(), level.limit(), level.policy().algorithm(), weight);
            addBudget(budgets, level, result);
            if (!result.acquired()) {
                return observed(rejection(level, result), levels, budgets);
            }
            minRemaining = Math.min(minRemaining, result.remaining());
        }
        return observed(allowed(levels.get(levels.size() - 1), minRemaining), levels, budgets);
    }

    CompletionStage<Evaluation> evaluateInternalAsync(
            PolicySet policies, String leafPolicyId, RateLimitContext context, long weight) {
        return evaluateInternalAsync(policies, leafPolicyId, context, weight, BoundedExecution.shared(), () -> true);
    }

    CompletionStage<Evaluation> evaluateInternalAsync(PolicySet policies, String leafPolicyId,
            RateLimitContext context, long weight, BoundedExecution execution, BooleanSupplier active) {
        return execution.submit(() -> {
            Limit.validateWeight(weight);
            Objects.requireNonNull(context, "context");
            policies.policy(leafPolicyId);
            if (policies.hasDynamicLimits(leafPolicyId) && store.requiresVersionedLimits()
                    && !(limitResolver instanceof VersionedLimitResolver))
                throw new PolicyConfigurationException("fenced dynamic policies require a VersionedLimitResolver");
            return policies.hasDynamicLimits(leafPolicyId) && limitResolver instanceof VersionedLimitResolver versioned
                    ? Objects.requireNonNull(versioned.snapshot(), "snapshot result").orElse(null) : null;
        }, active).thenCompose(snapshot -> {
            if (policies.hasDynamicLimits(leafPolicyId) && limitResolver instanceof VersionedLimitResolver && snapshot == null)
                return CompletableFuture.completedFuture(rejectedBeforeStore(
                        PendingLevel.unresolvableLimit(policies.chainFromLeaf(leafPolicyId).get(0), "unresolvable")));
            return preflightAsync(policies, policies.chainFromLeaf(leafPolicyId), context, 0,
                    new ArrayList<>(), snapshot, execution, active).thenCompose(levels -> {
                for (PendingLevel level : levels) {
                    if (level.limit() != null && weight > level.limit().capacity())
                        return CompletableFuture.completedFuture(rejectedBeforeStore(level));
                }
                if (store instanceof BatchRateLimitStore batch) {
                    var requests = levelRequests(levels, weight, policies);
                    if (requests.isEmpty()) return CompletableFuture.completedFuture(rejectedBeforeStore(levels.get(0)));
                    return execution.submitStage(() -> batch.tryAcquireAll(requests), active)
                            .thenApply(result -> batchEvaluation(levels, requests.size(), result));
                }
                return acquireChain(levels, 0, weight, Long.MAX_VALUE, execution, active, new ArrayList<>());
            }).thenApply(evaluation -> evaluation.observed(evaluation.budgets(), policies.rootPolicyId(leafPolicyId),
                    snapshot == null ? 0 : snapshot.revision()));
        });
    }

    private CompletionStage<List<PendingLevel>> preflightAsync(PolicySet policies, List<RateLimitPolicy> chain,
            RateLimitContext context, int index, List<PendingLevel> levels, LimitSnapshot snapshot,
            BoundedExecution execution, BooleanSupplier active) {
        if (index == chain.size()) return CompletableFuture.completedFuture(levels);
        var policy = chain.get(index);
        return execution.submit(() -> resolverFor(policy).resolve(context, policy), active).thenCompose(resolved -> {
            var key = resolved.orElseGet(() -> policy.defaultKey().map(value -> new LimitKey(value, "default")).orElse(null));
            if (key == null) { levels.add(PendingLevel.missingKey(policy)); return CompletableFuture.completedFuture(levels); }
            CompletionStage<Optional<Limit>> limit;
            if (policy.limitRef().isEmpty()) limit = CompletableFuture.completedFuture(policy.limit());
            else if (snapshot != null) limit = CompletableFuture.completedFuture(snapshot.resolve(policy.limitRef().orElseThrow(), key.keyGroup()));
            else if (limitResolver instanceof AsyncLimitResolver async)
                limit = execution.submitStage(() -> async.resolveAsync(policy.limitRef().orElseThrow(), key.keyGroup()), active);
            else limit = execution.submit(() -> Optional.ofNullable(effectiveLimit(policy, key.keyGroup(), limitResolver)), active);
            return limit.thenCompose(value -> {
                if (value.isEmpty()) {
                    log.warn("policy '{}' limit reference '{}' is unresolvable for key group '{}'; rejecting request",
                            policy.id(), policy.limitRef().orElseThrow(), key.keyGroup());
                    levels.add(PendingLevel.unresolvableLimit(policy, key.keyGroup()));
                    return CompletableFuture.completedFuture(levels);
                }
                var identity = new BucketIdentity(new QuotaDomain(namespace, policies.rootPolicyId(policy.id())),
                        policy.id(), policy.scope(), key.rawKey());
                levels.add(PendingLevel.resolved(policy, identity, key.keyGroup(), value.get(), snapshot));
                return preflightAsync(policies, chain, context, index + 1, levels, snapshot, execution, active);
            });
        });
    }

    /**
     * Store-facing view of the resolved prefix of {@code levels}: a trailing
     * pre-store rejection level (missing key or unresolvable limit) never
     * reaches the store.
     */
    private List<LevelRequest> levelRequests(List<PendingLevel> levels, long weight, PolicySet policies) {
        List<LevelRequest> requests = new ArrayList<>(levels.size());
        for (PendingLevel level : levels) {
            if (!level.reachesStore()) {
                break;
            }
            requests.add(new LevelRequest(
                    level.storageKey(), level.limit(), level.policy().algorithm(), weight, level.keyGroup(),
                    store instanceof io.quotaflow.core.store.RecoveryConfigurationAware
                            ? policies.recoveryFingerprint(level.storageKey().domain().rootPolicyId()) : null,
                    level.resolverRevision, level.resolverFingerprint));
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
    private static Evaluation batchEvaluation(List<PendingLevel> levels, int resolvedCount, ChainResult result) {
        var budgets = new ArrayList<BudgetSample>();
        for (var sample : result.budgets()) {
            if (sample.level() >= resolvedCount) throw new io.quotaflow.core.store.StateCompatibilityException("budget level is outside the evaluated chain");
            var level = levels.get(sample.level());
            budgets.add(new BudgetSample(level.policy().id(), level.keyGroup(), level.policy().algorithm(), sample.budget()));
        }
        return observed(batchDecision(levels, resolvedCount, result), levels, budgets);
    }
    private static Evaluation observed(Evaluation evaluation, List<PendingLevel> levels, List<BudgetSample> budgets) {
        var first = levels.get(0);
        return evaluation.observed(budgets, first.storageKey() == null ? first.policy().id() : first.storageKey().domain().rootPolicyId(), first.resolverRevision);
    }
    private static void addBudget(List<BudgetSample> budgets, PendingLevel level, StoreResult result) {
        if (result.budget() != null) budgets.add(new BudgetSample(level.policy().id(), level.keyGroup(), level.policy().algorithm(), result.budget()));
    }
    private static Evaluation batchDecision(
            List<PendingLevel> levels, int resolvedCount, ChainResult result) {
        if (!result.acquired()) {
            PendingLevel fired = levels.get(result.firedLevelIndex());
            if (result.recoveryPending() != null) {
                return new Evaluation(Decision.rejectedWithoutSchedule(fired.policy().id(), fired.policy().scope()),
                        fired.keyGroup(), result.recoveryPending());
            }
            return rejection(fired, result.remaining(), result.retryAfterMillis());
        }
        if (resolvedCount < levels.size()) {
            return rejectedBeforeStore(levels.get(resolvedCount));
        }
        return allowed(levels.get(result.firedLevelIndex()), result.remaining());
    }

    private CompletionStage<Evaluation> acquireChain(
            List<PendingLevel> levels, int index, long weight, long minRemaining,
            BoundedExecution execution, BooleanSupplier active, List<BudgetSample> budgets) {
        if (index == levels.size()) {
            return CompletableFuture.completedFuture(observed(allowed(levels.get(levels.size() - 1), minRemaining), levels, budgets));
        }
        PendingLevel level = levels.get(index);
        if (!level.reachesStore()) {
            return CompletableFuture.completedFuture(observed(rejectedBeforeStore(level), levels, budgets));
        }
        return execution.submitStage(() -> store.tryAcquireAsync(level.storageKey(), level.limit(), level.policy().algorithm(), weight), active)
                .thenCompose(result -> {
                    addBudget(budgets, level, result);
                    if (!result.acquired()) {
                        return CompletableFuture.completedFuture(observed(rejection(level, result), levels, budgets));
                    }
                    return acquireChain(levels, index + 1, weight, Math.min(minRemaining, result.remaining()), execution, active, budgets);
                });
    }

    /**
     * Resolves the chain, a storage key and an effective limit per level. A
     * missing key with no configured default, or an unresolvable dynamic limit
     * reference, short-circuits to a synthetic rejection level that never
     * reaches the store.
     */
    private List<PendingLevel> preflight(PolicySet policies, String leafPolicyId, RateLimitContext context, long weight) {
        Limit.validateWeight(weight);
        Objects.requireNonNull(context, "context");
        List<RateLimitPolicy> chain = policies.chainFromLeaf(leafPolicyId);
        List<PendingLevel> levels = new ArrayList<>(chain.size());
        LimitSnapshot snapshot = null;
        if (policies.hasDynamicLimits(leafPolicyId) && store.requiresVersionedLimits() && !(limitResolver instanceof VersionedLimitResolver))
            throw new PolicyConfigurationException("fenced dynamic policies require a VersionedLimitResolver");
        if (policies.hasDynamicLimits(leafPolicyId) && limitResolver instanceof VersionedLimitResolver versioned) {
            snapshot = Objects.requireNonNull(versioned.snapshot(), "snapshot result").orElse(null);
            if (snapshot == null) return List.of(PendingLevel.unresolvableLimit(chain.get(0), "unresolvable"));
        }
        LimitResolver capturedResolver = snapshot == null ? limitResolver : snapshot;
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
            Limit limit = effectiveLimit(policy, key.keyGroup(), capturedResolver);
            if (limit == null) {
                log.warn(
                        "policy '{}' limit reference '{}' is unresolvable for key group '{}'; rejecting request",
                        policy.id(), policy.limitRef().orElseThrow(), key.keyGroup());
                levels.add(PendingLevel.unresolvableLimit(policy, key.keyGroup()));
                return levels;
            }
            BucketIdentity storageKey = new BucketIdentity(
                    new QuotaDomain(namespace, policies.rootPolicyId(policy.id())),
                    policy.id(), policy.scope(), key.rawKey());
            levels.add(PendingLevel.resolved(policy, storageKey, key.keyGroup(), limit, snapshot));
        }
        return levels;
    }

    /**
     * Effective limit of one level: the static limit at zero cost, or the
     * {@link LimitResolver} result for a dynamic reference. Returns
     * {@code null} when the reference cannot be resolved for the key group.
     */
    private Limit effectiveLimit(RateLimitPolicy policy, String keyGroup, LimitResolver capturedResolver) {
        if (policy.limitRef().isEmpty()) {
            return policy.limit().orElseThrow();
        }
        if (limitResolver == null) {
            throw new PolicyConfigurationException("policy '" + policy.id() + "' declares limit reference '"
                    + policy.limitRef().orElseThrow() + "' but no LimitResolver is configured");
        }
        return capturedResolver.resolve(policy.limitRef().orElseThrow(), keyGroup).orElse(null);
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
        if (result.recoveryPending() != null) {
            return new Evaluation(Decision.rejectedWithoutSchedule(level.policy().id(), level.policy().scope()),
                    level.keyGroup(), result.recoveryPending());
        }
        return rejection(level, result.remaining(), result.retryAfterMillis());
    }

    private static Evaluation rejection(PendingLevel level, long remaining, long retryAfterMillis) {
        if (retryAfterMillis == 0) return rejectedBeforeStore(level);
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
        private final BucketIdentity storageKey;
        private final String keyGroup;
        private final Limit limit;
        private final long resolverRevision;
        private final String resolverFingerprint;

        private PendingLevel(RateLimitPolicy policy, BucketIdentity storageKey, String keyGroup, Limit limit) {
            this(policy, storageKey, keyGroup, limit, null);
        }
        private PendingLevel(RateLimitPolicy policy, BucketIdentity storageKey, String keyGroup, Limit limit, LimitSnapshot snapshot) {
            resolverRevision = snapshot == null ? 0 : snapshot.revision();
            resolverFingerprint = snapshot == null ? LimitSnapshot.NONE_FINGERPRINT : snapshot.fingerprint();
            this.policy = policy;
            this.storageKey = storageKey;
            this.keyGroup = keyGroup;
            this.limit = limit;
        }

        static PendingLevel resolved(RateLimitPolicy policy, BucketIdentity storageKey, String keyGroup, Limit limit, LimitSnapshot snapshot) {
            return new PendingLevel(policy, storageKey, keyGroup, limit, snapshot);
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

        BucketIdentity storageKey() {
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
