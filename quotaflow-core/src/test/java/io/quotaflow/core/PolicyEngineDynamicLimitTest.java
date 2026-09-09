package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.RateLimitStore;
import io.quotaflow.core.store.StoreResult;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Engine behavior for policies declaring a dynamic {@code limitRef}: the
 * effective limit is resolved before store evaluation on every path, an
 * unresolvable reference rejects, and static policies never touch the
 * resolver.
 */
class PolicyEngineDynamicLimitTest {

    /** Per-level store over a deterministic local store (no batch capability). */
    private static final class PerLevelStore implements RateLimitStore {
        private final LocalRateLimitStore delegate;
        private final AtomicInteger calls = new AtomicInteger();

        PerLevelStore(LocalRateLimitStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public StoreResult tryAcquire(String storageKey, Limit limit, Algorithm algorithm, long weight) {
            calls.incrementAndGet();
            return delegate.tryAcquire(storageKey, limit, algorithm, weight);
        }

        @Override
        public CompletionStage<StoreResult> tryAcquireAsync(
                String storageKey, Limit limit, Algorithm algorithm, long weight) {
            return CompletableFuture.completedFuture(tryAcquire(storageKey, limit, algorithm, weight));
        }
    }

    private static final Limit RESOLVED = new Limit(2, 1, Duration.ofHours(1));

    private final AtomicLong nanos = new AtomicLong();
    private final LocalRateLimitStore localStore = new LocalRateLimitStore(nanos::get);
    private final AtomicReference<Optional<Limit>> resolved = new AtomicReference<>(Optional.of(RESOLVED));
    private final AtomicInteger resolverCalls = new AtomicInteger();
    private final AtomicReference<String> seenRef = new AtomicReference<>();
    private final AtomicReference<String> seenKeyGroup = new AtomicReference<>();

    private final LimitResolver resolver = (limitRef, keyGroup) -> {
        resolverCalls.incrementAndGet();
        seenRef.set(limitRef);
        seenKeyGroup.set(keyGroup);
        return resolved.get();
    };

    private final PolicyEngine batchEngine =
            new PolicyEngine(localStore, KeyResolvers.scopeBased(), java.util.Map.of(), resolver);

    private static RateLimitPolicy dynamicPolicy(String id, Scope scope, String parentId) {
        return RateLimitPolicy.builder(id)
                .limitRef("tariff")
                .scope(scope)
                .parentId(parentId)
                .build();
    }

    private static RateLimitContext userContext(String principal) {
        return RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, principal).build();
    }

    @Test
    void resolvedLimitGovernsDecisionsOnBatchPath() {
        PolicySet set = PolicySet.compile(List.of(dynamicPolicy("u", Scope.USER, null)));
        RateLimitContext context = userContext("alice");
        assertTrue(batchEngine.evaluate(set, "u", context, 1).isAllowed());
        assertTrue(batchEngine.evaluate(set, "u", context, 1).isAllowed());
        Decision third = batchEngine.evaluate(set, "u", context, 1);
        assertFalse(third.isAllowed());
        assertEquals("u", third.policyId());
        assertEquals("tariff", seenRef.get());
        assertEquals("principal", seenKeyGroup.get());
    }

    @Test
    void resolvedLimitGovernsDecisionsOnPerLevelPath() {
        PerLevelStore store = new PerLevelStore(localStore);
        PolicyEngine engine = new PolicyEngine(store, KeyResolvers.scopeBased(), java.util.Map.of(), resolver);
        PolicySet set = PolicySet.compile(List.of(dynamicPolicy("u", Scope.USER, null)));
        RateLimitContext context = userContext("alice");
        assertTrue(engine.evaluate(set, "u", context, 1).isAllowed());
        assertTrue(engine.evaluate(set, "u", context, 1).isAllowed());
        assertFalse(engine.evaluate(set, "u", context, 1).isAllowed());
        assertEquals(3, store.calls.get());
    }

    @Test
    void resolvedLimitFlowsThroughChain() {
        PolicySet set = PolicySet.compile(List.of(
                dynamicPolicy("g", Scope.GLOBAL, null),
                RateLimitPolicy.builder("u")
                        .limit(new Limit(100, 1, Duration.ofHours(1)))
                        .scope(Scope.USER)
                        .parentId("g")
                        .build()));
        RateLimitContext context = userContext("alice");
        assertTrue(batchEngine.evaluate(set, "u", context, 1).isAllowed());
        assertTrue(batchEngine.evaluate(set, "u", context, 1).isAllowed());
        Decision third = batchEngine.evaluate(set, "u", context, 1);
        assertFalse(third.isAllowed());
        assertEquals("g", third.policyId(), "resolved parent limit fired, not the static child limit");
        assertEquals(Scope.GLOBAL, third.scope());
    }

    @Test
    void asyncPathResolvesLimitBeforeStoreEvaluation() {
        PolicySet set = PolicySet.compile(List.of(dynamicPolicy("u", Scope.USER, null)));
        RateLimitContext context = userContext("alice");
        assertTrue(batchEngine.evaluateAsync(set, "u", context, 1).toCompletableFuture().join().isAllowed());
        assertTrue(batchEngine.evaluateAsync(set, "u", context, 1).toCompletableFuture().join().isAllowed());
        Decision third = batchEngine.evaluateAsync(set, "u", context, 1).toCompletableFuture().join();
        assertFalse(third.isAllowed());
    }

    @Test
    void unresolvableReferenceRejectsWithoutRetrySchedule() {
        resolved.set(Optional.empty());
        PolicySet set = PolicySet.compile(List.of(dynamicPolicy("u", Scope.USER, null)));
        Decision decision = batchEngine.evaluate(set, "u", userContext("alice"), 1);
        assertFalse(decision.isAllowed());
        assertEquals("u", decision.policyId());
        assertTrue(decision.retryAfter().isEmpty());
        assertEquals(0, localStore.cellCount(), "unresolvable limit must not touch the store");
    }

    @Test
    void unresolvableReferenceRejectsOnAsyncPath() {
        resolved.set(Optional.empty());
        PolicySet set = PolicySet.compile(List.of(dynamicPolicy("u", Scope.USER, null)));
        Decision decision = batchEngine.evaluateAsync(set, "u", userContext("alice"), 1)
                .toCompletableFuture().join();
        assertFalse(decision.isAllowed());
        assertTrue(decision.retryAfter().isEmpty());
    }

    @Test
    void unresolvableReferenceAtParentLevelIdentifiesThatLevel() {
        PolicySet set = PolicySet.compile(List.of(
                dynamicPolicy("g", Scope.GLOBAL, null),
                RateLimitPolicy.builder("u")
                        .limit(new Limit(100, 1, Duration.ofHours(1)))
                        .scope(Scope.USER)
                        .parentId("g")
                        .build()));
        resolved.set(Optional.empty());
        Evaluation evaluation = batchEngine.evaluateInternal(set, "u", userContext("alice"), 1);
        assertFalse(evaluation.decision().isAllowed());
        assertEquals("g", evaluation.decision().policyId());
        assertEquals("global", evaluation.keyGroup());
    }

    @Test
    void staticPoliciesNeverInvokeTheResolver() {
        PolicySet set = PolicySet.compile(List.of(RateLimitPolicy.builder("u")
                .limit(new Limit(5, 1, Duration.ofHours(1)))
                .scope(Scope.USER)
                .build()));
        Decision decision = batchEngine.evaluate(set, "u", userContext("alice"), 1);
        assertTrue(decision.isAllowed());
        assertEquals(0, resolverCalls.get());
    }

    @Test
    void limitRefWithoutConfiguredResolverFailsEvaluationNamingPolicy() {
        PolicyEngine noResolver = new PolicyEngine(localStore);
        PolicySet set = PolicySet.compile(List.of(dynamicPolicy("u", Scope.USER, null)));
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> noResolver.evaluate(set, "u", userContext("alice"), 1));
        assertTrue(e.getMessage().contains("'u'"));
        assertTrue(e.getMessage().contains("tariff"));
    }
}
