package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class PolicyEngineTest {

    private final AtomicLong nanos = new AtomicLong();
    private final LocalRateLimitStore store = new LocalRateLimitStore(nanos::get);
    private final PolicyEngine engine = new PolicyEngine(store);

    private static RateLimitPolicy policy(String id, Scope scope, long capacity, String parentId) {
        return RateLimitPolicy.builder(id)
                .limit(new Limit(capacity, 1, Duration.ofMillis(1)))
                .scope(scope)
                .parentId(parentId)
                .build();
    }

    /** Chain: global(10) <- tenant(5) <- user(2). */
    private PolicySet threeLevelSet() {
        return PolicySet.compile(List.of(
                policy("g", Scope.GLOBAL, 10, null),
                policy("t", Scope.TENANT, 5, "g"),
                policy("u", Scope.USER, 2, "t")));
    }

    private static RateLimitContext context(String tenant, String principal) {
        RateLimitContext.Builder builder = RateLimitContext.builder();
        if (tenant != null) {
            builder.put(RateLimitContext.TENANT_ID, tenant);
        }
        if (principal != null) {
            builder.put(RateLimitContext.PRINCIPAL, principal);
        }
        return builder.build();
    }

    @Test
    void allowReportsLeafAsFiredLevelAndMinRemainingAcrossChain() {
        Decision decision = engine.evaluate(threeLevelSet(), "u", context("acme", "alice"), 1);
        assertTrue(decision.isAllowed());
        assertEquals(Verdict.ALLOWED, decision.verdict());
        assertEquals("u", decision.policyId());
        assertEquals(Scope.USER, decision.scope());
        // global 9, tenant 4, user 1 -> minimum is the user level
        assertEquals(1, decision.remaining());
        assertTrue(decision.retryAfter().isEmpty());
    }

    @Test
    void rejectionAtDeepestLevelIdentifiesThatLevelWithRetryAfter() {
        PolicySet set = threeLevelSet();
        engine.evaluate(set, "u", context("acme", "alice"), 1);
        engine.evaluate(set, "u", context("acme", "alice"), 1);
        Decision decision = engine.evaluate(set, "u", context("acme", "alice"), 1);
        assertFalse(decision.isAllowed());
        assertEquals("u", decision.policyId());
        assertEquals(Scope.USER, decision.scope());
        assertEquals(0, decision.remaining());
        assertTrue(decision.retryAfter().orElseThrow().toMillis() > 0);
    }

    @Test
    void rejectionShortCircuitsDeeperLevels() {
        PolicySet set = PolicySet.compile(List.of(
                policy("u", Scope.USER, 1, null),
                policy("k", Scope.KEY, 100, "u")));
        RateLimitContext context = RateLimitContext.builder()
                .put(RateLimitContext.PRINCIPAL, "alice")
                .put(RateLimitContext.API_KEY, "ak-1")
                .build();
        engine.evaluate(set, "k", context, 1);
        Decision rejected = engine.evaluate(set, "k", context, 1);
        assertEquals("u", rejected.policyId());
        // short-circuit: the rejection never touched the store at the key level;
        // only the cells created by the first (fully allowed) request exist
        assertEquals(2, store.cellCount());
    }

    @Test
    void externalProviderQuotaSharedAcrossTenants() {
        PolicySet set = PolicySet.compile(List.of(
                policy("provider", Scope.GLOBAL, 3, null),
                policy("tenant-a", Scope.TENANT, 100, "provider"),
                policy("tenant-b", Scope.TENANT, 100, "provider")));
        engine.evaluate(set, "tenant-a", context("a", null), 1);
        engine.evaluate(set, "tenant-b", context("b", null), 1);
        engine.evaluate(set, "tenant-a", context("a", null), 1);
        Decision decision = engine.evaluate(set, "tenant-b", context("b", null), 1);
        assertFalse(decision.isAllowed());
        assertEquals("provider", decision.policyId());
        assertEquals(Scope.GLOBAL, decision.scope());
    }

    @Test
    void weightedAcquisitionConsumesWeightAndReportsReducedRemaining() {
        PolicySet set = threeLevelSet();
        Decision decision = engine.evaluate(set, "u", context("acme", "alice"), 2);
        assertTrue(decision.isAllowed());
        assertEquals(0, decision.remaining());
    }

    @Test
    void insufficientCapacityForWeightRejectsWithoutConsumingAtThatLevel() {
        PolicySet set = PolicySet.compile(List.of(
                policy("t", Scope.TENANT, 10, null),
                policy("u", Scope.USER, 2, "t")));
        // weight 3 fits tenant but not user (capacity 2)
        Decision rejected = engine.evaluate(set, "u", context("acme", "alice"), 3);
        assertFalse(rejected.isAllowed());
        assertEquals("u", rejected.policyId());
        // user bucket was untouched: a fresh tenant context can still spend the full user capacity
        Decision allowed = engine.evaluate(set, "u", context("other", "alice"), 2);
        assertTrue(allowed.isAllowed());
        assertEquals(0, allowed.remaining());
    }

    @Test
    void parentExhaustionRejectsEvenWhenLeafHasCapacity() {
        PolicySet set = threeLevelSet();
        for (int i = 0; i < 5; i++) {
            engine.evaluate(set, "u", context("acme", "user-" + i), 1);
        }
        Decision decision = engine.evaluate(set, "u", context("acme", "user-5"), 1);
        assertFalse(decision.isAllowed());
        assertEquals("t", decision.policyId());
        assertEquals(Scope.TENANT, decision.scope());
    }

    @Test
    void unknownPolicyIdThrowsConfigurationException() {
        PolicySet set = threeLevelSet();
        assertThrows(PolicyConfigurationException.class,
                () -> engine.evaluate(set, "nope", context("acme", "alice"), 1));
    }

    @Test
    void invalidWeightThrows() {
        PolicySet set = threeLevelSet();
        assertThrows(IllegalArgumentException.class,
                () -> engine.evaluate(set, "u", context("acme", "alice"), 0));
    }

    @Test
    void nullContextThrows() {
        PolicySet set = threeLevelSet();
        assertThrows(NullPointerException.class,
                () -> engine.evaluate(set, "u", null, 1));
    }

    @Test
    void unresolvableKeyRejectsByDefaultWithoutRetrySchedule() {
        PolicySet set = threeLevelSet();
        Decision decision = engine.evaluate(set, "u", context("acme", null), 1);
        assertFalse(decision.isAllowed());
        assertEquals("u", decision.policyId());
        assertTrue(decision.retryAfter().isEmpty());
        assertEquals(0, decision.remaining());
        // rejection happened before any store access at the user level
        assertEquals(2, store.cellCount());
    }

    @Test
    void configuredDefaultKeySharesOneBucket() {
        PolicySet set = PolicySet.compile(List.of(
                RateLimitPolicy.builder("u")
                        .limit(new Limit(2, 1, Duration.ofMillis(1)))
                        .scope(Scope.USER)
                        .defaultKey("anonymous")
                        .build()));
        RateLimitContext empty = RateLimitContext.empty();
        assertTrue(engine.evaluate(set, "u", empty, 1).isAllowed());
        assertTrue(engine.evaluate(set, "u", empty, 1).isAllowed());
        assertFalse(engine.evaluate(set, "u", empty, 1).isAllowed());
        Evaluation evaluation = engine.evaluateInternal(set, "u", empty, 1);
        assertEquals("default", evaluation.keyGroup());
    }

    @Test
    void namedResolverDerivesKeysFromCustomDomainState() {
        PolicySet set = PolicySet.compile(List.of(
                RateLimitPolicy.builder("u")
                        .limit(new Limit(1, 1, Duration.ofMillis(1)))
                        .scope(Scope.USER)
                        .keyResolverId("billing")
                        .build()));
        PolicyEngine custom = new PolicyEngine(store, KeyResolvers.scopeBased(), Map.of(
                "billing", (context, policy) -> context.get("billing.account", String.class)
                        .map(account -> new LimitKey(account, "billing"))));
        RateLimitContext first = RateLimitContext.builder()
                .put("billing.account", "acct-1").put(RateLimitContext.PRINCIPAL, "alice").build();
        RateLimitContext second = RateLimitContext.builder()
                .put("billing.account", "acct-1").put(RateLimitContext.PRINCIPAL, "bob").build();
        Evaluation evaluation = custom.evaluateInternal(set, "u", first, 1);
        assertTrue(evaluation.decision().isAllowed());
        assertEquals("billing", evaluation.keyGroup());
        // different principal, same billing account -> same bucket
        assertFalse(custom.evaluate(set, "u", second, 1).isAllowed());
    }

    @Test
    void unknownNamedResolverThrowsConfigurationException() {
        PolicySet set = PolicySet.compile(List.of(
                RateLimitPolicy.builder("u")
                        .limit(new Limit(1, 1, Duration.ofMillis(1)))
                        .scope(Scope.USER)
                        .keyResolverId("ghost")
                        .build()));
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> engine.evaluate(set, "u", context(null, "alice"), 1));
        assertTrue(e.getMessage().contains("ghost"));
    }

    @Test
    void asyncEvaluationMatchesSyncSemantics() {
        PolicySet set = threeLevelSet();
        Decision allowed = engine.evaluateAsync(set, "u", context("acme", "alice"), 2)
                .toCompletableFuture().join();
        assertTrue(allowed.isAllowed());
        assertEquals(0, allowed.remaining());
        Decision rejected = engine.evaluateAsync(set, "u", context("acme", "alice"), 1)
                .toCompletableFuture().join();
        assertFalse(rejected.isAllowed());
        assertEquals("u", rejected.policyId());
    }

    @Test
    void asyncEvaluationSurfacesMissingKeyRejection() {
        PolicySet set = threeLevelSet();
        Decision decision = engine.evaluateAsync(set, "u", context("acme", null), 1)
                .toCompletableFuture().join();
        assertFalse(decision.isAllowed());
        assertTrue(decision.retryAfter().isEmpty());
    }

    @Test
    void asyncPreflightFailuresBecomeFailedFutures() {
        PolicySet set = threeLevelSet();
        CompletionException weightError = assertThrows(CompletionException.class,
                () -> engine.evaluateAsync(set, "u", context("acme", "alice"), 0)
                        .toCompletableFuture().join());
        assertTrue(weightError.getCause() instanceof IllegalArgumentException);
        CompletionException unknownPolicy = assertThrows(CompletionException.class,
                () -> engine.evaluateAsync(set, "nope", context("acme", "alice"), 1)
                        .toCompletableFuture().join());
        assertTrue(unknownPolicy.getCause() instanceof PolicyConfigurationException);
    }

    @Test
    void constructorRejectsNulls() {
        assertThrows(NullPointerException.class, () -> new PolicyEngine(null));
        assertThrows(NullPointerException.class,
                () -> new PolicyEngine(store, null, Map.of()));
        assertThrows(NullPointerException.class,
                () -> new PolicyEngine(store, KeyResolvers.scopeBased(), null));
    }
}
