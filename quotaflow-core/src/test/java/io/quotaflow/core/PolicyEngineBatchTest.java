package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.store.BatchRateLimitStore;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.RateLimitStore;
import io.quotaflow.core.store.StoreResult;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Engine behavior when the store implements {@link BatchRateLimitStore}: the
 * batch path is preferred, decisions map identically to the per-level path,
 * and a trailing missing-key level never reaches the store.
 */
class PolicyEngineBatchTest {

    /**
     * Batch-capable store over a deterministic local store. Not atomic across
     * levels (parents consume before a child rejects) — sufficient here
     * because these tests assert the engine's mapping, not store atomicity.
     */
    private static final class FakeBatchStore implements BatchRateLimitStore {
        private final LocalRateLimitStore delegate;
        private final AtomicInteger batchCalls = new AtomicInteger();
        private final AtomicInteger perLevelCalls = new AtomicInteger();
        private final AtomicInteger lastBatchSize = new AtomicInteger();

        FakeBatchStore(LocalRateLimitStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
            batchCalls.incrementAndGet();
            lastBatchSize.set(chain.size());
            long minRemaining = Long.MAX_VALUE;
            for (int i = 0; i < chain.size(); i++) {
                LevelRequest level = chain.get(i);
                StoreResult result =
                        delegate.tryAcquire(level.storageKey(), level.limit(), level.algorithm(), level.weight());
                if (!result.acquired()) {
                    return CompletableFuture.completedFuture(
                            ChainResult.rejected(i, result.remaining(), result.retryAfterMillis()));
                }
                minRemaining = Math.min(minRemaining, result.remaining());
            }
            return CompletableFuture.completedFuture(
                    ChainResult.acquired(chain.size() - 1, minRemaining));
        }

        @Override
        public StoreResult tryAcquire(String storageKey, Limit limit, Algorithm algorithm, long weight) {
            perLevelCalls.incrementAndGet();
            return delegate.tryAcquire(storageKey, limit, algorithm, weight);
        }

        @Override
        public CompletionStage<StoreResult> tryAcquireAsync(
                String storageKey, Limit limit, Algorithm algorithm, long weight) {
            return CompletableFuture.completedFuture(tryAcquire(storageKey, limit, algorithm, weight));
        }
    }

    private final AtomicLong nanos = new AtomicLong();
    private final FakeBatchStore store = new FakeBatchStore(new LocalRateLimitStore(nanos::get));
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
    void batchStoreIsPreferredAndMapsAllowedDecision() {
        Decision decision = engine.evaluate(threeLevelSet(), "u", context("acme", "alice"), 1);
        assertTrue(decision.isAllowed());
        assertEquals("u", decision.policyId());
        assertEquals(Scope.USER, decision.scope());
        // global 9, tenant 4, user 1 -> minimum is the user level
        assertEquals(1, decision.remaining());
        assertTrue(decision.retryAfter().isEmpty());
        assertEquals(1, store.batchCalls.get());
        assertEquals(3, store.lastBatchSize.get());
        assertEquals(0, store.perLevelCalls.get());
    }

    @Test
    void batchRejectionMapsFiredLevelRemainingAndRetryAfter() {
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
    void missingKeyAtRootSkipsTheStoreEntirely() {
        PolicySet set = PolicySet.compile(List.of(policy("t", Scope.TENANT, 5, null)));
        Decision decision = engine.evaluate(set, "t", RateLimitContext.empty(), 1);
        assertFalse(decision.isAllowed());
        assertTrue(decision.retryAfter().isEmpty());
        assertEquals(0, store.batchCalls.get());
        assertEquals(0, store.perLevelCalls.get());
    }

    @Test
    void trailingMissingKeyRejectsAfterBatchAllowanceWithoutAStoreCallForIt() {
        PolicySet set = PolicySet.compile(List.of(
                policy("t", Scope.TENANT, 5, null),
                policy("u", Scope.USER, 2, "t")));
        // tenant key resolves, user key does not: batch evaluates only the tenant level
        Decision decision = engine.evaluate(set, "u", context("acme", null), 1);
        assertFalse(decision.isAllowed());
        assertEquals("u", decision.policyId());
        assertTrue(decision.retryAfter().isEmpty());
        assertEquals(1, store.batchCalls.get());
        assertEquals(1, store.lastBatchSize.get());
    }

    @Test
    void asyncBatchPathMatchesSyncSemantics() {
        PolicySet set = threeLevelSet();
        Decision allowed = engine.evaluateAsync(set, "u", context("acme", "alice"), 2)
                .toCompletableFuture().join();
        assertTrue(allowed.isAllowed());
        assertEquals(0, allowed.remaining());
        assertEquals(1, store.batchCalls.get());
        Decision rejected = engine.evaluateAsync(set, "u", context("acme", "alice"), 1)
                .toCompletableFuture().join();
        assertFalse(rejected.isAllowed());
        assertEquals("u", rejected.policyId());
    }

    @Test
    void asyncMissingKeyAtRootSkipsTheStore() {
        PolicySet set = PolicySet.compile(List.of(policy("t", Scope.TENANT, 5, null)));
        Decision decision = engine.evaluateAsync(set, "t", RateLimitContext.empty(), 1)
                .toCompletableFuture().join();
        assertFalse(decision.isAllowed());
        assertEquals(0, store.batchCalls.get());
    }

    @Test
    void plainStoreStillUsesPerLevelEvaluation() {
        CountingStore plain = new CountingStore(new LocalRateLimitStore(nanos::get));
        PolicyEngine perLevelEngine = new PolicyEngine(plain);
        Decision decision = perLevelEngine.evaluate(threeLevelSet(), "u", context("acme", "alice"), 1);
        assertTrue(decision.isAllowed());
        assertEquals(3, plain.calls.get());
    }

    private static final class CountingStore implements RateLimitStore {
        private final RateLimitStore delegate;
        private final AtomicInteger calls = new AtomicInteger();

        CountingStore(RateLimitStore delegate) {
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
            calls.incrementAndGet();
            return delegate.tryAcquireAsync(storageKey, limit, algorithm, weight);
        }
    }

    @Test
    void levelRequestValidation() {
        assertThrows(NullPointerException.class,
                () -> new LevelRequest(null, new Limit(1, 1, Duration.ofSeconds(1)), Algorithm.GCRA, 1));
        assertThrows(NullPointerException.class,
                () -> new LevelRequest("k", null, Algorithm.GCRA, 1));
        assertThrows(NullPointerException.class,
                () -> new LevelRequest("k", new Limit(1, 1, Duration.ofSeconds(1)), null, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new LevelRequest("k", new Limit(1, 1, Duration.ofSeconds(1)), Algorithm.GCRA, 0));
    }
}
