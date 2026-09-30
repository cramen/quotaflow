package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class RecoveryPendingEngineTest {
    private static final PolicySet POLICIES = PolicySet.compile(List.of(RateLimitPolicy.builder("p").scope(Scope.GLOBAL)
            .reaction(Reaction.THROTTLE).limit(new Limit(1, 1, Duration.ofSeconds(1))).build()));
    private static class PendingStore implements RateLimitStore {
        final AtomicReference<RecoveryPending> pending = new AtomicReference<>(new RecoveryPending(
                new QuotaDomain("default", "p"), 1, new CompletableFuture<>()));
        @Override public CompletionStage<Void> registerPolicies(List<PolicyBinding> bindings) { return CompletableFuture.completedFuture(null); }
        @Override public StoreResult tryAcquire(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) { return StoreResult.pending(pending.get()); }
        @Override public CompletionStage<StoreResult> tryAcquireAsync(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) {
            return CompletableFuture.completedFuture(tryAcquire(key, limit, algorithm, weight));
        }
    }
    private static final class PendingBatch extends PendingStore implements BatchRateLimitStore, RecoveryConfigurationAware {
        PolicySet configured; String fingerprint;
        @Override public CompletionStage<Void> configureRecovery(PolicySet policies, String namespace, LimitResolver resolver) {
            configured = policies; return CompletableFuture.completedFuture(null);
        }
        @Override public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
            fingerprint = chain.get(0).configurationFingerprint();
            assertEquals("global", chain.get(0).keyGroup());
            return CompletableFuture.completedFuture(ChainResult.pending(0, pending.get()));
        }
    }
    @Test void enginePreservesReadinessForSingleAndAtomicPathsWithoutInventingRefill() {
        for (PendingStore store : List.of(new PendingStore(), new PendingBatch())) {
            var engine = new PolicyEngine(store); engine.registerPolicies(POLICIES).toCompletableFuture().join();
            Evaluation sync = engine.evaluateInternal(POLICIES, "p", RateLimitContext.empty(), 1);
            Evaluation async = engine.evaluateInternalAsync(POLICIES, "p", RateLimitContext.empty(), 1).toCompletableFuture().join();
            for (Evaluation evaluation : List.of(sync, async)) {
                assertSame(store.pending.get(), evaluation.recoveryPending());
                assertFalse(evaluation.decision().isAllowed());
                assertTrue(evaluation.decision().retryAfter().isEmpty()); assertEquals(0, evaluation.decision().remaining());
            }
            if (store instanceof PendingBatch batch) {
                assertSame(POLICIES, batch.configured);
                assertEquals(POLICIES.recoveryFingerprint("p"), batch.fingerprint);
            }
        }
    }
    @Test void facadeCurrentlyReturnsAnImmediateSchedulelessDecisionAndOneNotification() {
        var store = new PendingBatch(); var notifications = new AtomicInteger();
        var flow = DefaultQuotaFlow.builder(POLICIES, store).addListener((decision, group) -> notifications.incrementAndGet()).build();
        var decision = flow.acquire("p", RateLimitContext.empty(), 1, Duration.ofSeconds(1));
        assertFalse(decision.isAllowed()); assertTrue(decision.retryAfter().isEmpty());
        assertEquals(Duration.ZERO, decision.waitDuration()); assertEquals(1, notifications.get());
        assertFalse(store.pending.get().readiness().toCompletableFuture().isDone());
    }
}
