package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;

import io.quotaflow.core.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/** Compilable source migration example for a custom store/decorator. */
class CustomStoreMigrationTest {
    static final class CustomStore implements BatchRateLimitStore {
        private final LocalRateLimitStore delegate = new LocalRateLimitStore();
        public CompletionStage<Void> registerPolicies(List<PolicyBinding> policies) {
            return delegate.registerPolicies(policies);
        }
        public StoreResult tryAcquire(BucketIdentity identity, Limit limit, Algorithm algorithm, long weight) {
            return delegate.tryAcquire(identity, limit, algorithm, weight);
        }
        public CompletionStage<StoreResult> tryAcquireAsync(BucketIdentity identity, Limit limit, Algorithm algorithm, long weight) {
            return delegate.tryAcquireAsync(identity, limit, algorithm, weight);
        }
        public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
            return delegate.tryAcquireAll(chain);
        }
    }

    @Test
    void sourceMigratedDecoratorKeepsSharedAncestorAndRejectsInvalidBatches() {
        var parent = RateLimitPolicy.builder("provider").scope(Scope.GLOBAL)
                .limit(new Limit(1, 1, Duration.ofHours(1))).build();
        var child = RateLimitPolicy.builder("user").scope(Scope.USER).parentId("provider")
                .limit(new Limit(10, 1, Duration.ofHours(1))).build();
        var custom = new CustomStore();
        var flow = DefaultQuotaFlow.builder(PolicySet.compile(List.of(parent, child)), custom).namespace("orders").build();
        assertTrue(flow.tryAcquire("user", RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "alice").build()).isAllowed());
        assertFalse(flow.tryAcquire("user", RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "bob").build()).isAllowed());
        assertFalse(flow.tryAcquire("provider", RateLimitContext.empty()).isAllowed());
        var identity = new BucketIdentity(new QuotaDomain("orders", "provider"), "provider", Scope.GLOBAL, "global");
        assertEquals(identity.domain(), custom.delegate.snapshot().get(0).storageKey().domain());
        var one = new LevelRequest(identity, parent.limit().orElseThrow(), Algorithm.TOKEN_BUCKET, 1);
        assertThrows(IllegalArgumentException.class, () -> custom.tryAcquireAll(List.of(one, one)));
        var foreign = new LevelRequest(new BucketIdentity(new QuotaDomain("elsewhere", "provider"),
                "provider", Scope.GLOBAL, "global"), one.limit(), one.algorithm(), 1);
        assertThrows(IllegalArgumentException.class, () -> custom.tryAcquireAll(List.of(one, foreign)));
        assertThrows(NullPointerException.class, () -> LevelRequest.validateChain(null));
        assertThrows(IllegalArgumentException.class, () -> LevelRequest.validateChain(List.of()));
    }
}
