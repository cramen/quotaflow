package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class VersionedPreflightTest {
    private static Limit limit(long capacity) { return new Limit(capacity, 1, Duration.ofSeconds(1)); }
    private static final class RecordingStore implements RecoveryConfigurationAware {
        int registrations, calls; List<LevelRequest> last;
        public CompletionStage<Void> registerPolicies(List<PolicyBinding> bindings) { registrations++; return CompletableFuture.completedFuture(null); }
        public CompletionStage<Void> configureRecovery(PolicySet policies, String namespace, LimitResolver resolver) { return CompletableFuture.completedFuture(null); }
        public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
            last = List.copyOf(chain); calls++; return CompletableFuture.completedFuture(ChainResult.acquired(chain.size() - 1, 0));
        }
        public StoreResult tryAcquire(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) { throw new AssertionError("batch expected"); }
        public CompletionStage<StoreResult> tryAcquireAsync(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) { throw new AssertionError("batch expected"); }
    }
    @Test void providerPublicationDuringKeyResolutionCannotMixTheChainView() {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("p").scope(Scope.GLOBAL).limitRef("parent").build(),
                RateLimitPolicy.builder("c").scope(Scope.USER).parentId("p").limitRef("child").build()));
        var parent = new LimitSnapshot.Key("parent", "global"); var child = new LimitSnapshot.Key("child", "principal");
        var first = new LimitSnapshot(1, Map.of(parent, limit(10), child, limit(10)));
        var second = new LimitSnapshot(2, Map.of(parent, limit(1), child, limit(1)));
        var published = new AtomicReference<Optional<LimitSnapshot>>(Optional.of(first)); var captures = new AtomicInteger();
        VersionedLimitResolver provider = () -> { captures.incrementAndGet(); return published.get(); };
        KeyResolver switching = (context, policy) -> { published.set(Optional.of(second)); return KeyResolvers.scopeBased().resolve(context, policy); };
        var store = new RecordingStore(); var engine = new PolicyEngine(store, switching, Map.of(), provider);
        engine.registerPolicies(policies).toCompletableFuture().join();
        var user = RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "user").build();
        assertTrue(engine.evaluate(policies, "c", user, 1).isAllowed());
        assertEquals(1, captures.get());
        for (var request : store.last) { assertEquals(1, request.resolverRevision()); assertEquals(first.fingerprint(), request.resolverFingerprint()); assertEquals(limit(10), request.limit()); }
        engine.evaluateAsync(policies, "c", user, 1).toCompletableFuture().join();
        for (var request : store.last) { assertEquals(2, request.resolverRevision()); assertEquals(limit(1), request.limit()); }
        published.set(Optional.empty()); int before = store.calls;
        assertFalse(engine.evaluate(policies, "c", user, 1).isAllowed()); assertEquals(before, store.calls);
    }
    @Test void staticAncestorsShareDynamicDomainIdentityButIndependentStaticTreesDoNot() {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("p").scope(Scope.GLOBAL).limit(limit(10)).build(),
                RateLimitPolicy.builder("c").scope(Scope.USER).parentId("p").limitRef("child").build(),
                RateLimitPolicy.builder("other").scope(Scope.GLOBAL).limit(limit(10)).build()));
        var snapshot = new LimitSnapshot(1, Map.of(new LimitSnapshot.Key("child", "principal"), limit(10)));
        var captures = new AtomicInteger(); VersionedLimitResolver provider = () -> { captures.incrementAndGet(); return Optional.of(snapshot); };
        var store = new RecordingStore(); var engine = new PolicyEngine(store, KeyResolvers.scopeBased(), Map.of(), provider);
        engine.evaluate(policies, "p", RateLimitContext.empty(), 1);
        assertEquals(1, store.last.get(0).resolverRevision());
        engine.evaluate(policies, "other", RateLimitContext.empty(), 1);
        assertEquals(0, store.last.get(0).resolverRevision()); assertEquals(1, captures.get());
    }
    @Test void unversionedResolversCannotPublishCoordinatedBindingsButRemainValidLocally() {
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("p").scope(Scope.GLOBAL).limitRef("plan").build()));
        LimitResolver legacy = (reference, group) -> Optional.of(limit(10)); var store = new RecordingStore();
        var engine = new PolicyEngine(store, KeyResolvers.scopeBased(), Map.of(), legacy);
        assertThrows(PolicyConfigurationException.class, () -> engine.registerPolicies(policies)); assertEquals(0, store.registrations);
        var local = DefaultQuotaFlow.builder(policies, new LocalRateLimitStore()).limitResolver(legacy).build();
        assertTrue(local.tryAcquire("p", RateLimitContext.empty()).isAllowed());
    }
}
