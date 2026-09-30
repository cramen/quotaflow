package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RecoveryPolicySourceTest {
    private static final Limit LIMIT = new Limit(10, 1, Duration.ofSeconds(1));
    private static RateLimitPolicy root(Limit limit) { return RateLimitPolicy.builder("root").scope(Scope.GLOBAL).limit(limit).build(); }
    private static RateLimitPolicy child(String reference) {
        return RateLimitPolicy.builder("child").scope(Scope.USER).parentId("root").limitRef(reference).build();
    }
    @Test void fingerprintsAreCanonicalAndCoverQuotaAffectingParameters() {
        var first = PolicySet.compile(List.of(root(LIMIT), child("plan")));
        var reordered = PolicySet.compile(List.of(child("plan"), root(LIMIT)));
        assertEquals(first.recoveryFingerprint("root"), reordered.recoveryFingerprint("root"));
        assertSame(first.recoveryFingerprint("root"), first.recoveryFingerprint("root"));
        assertNotEquals(first.recoveryFingerprint("root"), PolicySet.compile(List.of(root(new Limit(9, 1, Duration.ofSeconds(1))), child("plan"))).recoveryFingerprint("root"));
        assertNotEquals(first.recoveryFingerprint("root"), PolicySet.compile(List.of(root(LIMIT), child("other"))).recoveryFingerprint("root"));
        var routing = RateLimitPolicy.builder("root").scope(Scope.GLOBAL).limit(LIMIT).keyResolverId("custom").defaultKey("all").build();
        assertNotEquals(first.recoveryFingerprint("root"), PolicySet.compile(List.of(routing, child("plan"))).recoveryFingerprint("root"));
        var changedReaction = RateLimitPolicy.builder("root").scope(Scope.GLOBAL).limit(LIMIT).reaction(Reaction.THROTTLE).build();
        assertEquals(first.recoveryFingerprint("root"), PolicySet.compile(List.of(changedReaction, child("plan"))).recoveryFingerprint("root"));
    }
    @Test void resolvesCurrentStaticDynamicAndRetiredTargetsWithoutFabricatingTariffs() {
        var policies = PolicySet.compile(List.of(root(LIMIT), child("plan")));
        var seen = new AtomicReference<String>();
        var source = new RecoveryPolicySource(policies, "n", (reference, group) -> {
            seen.set(reference + ":" + group); return Optional.of(LIMIT);
        });
        var domain = new QuotaDomain("n", "root");
        assertEquals("n", source.namespace()); assertEquals(Set.of(domain), source.domains());
        assertEquals(policies.recoveryFingerprint("root"), source.fingerprint(domain));
        assertThrows(PolicyConfigurationException.class, () -> source.fingerprint(new QuotaDomain("n", "missing")));
        var previous = new Limit(100, 1, Duration.ofSeconds(1));
        assertEquals(Optional.of(LIMIT), source.resolve(new BucketIdentity(domain, "root", Scope.GLOBAL, "all"), "global", previous));
        var dynamic = new BucketIdentity(domain, "child", Scope.USER, "raw-key");
        assertEquals(Optional.of(LIMIT), source.resolve(dynamic, "principal", previous));
        assertEquals("plan:principal", seen.get());
        assertTrue(new RecoveryPolicySource(policies, "n", null).resolve(dynamic, "principal", previous).isEmpty());
        assertTrue(new RecoveryPolicySource(policies, "n", (r, g) -> Optional.empty()).resolve(dynamic, "principal", previous).isEmpty());
        assertEquals(Optional.of(previous), new RecoveryPolicySource(PolicySet.compile(List.of(root(LIMIT))), "n", null)
                .resolve(dynamic, "principal", previous));
    }
    @Test void capturedTargetsRetainOneViewAndRemovedDomainsHaveATombstoneFingerprint() {
        var policies = PolicySet.compile(List.of(root(LIMIT), child("plan")));
        var key = new LimitSnapshot.Key("plan", "principal");
        var first = new LimitSnapshot(1, Map.of(key, LIMIT));
        var published = new AtomicReference<Optional<LimitSnapshot>>(Optional.of(first));
        VersionedLimitResolver resolver = published::get;
        var source = new RecoveryPolicySource(policies, "n", resolver); source.validateCapabilities();
        var domain = new QuotaDomain("n", "root"); var bucket = new BucketIdentity(domain, "child", Scope.USER, "u");
        var target = source.capture(domain).orElseThrow();
        assertTrue(target.active(bucket)); assertEquals(1, target.resolverRevision()); assertEquals(first.fingerprint(), target.resolverFingerprint());
        assertTrue(target.sameAs(source.capture(domain).orElseThrow())); assertFalse(target.sameAs(null));
        published.set(Optional.of(new LimitSnapshot(2, Map.of(key, new Limit(1, 1, Duration.ofSeconds(1))))));
        assertFalse(target.sameAs(source.capture(domain).orElseThrow())); assertEquals(Optional.of(LIMIT), target.resolve(bucket, "principal", null));
        assertEquals(domain, target.domain()); assertEquals(5, target.configuration(5).localRevision());
        published.set(Optional.empty()); assertTrue(source.capture(domain).isEmpty());
        assertThrows(PolicyConfigurationException.class, () -> source.capture(new QuotaDomain("other", "root")));
        var staticSource = new RecoveryPolicySource(PolicySet.compile(List.of(root(LIMIT))), "n", null);
        var staticTarget = staticSource.capture(domain).orElseThrow();
        assertEquals(0, staticTarget.resolverRevision()); assertFalse(staticTarget.active(bucket));
        assertEquals(Optional.of(LIMIT), staticTarget.resolve(bucket, "principal", LIMIT));
        assertFalse(target.sameAs(staticTarget));
        var unrelated = PolicySet.compile(List.of(RateLimitPolicy.builder("other").scope(Scope.GLOBAL).limit(LIMIT).build()));
        var retired = new RecoveryPolicySource(unrelated, "n", null).capture(domain).orElseThrow();
        assertFalse(retired.active(bucket)); assertEquals(0, retired.resolverRevision());
        assertNotEquals(staticTarget.fingerprint(), retired.fingerprint());
        assertEquals(retired.fingerprint(), new RecoveryPolicySource(unrelated, "n", null).capture(domain).orElseThrow().fingerprint());
        assertThrows(PolicyConfigurationException.class, () -> target.resolve(new BucketIdentity(new QuotaDomain("n", "other"), "p", Scope.USER, "u"), "principal", LIMIT));
        assertThrows(PolicyConfigurationException.class, () -> new RecoveryPolicySource(policies, "n", (r, g) -> Optional.of(LIMIT)).validateCapabilities());
    }
}
