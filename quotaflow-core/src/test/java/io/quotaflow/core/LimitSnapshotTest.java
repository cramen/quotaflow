package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LimitSnapshotTest {
    private static final Limit LIMIT = new Limit(10, 10, Duration.ofSeconds(1));
    private static final LimitSnapshot.Key KEY = new LimitSnapshot.Key("plan", "global");
    @Test void capturedSnapshotsAreImmutableAndResolutionNeverMixesPublishedViews() {
        var mutable = new HashMap<>(Map.of(KEY, LIMIT));
        var first = new LimitSnapshot(1, mutable);
        var published = new AtomicReference<Optional<LimitSnapshot>>(Optional.empty());
        VersionedLimitResolver provider = published::get;
        assertTrue(provider.resolve("plan", "global").isEmpty());
        published.set(Optional.of(first));
        var captured = provider.snapshot().orElseThrow();
        mutable.put(KEY, new Limit(2, 1, Duration.ofSeconds(1)));
        var second = new LimitSnapshot(2, mutable); published.set(Optional.of(second));
        assertEquals(Optional.of(LIMIT), captured.resolve("plan", "global"));
        assertEquals(Optional.of(mutable.get(KEY)), provider.resolve("plan", "global"));
        assertNotEquals(first.fingerprint(), second.fingerprint());
        assertTrue(first.resolve("missing", "global").isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> first.limits().put(KEY, mutable.get(KEY)));
        assertFalse(first.toString().contains("plan"));
    }
    @Test void contentDigestIgnoresMapOrderAndEquivalentNumericRepresentations() {
        var other = new LimitSnapshot.Key("other", "principal");
        var a = new LinkedHashMap<LimitSnapshot.Key, Limit>(); a.put(KEY, LIMIT); a.put(other, LIMIT);
        var b = new LinkedHashMap<LimitSnapshot.Key, Limit>(); b.put(other, LIMIT); b.put(KEY, new Limit(10, 20, Duration.ofSeconds(2)));
        assertEquals(new LimitSnapshot(1, a).fingerprint(), new LimitSnapshot(2, b).fingerprint());
        assertNotEquals(new LimitSnapshot(1, Map.of(new LimitSnapshot.Key("a", "bc"), LIMIT)).fingerprint(),
                new LimitSnapshot(1, Map.of(new LimitSnapshot.Key("ab", "c"), LIMIT)).fingerprint());
    }
    @Test void providerIdentityIsValidatedWithoutAllowingStaticMarkersAsDynamicProof() {
        for (long revision : new long[]{-1, 0, RecoverySession.MAX_COUNTER + 1})
            assertThrows(IllegalArgumentException.class, () -> new LimitSnapshot(revision, Map.of()));
        assertEquals(RecoverySession.MAX_COUNTER, new LimitSnapshot(RecoverySession.MAX_COUNTER, Map.of()).revision());
        assertThrows(IllegalArgumentException.class, () -> new LimitSnapshot.Key("", "global"));
        assertThrows(IllegalArgumentException.class, () -> new RecoveryConfiguration("a".repeat(64), 0, 1, LimitSnapshot.NONE_FINGERPRINT));
        for (String invalid : List.of("short", "A".repeat(64), "g".repeat(64), "/".repeat(64), ":".repeat(64)))
            assertThrows(IllegalArgumentException.class, () -> new RecoveryConfiguration("a".repeat(64), 0, 1, invalid));
        assertThrows(IllegalArgumentException.class, () -> new RecoveryConfiguration("invalid", 0));
        assertThrows(IllegalArgumentException.class, () -> new RecoveryConfiguration("a".repeat(64), -1));
        assertThrows(IllegalArgumentException.class, () -> new RecoveryConfiguration("a".repeat(64), RecoverySession.MAX_COUNTER + 1));
        for (long revision : new long[]{-1, 0, RecoverySession.MAX_COUNTER + 1})
            assertThrows(IllegalArgumentException.class, () -> RecoveryConfiguration.validateResolver(revision, "a".repeat(64)));
        var snapshot = new LimitSnapshot(1, Map.of(KEY, LIMIT));
        var session = new RecoverySession("initial", RecoveryCohort.single().digest(), "single", 0, 1, "owner");
        var context = new RecoveryContext(new QuotaDomain("default", "p"), session, 1, 1, 1,
                "a".repeat(64), RecoveryPhase.NORMAL, snapshot.revision(), snapshot.fingerprint());
        assertTrue(new RecoveryConfiguration("a".repeat(64), 9, snapshot.revision(), snapshot.fingerprint()).matches(context));
        assertFalse(new RecoveryConfiguration("a".repeat(64), 9).matches(context));
        assertFalse(new RecoveryConfiguration("b".repeat(64), 9, snapshot.revision(), snapshot.fingerprint()).matches(context));
        assertFalse(new RecoveryConfiguration("a".repeat(64), 9, snapshot.revision(), "f".repeat(64)).matches(context));
    }
    @Test void anAtomicChainCannotMixCapturedViews() {
        var domain = new QuotaDomain("n", "p");
        var a = new BucketIdentity(domain, "p", Scope.GLOBAL, "all");
        var b = new BucketIdentity(domain, "c", Scope.USER, "user");
        String fingerprint = new LimitSnapshot(1, Map.of(KEY, LIMIT)).fingerprint();
        var first = new LevelRequest(a, LIMIT, Algorithm.GCRA, 1, "global", "a".repeat(64), 1, fingerprint);
        var coherent = new LevelRequest(b, LIMIT, Algorithm.GCRA, 1, "principal", "a".repeat(64), 1, fingerprint);
        assertDoesNotThrow(() -> LevelRequest.validateChain(List.of(first, coherent)));
        assertThrows(IllegalArgumentException.class, () -> LevelRequest.validateChain(List.of(first,
                new LevelRequest(b, LIMIT, Algorithm.GCRA, 1, "principal", "a".repeat(64), 2, fingerprint))));
        assertThrows(IllegalArgumentException.class, () -> LevelRequest.validateChain(List.of(first,
                new LevelRequest(b, LIMIT, Algorithm.GCRA, 1, "principal", "a".repeat(64), 1, "f".repeat(64)))));
        assertThrows(IllegalArgumentException.class, () -> LevelRequest.validateChain(List.of(first,
                new LevelRequest(b, LIMIT, Algorithm.GCRA, 1, "principal", "b".repeat(64), 1, fingerprint))));
    }
}
