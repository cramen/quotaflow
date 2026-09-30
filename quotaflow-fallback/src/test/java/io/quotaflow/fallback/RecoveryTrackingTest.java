package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class RecoveryTrackingTest {
    private static LevelRequest request(String key) {
        return new LevelRequest(new BucketIdentity(new QuotaDomain("n", "p"), "p", Scope.USER, key),
                new Limit(1, 1, Duration.ofSeconds(1)), Algorithm.GCRA, 1);
    }
    @Test void reservesWholeChainBeforeAnyDebitAndKeepsPinnedEntries() {
        var tracking = new RecoveryTracking(2);
        var pinned = tracking.retain(List.of(request("a")), 0);
        assertNull(tracking.retain(List.of(request("b"), request("c")), 0));
        assertEquals(1, tracking.size());
        var same = tracking.retain(List.of(request("a")), 1);
        assertSame(pinned.get(0), same.get(0));
        tracking.release(same.get(0));
        assertEquals(1, tracking.size());
        tracking.release(pinned.get(0));
        assertEquals(0, tracking.size());
    }
    @Test void expiryRequiresIdleLeaseAndMonotonicLastUse() {
        var tracking = new RecoveryTracking(1);
        var pin = tracking.retain(List.of(request("a")), 100).get(0);
        var operation = tracking.retain(List.of(request("a")), 50).get(0);
        assertEquals(100, pin.lastUse.get());
        assertFalse(pin.expired(2_000_000_000L), "an unresolved attempt prevents expiry");
        tracking.release(operation);
        assertFalse(pin.expired(1_000_000_099L));
        assertTrue(pin.expired(1_000_000_100L));
        var longer = new LevelRequest(pin.key, new Limit(2, 1, Duration.ofSeconds(1)), Algorithm.GCRA, 1);
        var lease = tracking.retain(List.of(longer), 200).get(0);
        tracking.release(lease);
        var shorter = tracking.retain(List.of(request("a")), 300).get(0);
        tracking.release(shorter);
        assertFalse(pin.expired(1_000_000_300L), "a shorter policy must not shorten retained debt history");
        assertTrue(pin.expired(2_000_000_300L));
        tracking.release(pin);
    }
    @Test void lateOldRequestsCannotReplaceCurrentPolicyMetadata() {
        var tracking = new RecoveryTracking(1);
        var old = request("a");
        var current = new LevelRequest(old.storageKey(), new Limit(2, 1, Duration.ofSeconds(1)), Algorithm.GCRA, 1);
        var pin = tracking.retain(List.of(current), 100, 2).get(0);
        var stale = tracking.retain(List.of(old), 200, 1).get(0);
        assertEquals(current, pin.metadata.get().request());
        tracking.release(stale); tracking.release(pin);
    }
}
