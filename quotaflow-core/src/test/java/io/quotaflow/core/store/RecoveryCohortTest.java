package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import org.junit.jupiter.api.Test;

class RecoveryCohortTest {
    @Test void membershipOrderDoesNotChangeItsCanonicalDigest() {
        var a = new RecoveryCohort(List.of("b", "a"));
        var b = new RecoveryCohort(List.of("a", "b"));
        assertEquals(a, b); assertEquals(a.digest(), b.digest());
        assertEquals(0, a.slot("a")); assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a.digest(), new RecoveryCohort(List.of("ab")).digest());
        assertNotEquals(new RecoveryCohort(List.of("a", "bc")).digest(), new RecoveryCohort(List.of("ab", "c")).digest());
        assertThrows(IllegalArgumentException.class, () -> a.validate(1, "a"));
        assertThrows(IllegalArgumentException.class, () -> a.validate(2, "missing"));
        assertThrows(IllegalArgumentException.class, () -> new RecoveryCohort(List.of("a", "a")));
        assertThrows(IllegalArgumentException.class, () -> new RecoveryCohort(List.of()));
        RecoveryCohort.single().validate(1, "single");
    }
    @Test void contextsRejectInvalidCountersAndRedactOwnershipHandles() {
        var session = new RecoverySession("incarnation", RecoveryCohort.single().digest(), "single", 0, 1, "secret-session");
        var ctx = new RecoveryContext(new QuotaDomain("n", "root"), session, 0, 0, 0, "a".repeat(64), RecoveryPhase.NORMAL);
        assertFalse(session.toString().contains("secret-session"));
        assertFalse(ctx.toString().contains("secret-session"));
        assertThrows(IllegalArgumentException.class, () -> new RecoveryContext(ctx.domain(), session,
                RecoverySession.MAX_COUNTER + 1, 0, 0, ctx.configurationFingerprint(), ctx.phase()));
    }
    @Test void invalidOwnershipAndContextFieldsNeverReachAStore() {
        String digest = RecoveryCohort.single().digest();
        for (long generation : new long[]{0, -1, RecoverySession.MAX_COUNTER + 1})
            assertThrows(IllegalArgumentException.class, () -> new RecoverySession("inc", digest, "single", 0, generation, "owner"));
        assertThrows(IllegalArgumentException.class, () -> new RecoverySession("inc", digest, "single", -1, 1, "owner"));
        assertThrows(IllegalArgumentException.class, () -> new RecoverySession("inc", "invalid", "single", 0, 1, "owner"));
        var session = new RecoverySession("inc", digest, "single", 0, RecoverySession.MAX_COUNTER, "owner");
        var domain = new QuotaDomain("n", "r");
        assertThrows(IllegalArgumentException.class, () -> new RecoveryContext(domain, session, 0, -1, 0, digest, RecoveryPhase.NORMAL));
        assertThrows(IllegalArgumentException.class, () -> new RecoveryContext(domain, session, 0, 0, -1, digest, RecoveryPhase.NORMAL));
        assertThrows(IllegalArgumentException.class, () -> new RecoveryContext(domain, session, 0, 0, 0, "invalid", RecoveryPhase.NORMAL));
        assertNotEquals(RecoveryCohort.single(), "single");
        assertNotEquals(RecoveryCohort.single(), new RecoveryCohort(List.of("other")));
        assertTrue(RecoveryCohort.single().toString().contains("size=1"));
    }
}
