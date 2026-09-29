package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.*;

import io.lettuce.core.cluster.SlotHash;
import io.quotaflow.core.Scope;
import io.quotaflow.core.store.BucketIdentity;
import io.quotaflow.core.store.QuotaDomain;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class RedisKeySchemeTest {
    private final RedisKeyScheme scheme = RedisKeyScheme.defaults();
    private final QuotaDomain domain = new QuotaDomain("deployment", "provider");

    private BucketIdentity bucket(String policy, Scope scope, String raw) {
        return new BucketIdentity(domain, policy, scope, raw);
    }

    @Test
    void versionTwoEncodingHasAStableCrossLanguageFixture() {
        assertEquals("qf:v2:{07c4c3c27f5dc623d4bb86c436620cd5ddc2d96b7b9a5b84d903de742b6e59ea}:af4bac75c45b71ee300dad2a2ab96710d4883462b24a1e740465d1b05488e59c", scheme.singleKey(bucket("tenant", Scope.TENANT, "acme")));
    }

    @Test
    void siblingsAndDirectCallsUseOneParentAndOneSlot() {
        BucketIdentity parent = bucket("provider", Scope.GLOBAL, "shared");
        BucketIdentity alice = bucket("user", Scope.USER, "alice");
        BucketIdentity bob = bucket("user", Scope.USER, "bob");
        List<String> a = scheme.chainKeys(List.of(parent, alice));
        List<String> b = scheme.chainKeys(List.of(parent, bob));
        assertEquals(a.get(0), b.get(0));
        assertEquals(a.get(0), scheme.singleKey(parent));
        assertEquals(a.get(0), scheme.chainLevelKey(domain, parent));
        assertNotEquals(a.get(1), b.get(1));
        int slot = SlotHash.getSlot(a.get(0));
        for (String key : List.of(a.get(1), b.get(1), scheme.controlKey(domain))) {
            assertEquals(slot, SlotHash.getSlot(key));
        }
        assertFalse(a.contains(scheme.controlKey(domain)));
    }

    @Test
    void changingRootRawKeyDoesNotDuplicateDescendant() {
        BucketIdentity child = bucket("tenant", Scope.TENANT, "same-tenant");
        assertEquals(scheme.chainKeys(List.of(bucket("provider", Scope.GLOBAL, "one"), child)).get(1),
                scheme.chainKeys(List.of(bucket("provider", Scope.GLOBAL, "two"), child)).get(1));
    }

    @Test
    void adversarialInputCannotChooseSlotsOrAliasTuples() {
        List<BucketIdentity> identities = List.of(
                bucket("p:x", Scope.KEY, "y"), bucket("p", Scope.KEY, "x:y"),
                bucket("p", Scope.KEY, "{slot}"), bucket("p", Scope.KEY, "sha256:" + "a".repeat(64)),
                bucket("p", Scope.KEY, "x".repeat(10000)), bucket("p", Scope.KEY, "\uD83D\uDE80"),
                new BucketIdentity(new QuotaDomain("other", "provider"), "p", Scope.KEY, "y"),
                new BucketIdentity(new QuotaDomain("deployment", "other"), "p", Scope.KEY, "y"));
        var keys = new HashSet<String>();
        for (BucketIdentity identity : identities) {
            String key = scheme.singleKey(identity);
            assertEquals(137, key.getBytes(StandardCharsets.US_ASCII).length);
            assertTrue(key.matches("qf:v2:\\{[0-9a-f]{64}\\}:[0-9a-f]{64}"));
            assertEquals(key, scheme.singleKey(identity));
            assertTrue(keys.add(key));
        }
        assertNotEquals(scheme.controlKey(domain), scheme.controlKey(new QuotaDomain("deployment", "other")));
        assertNotEquals(scheme.manifestKey("a:b"), scheme.manifestKey("a"));
    }

    @Test
    void literalLegacyDigestDoesNotAliasTheLongInput() throws Exception {
        String raw = "x".repeat(10000);
        String digest = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                .digest(raw.getBytes(StandardCharsets.UTF_8)));
        assertNotEquals(scheme.singleKey(bucket("p", Scope.KEY, raw)),
                scheme.singleKey(bucket("p", Scope.KEY, "sha256:" + digest)));
    }

    @Test
    void rejectsMixedDomainsAndInvalidArgumentsBeforeExecution() {
        BucketIdentity one = bucket("p", Scope.KEY, "one");
        BucketIdentity other = new BucketIdentity(new QuotaDomain("other", "provider"), "p", Scope.KEY, "two");
        assertThrows(IllegalArgumentException.class, () -> scheme.chainKeys(List.of()));
        assertThrows(IllegalArgumentException.class, () -> scheme.chainKeys(List.of(one, other)));
        assertThrows(IllegalArgumentException.class, () -> scheme.chainLevelKey(domain, other));
        assertThrows(NullPointerException.class, () -> scheme.singleKey(null));
        assertThrows(NullPointerException.class, () -> scheme.chainKeys(null));
        assertThrows(NullPointerException.class, () -> scheme.controlKey(null));
        assertThrows(IllegalArgumentException.class, () -> scheme.manifestKey(""));
    }
}
