package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;

import io.quotaflow.core.Scope;
import org.junit.jupiter.api.Test;

class BucketIdentityTest {
    @Test
    void identityUsesAllComponentsWithoutDelimiterOrTelemetryAliases() {
        QuotaDomain domain = new QuotaDomain("deployment", "provider");
        BucketIdentity parent = new BucketIdentity(domain, "tenant", Scope.TENANT, "secret:tenant");
        assertEquals(parent, new BucketIdentity(domain, "tenant", Scope.TENANT, "secret:tenant"));
        assertEquals(parent.hashCode(), new BucketIdentity(domain, "tenant", Scope.TENANT, "secret:tenant").hashCode());
        assertNotEquals(parent, new BucketIdentity(domain, "tenant:secret", Scope.TENANT, "tenant"));
        assertNotEquals(parent, new BucketIdentity(domain, "tenant", Scope.USER, "secret:tenant"));
        assertNotEquals(parent, new BucketIdentity(new QuotaDomain("other", "provider"), "tenant", Scope.TENANT, "secret:tenant"));
        assertNotEquals(parent, new BucketIdentity(new QuotaDomain("deployment", "other"), "tenant", Scope.TENANT, "secret:tenant"));
        assertFalse(parent.toString().contains("secret:tenant"));
        assertEquals("secret:tenant", parent.rawKey());
        assertEquals(domain, parent.domain());
    }

    @Test
    void validatesComponentsWithoutEchoingSensitiveInput() {
        QuotaDomain domain = new QuotaDomain("deployment", "provider");
        for (String invalid : new String[] {null, "", " ", "\uD800", "\uD800x", "\uDC00"}) {
            assertThrows(IllegalArgumentException.class, () -> new QuotaDomain(invalid, "p"));
            assertThrows(IllegalArgumentException.class, () -> new QuotaDomain("n", invalid));
            assertThrows(IllegalArgumentException.class, () -> new BucketIdentity(domain, invalid, Scope.KEY, "k"));
            assertThrows(IllegalArgumentException.class, () -> new BucketIdentity(domain, "p", Scope.KEY, invalid));
        }
        assertThrows(NullPointerException.class, () -> new BucketIdentity(null, "p", Scope.KEY, "k"));
        assertThrows(NullPointerException.class, () -> new BucketIdentity(domain, "p", null, "k"));
        assertEquals("\uD83D\uDE80", new BucketIdentity(domain, "p", Scope.KEY, "\uD83D\uDE80").rawKey());
    }
}
