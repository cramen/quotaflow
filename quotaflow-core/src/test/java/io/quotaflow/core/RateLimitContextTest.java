package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RateLimitContextTest {

    @Test
    void wellKnownKeyConstants() {
        assertEquals("principal", RateLimitContext.PRINCIPAL);
        assertEquals("tenant.id", RateLimitContext.TENANT_ID);
        assertEquals("api.key", RateLimitContext.API_KEY);
        assertEquals("jwt.claim.", RateLimitContext.JWT_CLAIM_PREFIX);
        assertEquals("header.", RateLimitContext.HEADER_PREFIX);
        assertEquals("jwt.claim.sub", RateLimitContext.jwtClaim("sub"));
        assertEquals("header.X-Tenant", RateLimitContext.header("X-Tenant"));
    }

    @Test
    void storesAndRetrievesAttributes() {
        RateLimitContext context = RateLimitContext.builder()
                .put(RateLimitContext.PRINCIPAL, "alice")
                .put(RateLimitContext.TENANT_ID, "acme")
                .build();
        assertEquals("alice", context.get(RateLimitContext.PRINCIPAL).orElseThrow());
        assertEquals("alice", context.get(RateLimitContext.PRINCIPAL, String.class).orElseThrow());
        assertTrue(context.get("absent").isEmpty());
    }

    @Test
    void typedGetFiltersWrongType() {
        RateLimitContext context = RateLimitContext.builder().put("n", 42).build();
        assertEquals(42, context.get("n", Integer.class).orElseThrow());
        assertTrue(context.get("n", String.class).isEmpty());
    }

    @Test
    void builtContextIsImmutable() {
        RateLimitContext.Builder builder = RateLimitContext.builder().put("a", "1");
        RateLimitContext context = builder.build();
        builder.put("b", "2");
        assertTrue(context.get("b").isEmpty());
    }

    @Test
    void emptyContextIsShared() {
        assertTrue(RateLimitContext.empty().get("anything").isEmpty());
        assertTrue(RateLimitContext.builder().build().get("anything").isEmpty());
    }

    @Test
    void rejectsNullKeysAndValues() {
        RateLimitContext.Builder builder = RateLimitContext.builder();
        assertThrows(NullPointerException.class, () -> builder.put(null, "v"));
        assertThrows(NullPointerException.class, () -> builder.put("k", null));
        assertThrows(NullPointerException.class, () -> RateLimitContext.jwtClaim(null));
        assertThrows(NullPointerException.class, () -> RateLimitContext.header(null));
    }
}
