package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class KeyResolversTest {

    private static final RateLimitPolicy TENANT_POLICY = RateLimitPolicy.builder("t")
            .limit(new Limit(1, 1, Duration.ofSeconds(1)))
            .scope(Scope.TENANT)
            .build();

    private static RateLimitPolicy scoped(Scope scope) {
        return RateLimitPolicy.builder("p-" + scope)
                .limit(new Limit(1, 1, Duration.ofSeconds(1)))
                .scope(scope)
                .build();
    }

    @Test
    void resolvesPrincipal() {
        RateLimitContext context = RateLimitContext.builder()
                .put(RateLimitContext.PRINCIPAL, "alice")
                .build();
        assertEquals(new LimitKey("alice", "principal"),
                KeyResolvers.principal().resolve(context, TENANT_POLICY).orElseThrow());
    }

    @Test
    void resolvesJwtClaim() {
        RateLimitContext context = RateLimitContext.builder()
                .put(RateLimitContext.jwtClaim("sub"), "user-42")
                .build();
        assertEquals(new LimitKey("user-42", "jwt:sub"),
                KeyResolvers.jwtClaim("sub").resolve(context, TENANT_POLICY).orElseThrow());
    }

    @Test
    void resolvesTenantId() {
        RateLimitContext context = RateLimitContext.builder()
                .put(RateLimitContext.TENANT_ID, "acme")
                .build();
        assertEquals(new LimitKey("acme", "tenant"),
                KeyResolvers.tenantId().resolve(context, TENANT_POLICY).orElseThrow());
    }

    @Test
    void resolvesApiKey() {
        RateLimitContext context = RateLimitContext.builder()
                .put(RateLimitContext.API_KEY, "ak-1")
                .build();
        assertEquals(new LimitKey("ak-1", "api-key"),
                KeyResolvers.apiKey().resolve(context, TENANT_POLICY).orElseThrow());
    }

    @Test
    void resolvesHeader() {
        RateLimitContext context = RateLimitContext.builder()
                .put(RateLimitContext.header("X-Client"), "cli-9")
                .build();
        assertEquals(new LimitKey("cli-9", "header:X-Client"),
                KeyResolvers.header("X-Client").resolve(context, TENANT_POLICY).orElseThrow());
    }

    @Test
    void missingAttributeResolvesToEmpty() {
        assertTrue(KeyResolvers.principal().resolve(RateLimitContext.empty(), TENANT_POLICY).isEmpty());
        assertTrue(KeyResolvers.jwtClaim("sub").resolve(RateLimitContext.empty(), TENANT_POLICY).isEmpty());
        assertTrue(KeyResolvers.tenantId().resolve(RateLimitContext.empty(), TENANT_POLICY).isEmpty());
        assertTrue(KeyResolvers.apiKey().resolve(RateLimitContext.empty(), TENANT_POLICY).isEmpty());
        assertTrue(KeyResolvers.header("X").resolve(RateLimitContext.empty(), TENANT_POLICY).isEmpty());
    }

    @Test
    void blankAttributeResolvesToEmpty() {
        RateLimitContext context = RateLimitContext.builder()
                .put(RateLimitContext.PRINCIPAL, "  ")
                .build();
        assertTrue(KeyResolvers.principal().resolve(context, TENANT_POLICY).isEmpty());
    }

    @Test
    void nonStringAttributeIsStringified() {
        RateLimitContext context = RateLimitContext.builder()
                .put(RateLimitContext.TENANT_ID, 42)
                .build();
        assertEquals(new LimitKey("42", "tenant"),
                KeyResolvers.tenantId().resolve(context, TENANT_POLICY).orElseThrow());
    }

    @Test
    void scopeBasedResolverMapsScopesToAttributes() {
        RateLimitContext context = RateLimitContext.builder()
                .put(RateLimitContext.TENANT_ID, "acme")
                .put(RateLimitContext.PRINCIPAL, "alice")
                .put(RateLimitContext.API_KEY, "ak-1")
                .build();
        KeyResolver resolver = KeyResolvers.scopeBased();
        assertEquals(new LimitKey("global", "global"),
                resolver.resolve(RateLimitContext.empty(), scoped(Scope.GLOBAL)).orElseThrow());
        assertEquals(Optional.of(new LimitKey("acme", "tenant")),
                resolver.resolve(context, scoped(Scope.TENANT)));
        assertEquals(Optional.of(new LimitKey("alice", "principal")),
                resolver.resolve(context, scoped(Scope.USER)));
        assertEquals(Optional.of(new LimitKey("ak-1", "api-key")),
                resolver.resolve(context, scoped(Scope.KEY)));
        assertTrue(resolver.resolve(RateLimitContext.empty(), scoped(Scope.USER)).isEmpty());
    }

    @Test
    void customResolverIsUsable() {
        KeyResolver custom = (context, policy) -> context.get("billing.account", String.class)
                .map(account -> new LimitKey(account, "billing"));
        RateLimitContext context = RateLimitContext.builder().put("billing.account", "acct-1").build();
        assertEquals(new LimitKey("acct-1", "billing"), custom.resolve(context, TENANT_POLICY).orElseThrow());
    }
}
