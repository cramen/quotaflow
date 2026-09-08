package io.quotaflow.core;

import java.util.Optional;

/**
 * Built-in {@link KeyResolver} factories over the well-known
 * {@link RateLimitContext} attributes.
 */
public final class KeyResolvers {

    /** Well-known resolver id for {@link #principal()}. */
    public static final String PRINCIPAL_ID = "principal";
    /** Well-known resolver id for {@link #tenantId()}. */
    public static final String TENANT_ID_ID = "tenant-id";
    /** Well-known resolver id for {@link #apiKey()}. */
    public static final String API_KEY_ID = "api-key";

    private static final String GLOBAL_KEY_GROUP = "global";
    private static final String GLOBAL_RAW_KEY = "global";

    private KeyResolvers() {
    }

    /** Resolves the key from the authenticated principal name. */
    public static KeyResolver principal() {
        return attributeBased(RateLimitContext.PRINCIPAL, "principal");
    }

    /** Resolves the key from the named JWT claim. */
    public static KeyResolver jwtClaim(String claimName) {
        return attributeBased(RateLimitContext.jwtClaim(claimName), "jwt:" + claimName);
    }

    /** Resolves the key from the tenant id attribute. */
    public static KeyResolver tenantId() {
        return attributeBased(RateLimitContext.TENANT_ID, "tenant");
    }

    /** Resolves the key from the API key attribute. */
    public static KeyResolver apiKey() {
        return attributeBased(RateLimitContext.API_KEY, "api-key");
    }

    /** Resolves the key from the named header. */
    public static KeyResolver header(String headerName) {
        return attributeBased(RateLimitContext.header(headerName), "header:" + headerName);
    }

    /**
     * Default scope-based resolver: a single bucket for {@link Scope#GLOBAL},
     * tenant id for {@link Scope#TENANT}, principal for {@link Scope#USER} and
     * API key for {@link Scope#KEY}.
     */
    public static KeyResolver scopeBased() {
        return (context, policy) -> switch (policy.scope()) {
            case GLOBAL -> Optional.of(new LimitKey(GLOBAL_RAW_KEY, GLOBAL_KEY_GROUP));
            case TENANT -> tenantId().resolve(context, policy);
            case USER -> principal().resolve(context, policy);
            case KEY -> apiKey().resolve(context, policy);
        };
    }

    private static KeyResolver attributeBased(String attributeKey, String keyGroup) {
        return (context, policy) -> context.get(attributeKey)
                .map(String::valueOf)
                .filter(value -> !value.isBlank())
                .map(value -> new LimitKey(value, keyGroup));
    }
}
