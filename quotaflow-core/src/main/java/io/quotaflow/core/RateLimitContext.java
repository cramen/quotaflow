package io.quotaflow.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable, framework-free attribute carrier populated by framework adapters
 * and consumed by {@link KeyResolver}s. Well-known attribute keys are exposed
 * as constants so adapters and resolvers share them at compile time.
 */
public final class RateLimitContext {

    public static final String PRINCIPAL = "principal";
    public static final String TENANT_ID = "tenant.id";
    public static final String API_KEY = "api.key";
    public static final String JWT_CLAIM_PREFIX = "jwt.claim.";
    public static final String HEADER_PREFIX = "header.";

    private static final RateLimitContext EMPTY = new RateLimitContext(Map.of());

    private final Map<String, Object> attributes;

    private RateLimitContext(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public static String jwtClaim(String name) {
        return JWT_CLAIM_PREFIX + Objects.requireNonNull(name, "name");
    }

    public static String header(String name) {
        return HEADER_PREFIX + Objects.requireNonNull(name, "name");
    }

    public static RateLimitContext empty() {
        return EMPTY;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<Object> get(String key) {
        return Optional.ofNullable(attributes.get(key));
    }

    public <T> Optional<T> get(String key, Class<T> type) {
        return get(key).filter(type::isInstance).map(type::cast);
    }

    public static final class Builder {
        private final Map<String, Object> attributes = new HashMap<>();

        private Builder() {
        }

        public Builder put(String key, Object value) {
            attributes.put(
                    Objects.requireNonNull(key, "key"),
                    Objects.requireNonNull(value, "value"));
            return this;
        }

        public RateLimitContext build() {
            if (attributes.isEmpty()) {
                return EMPTY;
            }
            return new RateLimitContext(Collections.unmodifiableMap(new HashMap<>(attributes)));
        }
    }
}
