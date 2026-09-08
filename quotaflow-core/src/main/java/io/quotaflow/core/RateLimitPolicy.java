package io.quotaflow.core;

import java.util.Objects;
import java.util.Optional;

/**
 * Immutable rate limit policy. Policies form chains through {@code parentId};
 * the engine evaluates a chain root-to-leaf (broadest scope first).
 *
 * <p>{@code keyResolverId} selects a named {@link KeyResolver}; when absent the
 * facade's default scope-based resolver is used. When the key cannot be
 * resolved, the request is rejected unless {@code defaultKey} is explicitly
 * configured, in which case that fixed key is used instead.
 */
public final class RateLimitPolicy {

    private final String id;
    private final Limit limit;
    private final Algorithm algorithm;
    private final Scope scope;
    private final Reaction reaction;
    private final int priority;
    private final String parentId;
    private final String keyResolverId;
    private final String defaultKey;

    private RateLimitPolicy(Builder builder) {
        this.id = requireNonBlank(builder.id, "id");
        this.limit = Objects.requireNonNull(builder.limit, "limit");
        this.algorithm = Objects.requireNonNull(builder.algorithm, "algorithm");
        this.scope = Objects.requireNonNull(builder.scope, "scope");
        this.reaction = Objects.requireNonNull(builder.reaction, "reaction");
        this.priority = builder.priority;
        this.parentId = builder.parentId;
        this.keyResolverId = builder.keyResolverId;
        this.defaultKey = builder.defaultKey;
        if (builder.parentId != null && builder.parentId.equals(builder.id)) {
            throw new IllegalArgumentException("policy '" + id + "' must not declare itself as parent");
        }
    }

    public static Builder builder(String id) {
        return new Builder(id);
    }

    public String id() {
        return id;
    }

    public Limit limit() {
        return limit;
    }

    public Algorithm algorithm() {
        return algorithm;
    }

    public Scope scope() {
        return scope;
    }

    public Reaction reaction() {
        return reaction;
    }

    /** Reserved for throttle prioritization; no effect in reject mode. */
    public int priority() {
        return priority;
    }

    public Optional<String> parentId() {
        return Optional.ofNullable(parentId);
    }

    public Optional<String> keyResolverId() {
        return Optional.ofNullable(keyResolverId);
    }

    /** Fixed fallback key used when key resolution yields nothing. */
    public Optional<String> defaultKey() {
        return Optional.ofNullable(defaultKey);
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    public static final class Builder {
        private final String id;
        private Limit limit;
        private Algorithm algorithm = Algorithm.TOKEN_BUCKET;
        private Scope scope;
        private Reaction reaction = Reaction.REJECT;
        private int priority;
        private String parentId;
        private String keyResolverId;
        private String defaultKey;

        private Builder(String id) {
            this.id = id;
        }

        public Builder limit(Limit limit) {
            this.limit = limit;
            return this;
        }

        public Builder algorithm(Algorithm algorithm) {
            this.algorithm = algorithm;
            return this;
        }

        public Builder scope(Scope scope) {
            this.scope = scope;
            return this;
        }

        public Builder reaction(Reaction reaction) {
            this.reaction = reaction;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder parentId(String parentId) {
            this.parentId = parentId;
            return this;
        }

        public Builder keyResolverId(String keyResolverId) {
            this.keyResolverId = keyResolverId;
            return this;
        }

        public Builder defaultKey(String defaultKey) {
            this.defaultKey = defaultKey;
            return this;
        }

        public RateLimitPolicy build() {
            return new RateLimitPolicy(this);
        }
    }
}
