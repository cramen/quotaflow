package io.quotaflow.store.redis;

import java.time.Duration;
import java.util.Objects;

/**
 * Timeouts for {@link RedisRateLimitStore}. Validated at construction: the
 * store-call timeout must be strictly below the business timeout, so a stalled
 * store surfaces as a timeout early enough for the caller's degradation path
 * to react before the business deadline.
 */
public record RedisStoreConfig(Duration commandTimeout, Duration businessTimeout) {

    public RedisStoreConfig {
        Objects.requireNonNull(commandTimeout, "commandTimeout");
        Objects.requireNonNull(businessTimeout, "businessTimeout");
        if (commandTimeout.isZero() || commandTimeout.isNegative()) {
            throw new IllegalArgumentException("commandTimeout must be positive, got " + commandTimeout);
        }
        if (businessTimeout.isZero() || businessTimeout.isNegative()) {
            throw new IllegalArgumentException("businessTimeout must be positive, got " + businessTimeout);
        }
        if (commandTimeout.compareTo(businessTimeout) >= 0) {
            throw new IllegalArgumentException(
                    "store command timeout (" + commandTimeout + ") must be strictly below the business"
                            + " timeout (" + businessTimeout + ") so a stalled store degrades before the"
                            + " business deadline; shorten the store timeout or raise the business timeout");
        }
    }

    /** Sensible starting point: 100 ms store calls under a 1 s business deadline. */
    public static RedisStoreConfig defaults() {
        return new RedisStoreConfig(Duration.ofMillis(100), Duration.ofSeconds(1));
    }
}
