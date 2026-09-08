package io.quotaflow.core;

/**
 * A resolved limit key. {@code rawKey} identifies the bucket in storage and
 * must never be logged above DEBUG or used as a metric tag; {@code keyGroup}
 * is the aggregated, cardinality-safe identity that observability may use.
 */
public record LimitKey(String rawKey, String keyGroup) {

    public LimitKey {
        if (rawKey == null || rawKey.isBlank()) {
            throw new IllegalArgumentException("rawKey must not be blank");
        }
        if (keyGroup == null || keyGroup.isBlank()) {
            throw new IllegalArgumentException("keyGroup must not be blank");
        }
    }
}
