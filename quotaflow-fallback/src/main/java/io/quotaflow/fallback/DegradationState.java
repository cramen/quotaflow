package io.quotaflow.fallback;

/**
 * Degradation state of a {@link FallbackRateLimitStore}: {@code CLOSED} serves
 * from the primary store, {@code OPEN} serves from the local fallback with no
 * primary calls, {@code HALF_OPEN} admits a single probe request to the
 * primary while all other traffic stays local.
 */
public enum DegradationState {
    CLOSED,
    OPEN,
    HALF_OPEN
}
