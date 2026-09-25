package io.quotaflow.spring;

/**
 * Behavior of {@link RateLimited} when its key expression yields no key
 * (blank expression, {@code null} or blank result). Neither option is an
 * unconditional allow.
 */
public enum OnMissingKey {

    /**
     * Reject the invocation immediately without consulting the store — the
     * same missing-key rejection the engine produces, decided before any
     * policy default key could apply.
     */
    REJECT,

    /**
     * Proceed without a key from the invocation, letting the policy's
     * configured default key apply; without one the engine still rejects.
     */
    USE_DEFAULT_KEY
}
