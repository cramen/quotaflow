package io.quotaflow.core;

import java.util.Optional;

/**
 * SPI that derives a limit key from the resolution context for a given policy.
 * Implementations return {@link Optional#empty()} when no key can be produced;
 * the engine then applies the policy's missing-key behavior (reject by
 * default, fixed default key only when explicitly configured).
 */
@FunctionalInterface
public interface KeyResolver {

    Optional<LimitKey> resolve(RateLimitContext context, RateLimitPolicy policy);
}
