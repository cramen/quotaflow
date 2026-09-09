package io.quotaflow.core;

import java.util.Optional;

/**
 * Engine hook that resolves the effective limit of a policy declaring a
 * dynamic {@code limitRef} at decision time. Implementations receive only the
 * limit reference and the cardinality-safe key group — never raw keys — so
 * resolution cannot leak tenant or user identities into external systems.
 *
 * <p>The hook is consulted exactly once per resolution; production wiring is
 * expected to place a TTL cache in front of expensive resolvers so the hot
 * path performs no external calls. Resolver implementations should be fast
 * in-memory lookups; blocking external calls inside a resolver are the
 * resolver author's responsibility.
 *
 * <p>An empty result means the reference cannot be resolved for this key
 * group; the engine then rejects the request (never an unconditional allow).
 */
@FunctionalInterface
public interface LimitResolver {

    Optional<Limit> resolve(String limitRef, String keyGroup);
}
