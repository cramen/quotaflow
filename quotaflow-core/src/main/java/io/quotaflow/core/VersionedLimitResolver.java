package io.quotaflow.core;

import java.util.Objects;
import java.util.Optional;

/**
 * Publishes coherent immutable tariff views. Snapshot retrieval must be a fast in-memory operation.
 * Revisions are ordered by the shared tariff authority across the fleet and survive provider restarts;
 * they are not client timestamps or per-instance counters. Empty means no trustworthy view is available.
 */
@FunctionalInterface
public interface VersionedLimitResolver extends LimitResolver {
    Optional<LimitSnapshot> snapshot();

    @Override default Optional<Limit> resolve(String limitRef, String keyGroup) {
        return Objects.requireNonNull(snapshot(), "snapshot result").flatMap(view -> view.resolve(limitRef, keyGroup));
    }
}
