package io.quotaflow.spring;

import io.quotaflow.core.store.BatchRateLimitStore;
import io.quotaflow.core.store.StateSeeder;
import java.util.Objects;

/**
 * The primary (distributed or local-only) store behind the fallback wrapper,
 * plus its recovery seeder. Closes the underlying client resources on context
 * shutdown when the primary owns any.
 */
final class PrimaryStoreHolder implements AutoCloseable {

    private final BatchRateLimitStore store;
    private final StateSeeder seeder;
    private final AutoCloseable resources;

    PrimaryStoreHolder(BatchRateLimitStore store, StateSeeder seeder, AutoCloseable resources) {
        this.store = Objects.requireNonNull(store, "store");
        this.seeder = Objects.requireNonNull(seeder, "seeder");
        this.resources = Objects.requireNonNull(resources, "resources");
    }

    BatchRateLimitStore store() {
        return store;
    }

    StateSeeder seeder() {
        return seeder;
    }

    @Override
    public void close() throws Exception {
        resources.close();
    }
}
