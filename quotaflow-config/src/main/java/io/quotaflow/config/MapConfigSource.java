package io.quotaflow.config;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mutable in-memory {@link ConfigSource} for programmatic configuration and
 * tests. {@link #update(Map)} publishes a new payload atomically; watchers and
 * {@code reload()} observe it on their next pass.
 */
public final class MapConfigSource implements ConfigSource {

    private final AtomicReference<Map<String, String>> payload;

    public MapConfigSource(Map<String, String> initial) {
        this.payload = new AtomicReference<>(Map.copyOf(Objects.requireNonNull(initial, "initial")));
    }

    public void update(Map<String, String> next) {
        payload.set(Map.copyOf(Objects.requireNonNull(next, "next")));
    }

    @Override
    public Map<String, String> load() {
        return payload.get();
    }
}
