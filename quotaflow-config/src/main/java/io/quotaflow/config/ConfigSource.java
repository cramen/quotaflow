package io.quotaflow.config;

import java.util.Map;

/**
 * Source of flat configuration payloads. The default implementations are
 * dependency-free (properties file, in-memory map); future sources (Spring
 * Cloud Config, Redis-backed) implement this SPI and feed the same reload
 * pipeline.
 *
 * <p>Implementations should return an immutable snapshot; the reloader
 * defensively copies the payload. A failing load signals the failure by
 * throwing {@link ConfigSourceException} (or any runtime exception) — the
 * serving policy set is never disturbed by a failed read.
 */
@FunctionalInterface
public interface ConfigSource {

    Map<String, String> load();
}
