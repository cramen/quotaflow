package io.quotaflow.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * {@link ConfigSource} over a Java properties file. Each {@link #load()}
 * re-reads the file, so polling watchers observe the latest content regardless
 * of file-system timestamp granularity.
 */
public final class PropertiesFileConfigSource implements ConfigSource {

    private final Path path;

    public PropertiesFileConfigSource(Path path) {
        this.path = Objects.requireNonNull(path, "path");
    }

    public Path path() {
        return path;
    }

    @Override
    public Map<String, String> load() {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            properties.load(in);
        } catch (IOException e) {
            throw new ConfigSourceException(
                    "failed to read configuration file " + path + ": " + e.getMessage(), e);
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (String name : properties.stringPropertyNames()) {
            map.put(name, properties.getProperty(name));
        }
        return map;
    }
}
