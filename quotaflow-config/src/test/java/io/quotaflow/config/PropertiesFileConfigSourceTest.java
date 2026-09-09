package io.quotaflow.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PropertiesFileConfigSourceTest {

    @TempDir
    Path dir;

    @Test
    void loadsPropertiesAsStringMap() throws Exception {
        Path file = dir.resolve("quotaflow.properties");
        Files.writeString(file, "quotaflow.policies.u.scope=user\nquotaflow.policies.u.limit.capacity=10\n");
        Map<String, String> loaded = new PropertiesFileConfigSource(file).load();
        assertEquals("user", loaded.get("quotaflow.policies.u.scope"));
        assertEquals("10", loaded.get("quotaflow.policies.u.limit.capacity"));
    }

    @Test
    void reReadsReflectFileChanges() throws Exception {
        Path file = dir.resolve("quotaflow.properties");
        Files.writeString(file, "a=1\n");
        PropertiesFileConfigSource source = new PropertiesFileConfigSource(file);
        assertEquals("1", source.load().get("a"));
        Files.writeString(file, "a=2\n");
        assertEquals("2", source.load().get("a"));
    }

    @Test
    void missingFileFailsWithSourceException() {
        PropertiesFileConfigSource source =
                new PropertiesFileConfigSource(dir.resolve("absent.properties"));
        ConfigSourceException e = assertThrows(ConfigSourceException.class, source::load);
        assertEquals(dir.resolve("absent.properties"), source.path());
        org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().contains("absent.properties"));
    }
}
