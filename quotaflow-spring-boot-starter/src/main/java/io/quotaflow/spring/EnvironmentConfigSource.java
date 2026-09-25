package io.quotaflow.spring;

import io.quotaflow.config.ConfigSource;
import io.quotaflow.config.ConfigurationParser;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;

/**
 * {@link ConfigSource} over the Spring {@link ConfigurableEnvironment}: every
 * reload re-reads the environment, so replaced property sources (for example a
 * refreshed Spring Cloud Config) take effect through the standard pipeline.
 *
 * <p>Only {@code quotaflow.policies.*} and {@code quotaflow.defaults.*} keys
 * enter the payload — the parser rejects anything else in the namespace, so
 * Spring-only tuning keys ({@code quotaflow.redis.*} and friends) are filtered
 * out. Property names after the policy id are normalized to the kebab-case the
 * parser expects, so YAML-authored camelCase keys (for example
 * {@code refillPeriod}) reload identically to kebab-case ones; the policy id
 * segment is never touched. Keys from enumerable property sources only;
 * operating-system environment variables are intentionally not traversed.
 */
final class EnvironmentConfigSource implements ConfigSource {

    private final ConfigurableEnvironment environment;

    EnvironmentConfigSource(ConfigurableEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public Map<String, String> load() {
        Map<String, String> payload = new LinkedHashMap<>();
        for (PropertySource<?> source : environment.getPropertySources()) {
            if (!(source instanceof EnumerablePropertySource<?> enumerable)) {
                continue;
            }
            for (String name : enumerable.getPropertyNames()) {
                String normalized = normalize(name);
                if (normalized == null || payload.containsKey(normalized)) {
                    continue;
                }
                String value = environment.getProperty(name);
                if (value != null) {
                    payload.put(normalized, value);
                }
            }
        }
        return payload;
    }

    private static String normalize(String key) {
        if (key.startsWith(ConfigurationParser.POLICIES_PREFIX)) {
            String rest = key.substring(ConfigurationParser.POLICIES_PREFIX.length());
            int dot = rest.indexOf('.');
            if (dot <= 0 || dot == rest.length() - 1) {
                // malformed; pass through so the parser reports the offending key
                return key;
            }
            return ConfigurationParser.POLICIES_PREFIX
                    + rest.substring(0, dot + 1)
                    + toKebabCase(rest.substring(dot + 1));
        }
        if (key.startsWith(ConfigurationParser.DEFAULTS_PREFIX)) {
            return ConfigurationParser.DEFAULTS_PREFIX
                    + toKebabCase(key.substring(ConfigurationParser.DEFAULTS_PREFIX.length()));
        }
        return null;
    }

    /** Lowercases each dot-separated segment, inserting '-' before every uppercase letter. */
    private static String toKebabCase(String value) {
        StringBuilder result = new StringBuilder(value.length() + 4);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '.') {
                result.append('.');
            } else if (Character.isUpperCase(c)) {
                result.append('-').append(Character.toLowerCase(c));
            } else {
                result.append(c);
            }
        }
        return result.toString().toLowerCase(Locale.ROOT);
    }
}
