package io.quotaflow.config;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.PolicyConfigurationException;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.RateLimitPolicy;
import io.quotaflow.core.Reaction;
import io.quotaflow.core.Scope;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Parses a flat {@code Map<String,String>} (properties files adapt trivially)
 * into a validated {@link QuotaFlowConfiguration}. The model mirrors the
 * policy model: {@code quotaflow.defaults.*} provides global defaults and
 * {@code quotaflow.policies.<id>.*} declares one policy per id. All
 * compilation and chain validation is delegated to {@link PolicySet#compile}
 * so there is exactly one source of truth for validation rules.
 *
 * <p>Recognized keys:
 * <ul>
 *   <li>{@code quotaflow.defaults.algorithm|reaction|refill-period|expected-instances}</li>
 *   <li>{@code quotaflow.policies.<id>.scope} (required), {@code .algorithm},
 *       {@code .reaction}, {@code .priority}, {@code .parent},
 *       {@code .key-resolver}, {@code .default-key}</li>
 *   <li>static limit: {@code .limit.capacity}, {@code .limit.refill-amount},
 *       {@code .limit.refill-period} (falls back to the default refill period)</li>
 *   <li>dynamic limit: {@code .limit-ref} (mutually exclusive with all
 *       {@code .limit.*} fields)</li>
 * </ul>
 *
 * <p>Enum values are case-insensitive and accept either {@code -} or
 * {@code _} as separator (e.g. {@code token-bucket}). Durations use the
 * ISO-8601 format of {@link Duration#parse}. Any key inside the
 * {@code quotaflow.} namespace that is not recognized fails parsing with an
 * error naming the key; keys outside the namespace are ignored so shared
 * property files stay usable. Policy ids must not contain dots.
 */
public final class ConfigurationParser {

    public static final String NAMESPACE = "quotaflow.";
    public static final String DEFAULTS_PREFIX = "quotaflow.defaults.";
    public static final String POLICIES_PREFIX = "quotaflow.policies.";

    private static final String DEFAULTS_ALGORITHM = "quotaflow.defaults.algorithm";
    private static final String DEFAULTS_REACTION = "quotaflow.defaults.reaction";
    private static final String DEFAULTS_REFILL_PERIOD = "quotaflow.defaults.refill-period";
    private static final String DEFAULTS_EXPECTED_INSTANCES = "quotaflow.defaults.expected-instances";

    private ConfigurationParser() {
    }

    /**
     * Parses and compiles the payload.
     *
     * @throws PolicyConfigurationException on unknown keys, malformed values
     *         or any policy validation failure; the message names the
     *         offending key or policy
     */
    public static QuotaFlowConfiguration parse(Map<String, String> properties) {
        Map<String, Map<String, String>> policies = new LinkedHashMap<>();
        Map<String, String> defaults = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(NAMESPACE)) {
                continue;
            }
            if (key.startsWith(POLICIES_PREFIX)) {
                String rest = key.substring(POLICIES_PREFIX.length());
                int dot = rest.indexOf('.');
                if (dot <= 0 || dot == rest.length() - 1) {
                    throw unknownKey(key);
                }
                policies.computeIfAbsent(rest.substring(0, dot), id -> new LinkedHashMap<>())
                        .put(rest.substring(dot + 1), entry.getValue());
            } else if (key.startsWith(DEFAULTS_PREFIX)) {
                defaults.put(key.substring(DEFAULTS_PREFIX.length()), entry.getValue());
            } else {
                throw unknownKey(key);
            }
        }

        Defaults d = parseDefaults(defaults);
        List<RateLimitPolicy> compiled = new ArrayList<>(policies.size());
        for (Map.Entry<String, Map<String, String>> policy : policies.entrySet()) {
            compiled.add(parsePolicy(policy.getKey(), policy.getValue(), d));
        }
        return new QuotaFlowConfiguration(PolicySet.compile(compiled), d.expectedInstances());
    }

    private static Defaults parseDefaults(Map<String, String> defaults) {
        Algorithm algorithm = null;
        Reaction reaction = null;
        Duration refillPeriod = null;
        Optional<Integer> expectedInstances = Optional.empty();
        for (Map.Entry<String, String> entry : defaults.entrySet()) {
            String key = DEFAULTS_PREFIX + entry.getKey();
            String value = entry.getValue();
            switch (entry.getKey()) {
                case "algorithm" -> algorithm = parseEnum(key, value, Algorithm.class);
                case "reaction" -> reaction = parseEnum(key, value, Reaction.class);
                case "refill-period" -> refillPeriod = parseDuration(key, value);
                case "expected-instances" -> expectedInstances =
                        Optional.of(parsePositiveInt(key, value));
                default -> throw unknownKey(key);
            }
        }
        return new Defaults(
                algorithm == null ? Algorithm.TOKEN_BUCKET : algorithm,
                reaction == null ? Reaction.REJECT : reaction,
                refillPeriod,
                expectedInstances);
    }

    private static RateLimitPolicy parsePolicy(String id, Map<String, String> fields, Defaults defaults) {
        String prefix = POLICIES_PREFIX + id + '.';
        RateLimitPolicy.Builder builder = RateLimitPolicy.builder(id)
                .algorithm(defaults.algorithm())
                .reaction(defaults.reaction());
        String limitRef = null;
        Long capacity = null;
        Long refillAmount = null;
        Duration refillPeriod = null;
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            String key = prefix + entry.getKey();
            String value = entry.getValue();
            switch (entry.getKey()) {
                case "scope" -> builder.scope(parseEnum(key, value, Scope.class));
                case "algorithm" -> builder.algorithm(parseEnum(key, value, Algorithm.class));
                case "reaction" -> builder.reaction(parseEnum(key, value, Reaction.class));
                case "priority" -> builder.priority(parseInt(key, value));
                case "parent" -> builder.parentId(value);
                case "key-resolver" -> builder.keyResolverId(value);
                case "default-key" -> builder.defaultKey(value);
                case "limit.capacity" -> capacity = parsePositiveLong(key, value);
                case "limit.refill-amount" -> refillAmount = parsePositiveLong(key, value);
                case "limit.refill-period" -> refillPeriod = parseDuration(key, value);
                case "limit-ref" -> limitRef = value;
                default -> throw unknownKey(key);
            }
        }
        if (limitRef != null && !limitRef.isBlank()) {
            if (capacity != null || refillAmount != null || refillPeriod != null) {
                throw new PolicyConfigurationException("policy '" + id + "' declares both '" + prefix
                        + "limit-ref' and static limit fields; exactly one limit source is allowed");
            }
            builder.limitRef(limitRef);
            return builder.build();
        }
        if (capacity == null) {
            throw missingKey(prefix + "limit.capacity", id);
        }
        if (refillAmount == null) {
            throw missingKey(prefix + "limit.refill-amount", id);
        }
        Duration period = refillPeriod != null ? refillPeriod : defaults.refillPeriod();
        if (period == null) {
            throw new PolicyConfigurationException("policy '" + id + "' has no refill period: set '"
                    + prefix + "limit.refill-period' or '" + DEFAULTS_REFILL_PERIOD + "'");
        }
        builder.limit(new Limit(capacity, refillAmount, period));
        return builder.build();
    }

    private record Defaults(
            Algorithm algorithm, Reaction reaction, Duration refillPeriod,
            Optional<Integer> expectedInstances) {
    }

    private static PolicyConfigurationException unknownKey(String key) {
        return new PolicyConfigurationException("unknown configuration key '" + key + "'");
    }

    private static PolicyConfigurationException missingKey(String key, String policyId) {
        return new PolicyConfigurationException(
                "policy '" + policyId + "' is missing required key '" + key
                        + "' (or declare '" + POLICIES_PREFIX + policyId + ".limit-ref')");
    }

    private static <E extends Enum<E>> E parseEnum(String key, String value, Class<E> type) {
        String normalized = value.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        try {
            return Enum.valueOf(type, normalized);
        } catch (IllegalArgumentException e) {
            throw new PolicyConfigurationException("invalid value '" + value + "' for key '" + key
                    + "'; expected one of " + java.util.Arrays.toString(type.getEnumConstants()));
        }
    }

    private static Duration parseDuration(String key, String value) {
        try {
            return Duration.parse(value.trim());
        } catch (DateTimeParseException e) {
            throw new PolicyConfigurationException("invalid value '" + value + "' for key '" + key
                    + "'; expected an ISO-8601 duration (e.g. PT1S, PT1M)");
        }
    }

    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new PolicyConfigurationException(
                    "invalid value '" + value + "' for key '" + key + "'; expected an integer");
        }
    }

    private static int parsePositiveInt(String key, String value) {
        int parsed = parseInt(key, value);
        if (parsed < 1) {
            throw new PolicyConfigurationException(
                    "invalid value '" + value + "' for key '" + key + "'; expected an integer >= 1");
        }
        return parsed;
    }

    private static long parsePositiveLong(String key, String value) {
        long parsed;
        try {
            parsed = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new PolicyConfigurationException(
                    "invalid value '" + value + "' for key '" + key + "'; expected an integer");
        }
        if (parsed < 1) {
            throw new PolicyConfigurationException(
                    "invalid value '" + value + "' for key '" + key + "'; expected an integer >= 1");
        }
        return parsed;
    }
}
