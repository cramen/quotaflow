package io.quotaflow.spring;

import io.quotaflow.config.CachingLimitResolver;
import io.quotaflow.config.ConfigReloader;
import io.quotaflow.config.ConfigurationParser;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Reaction;
import io.quotaflow.core.Scope;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binding for {@code quotaflow.*} application properties. The policy model
 * mirrors the configuration module's flat map model exactly: {@link #toConfigurationMap()}
 * converts the bound properties into the {@code quotaflow.defaults.*} /
 * {@code quotaflow.policies.<id>.*} key shape {@link ConfigurationParser}
 * understands, so validation and compilation stay single-sourced in the
 * configuration module (invalid policies fail startup with its actionable
 * errors). Spring's relaxed binding accepts kebab-case, camelCase and
 * underscore-separated property names; unknown keys inside {@code quotaflow.*}
 * fail binding.
 *
 * <p>Keys that tune the Spring wiring itself (store endpoints, fallback
 * breaker, reload pipeline) live alongside the policy model but are not part
 * of the configuration payload the reload pipeline swaps.
 */
@ConfigurationProperties(prefix = "quotaflow", ignoreUnknownFields = false)
public class QuotaFlowProperties {

    /** Master switch of the integration; everything backs off when false. */
    private boolean enabled = true;

    /** Fail startup when no reachable Redis is found instead of degrading to local-only mode. */
    private boolean failOnRedisMissing = false;

    /** Bound of each throttle policy's waiter queue. */
    private int maxWaitersPerPolicy = 1000;

    private final Redis redis = new Redis();
    private final Fallback fallback = new Fallback();
    private final Reload reload = new Reload();
    private final Defaults defaults = new Defaults();
    private final Map<String, Policy> policies = new LinkedHashMap<>();

    /**
     * Converts the bound policy model into the flat map shape
     * {@link ConfigurationParser#parse(Map)} understands. Only
     * {@code quotaflow.defaults.*} and {@code quotaflow.policies.*} keys are
     * emitted; Spring-only tuning keys never enter the configuration payload.
     */
    public Map<String, String> toConfigurationMap() {
        Map<String, String> map = new LinkedHashMap<>();
        if (defaults.getAlgorithm() != null) {
            map.put(ConfigurationParser.DEFAULTS_PREFIX + "algorithm", defaults.getAlgorithm().name());
        }
        if (defaults.getReaction() != null) {
            map.put(ConfigurationParser.DEFAULTS_PREFIX + "reaction", defaults.getReaction().name());
        }
        if (defaults.getRefillPeriod() != null) {
            map.put(ConfigurationParser.DEFAULTS_PREFIX + "refill-period",
                    defaults.getRefillPeriod().toString());
        }
        if (defaults.getExpectedInstances() != null) {
            map.put(ConfigurationParser.DEFAULTS_PREFIX + "expected-instances",
                    defaults.getExpectedInstances().toString());
        }
        policies.forEach((id, policy) -> policy.putInto(map, ConfigurationParser.POLICIES_PREFIX + id + '.'));
        return map;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isFailOnRedisMissing() {
        return failOnRedisMissing;
    }

    public void setFailOnRedisMissing(boolean failOnRedisMissing) {
        this.failOnRedisMissing = failOnRedisMissing;
    }

    public int getMaxWaitersPerPolicy() {
        return maxWaitersPerPolicy;
    }

    public void setMaxWaitersPerPolicy(int maxWaitersPerPolicy) {
        this.maxWaitersPerPolicy = maxWaitersPerPolicy;
    }

    public Redis getRedis() {
        return redis;
    }

    public Fallback getFallback() {
        return fallback;
    }

    public Reload getReload() {
        return reload;
    }

    public Defaults getDefaults() {
        return defaults;
    }

    public Map<String, Policy> getPolicies() {
        return policies;
    }

    /** Distributed store endpoint and timeouts. */
    public static class Redis {

        /** Redis URL in Lettuce form ({@code redis://host:port}). */
        private String url = "redis://localhost:6379";

        /** How long establishing the startup connection may take before local-only mode kicks in. */
        private Duration connectTimeout = Duration.ofSeconds(1);

        /** Upper bound of a single store command. */
        private Duration commandTimeout = Duration.ofMillis(100);

        /** Business deadline the store timeout must stay strictly below. */
        private Duration businessTimeout = Duration.ofSeconds(1);

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public Duration getConnectTimeout() {
            return connectTimeout;
        }

        public void setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        public Duration getCommandTimeout() {
            return commandTimeout;
        }

        public void setCommandTimeout(Duration commandTimeout) {
            this.commandTimeout = commandTimeout;
        }

        public Duration getBusinessTimeout() {
            return businessTimeout;
        }

        public void setBusinessTimeout(Duration businessTimeout) {
            this.businessTimeout = businessTimeout;
        }
    }

    /** Degradation breaker tuning for the fallback store wrapper. */
    public static class Fallback {

        /** Consecutive primary store failures that trip the breaker open. */
        private int failureThreshold = 3;

        /** How long the breaker stays open before the first probe. */
        private Duration openDuration = Duration.ofSeconds(1);

        /** Cap for the backoff that doubles the open duration after each failed probe. */
        private Duration maxOpenDuration = Duration.ofSeconds(30);

        /** Hard cap on bucket entries replayed into the recovered store. */
        private int maxSeedEntries = 10_000;

        public int getFailureThreshold() {
            return failureThreshold;
        }

        public void setFailureThreshold(int failureThreshold) {
            this.failureThreshold = failureThreshold;
        }

        public Duration getOpenDuration() {
            return openDuration;
        }

        public void setOpenDuration(Duration openDuration) {
            this.openDuration = openDuration;
        }

        public Duration getMaxOpenDuration() {
            return maxOpenDuration;
        }

        public void setMaxOpenDuration(Duration maxOpenDuration) {
            this.maxOpenDuration = maxOpenDuration;
        }

        public int getMaxSeedEntries() {
            return maxSeedEntries;
        }

        public void setMaxSeedEntries(int maxSeedEntries) {
            this.maxSeedEntries = maxSeedEntries;
        }
    }

    /** Hot-reload pipeline tuning. */
    public static class Reload {

        /** Watcher poll interval; a zero or negative interval disables watching. */
        private Duration pollInterval = ConfigReloader.DEFAULT_POLL_INTERVAL;

        /** TTL of cached dynamic limit resolutions. */
        private Duration resolverTtl = CachingLimitResolver.DEFAULT_TTL;

        public Duration getPollInterval() {
            return pollInterval;
        }

        public void setPollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
        }

        public Duration getResolverTtl() {
            return resolverTtl;
        }

        public void setResolverTtl(Duration resolverTtl) {
            this.resolverTtl = resolverTtl;
        }
    }

    /** Global defaults every policy inherits unless it overrides the field. */
    public static class Defaults {

        private Algorithm algorithm;
        private Reaction reaction;
        private Duration refillPeriod;
        private Integer expectedInstances;

        public Algorithm getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(Algorithm algorithm) {
            this.algorithm = algorithm;
        }

        public Reaction getReaction() {
            return reaction;
        }

        public void setReaction(Reaction reaction) {
            this.reaction = reaction;
        }

        public Duration getRefillPeriod() {
            return refillPeriod;
        }

        public void setRefillPeriod(Duration refillPeriod) {
            this.refillPeriod = refillPeriod;
        }

        public Integer getExpectedInstances() {
            return expectedInstances;
        }

        public void setExpectedInstances(Integer expectedInstances) {
            this.expectedInstances = expectedInstances;
        }
    }

    /** One rate limit policy; mirrors the configuration module's policy fields. */
    public static class Policy {

        private Scope scope;
        private Algorithm algorithm;
        private Reaction reaction;
        private Integer priority;
        private String parent;
        private String keyResolver;
        private String defaultKey;
        private String limitRef;
        private final PolicyLimit limit = new PolicyLimit();

        private void putInto(Map<String, String> map, String prefix) {
            if (scope != null) {
                map.put(prefix + "scope", scope.name());
            }
            if (algorithm != null) {
                map.put(prefix + "algorithm", algorithm.name());
            }
            if (reaction != null) {
                map.put(prefix + "reaction", reaction.name());
            }
            if (priority != null) {
                map.put(prefix + "priority", priority.toString());
            }
            if (parent != null) {
                map.put(prefix + "parent", parent);
            }
            if (keyResolver != null) {
                map.put(prefix + "key-resolver", keyResolver);
            }
            if (defaultKey != null) {
                map.put(prefix + "default-key", defaultKey);
            }
            if (limitRef != null) {
                map.put(prefix + "limit-ref", limitRef);
            }
            if (limit.getCapacity() != null) {
                map.put(prefix + "limit.capacity", limit.getCapacity().toString());
            }
            if (limit.getRefillAmount() != null) {
                map.put(prefix + "limit.refill-amount", limit.getRefillAmount().toString());
            }
            if (limit.getRefillPeriod() != null) {
                map.put(prefix + "limit.refill-period", limit.getRefillPeriod().toString());
            }
        }

        public Scope getScope() {
            return scope;
        }

        public void setScope(Scope scope) {
            this.scope = scope;
        }

        public Algorithm getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(Algorithm algorithm) {
            this.algorithm = algorithm;
        }

        public Reaction getReaction() {
            return reaction;
        }

        public void setReaction(Reaction reaction) {
            this.reaction = reaction;
        }

        public Integer getPriority() {
            return priority;
        }

        public void setPriority(Integer priority) {
            this.priority = priority;
        }

        public String getParent() {
            return parent;
        }

        public void setParent(String parent) {
            this.parent = parent;
        }

        public String getKeyResolver() {
            return keyResolver;
        }

        public void setKeyResolver(String keyResolver) {
            this.keyResolver = keyResolver;
        }

        public String getDefaultKey() {
            return defaultKey;
        }

        public void setDefaultKey(String defaultKey) {
            this.defaultKey = defaultKey;
        }

        public String getLimitRef() {
            return limitRef;
        }

        public void setLimitRef(String limitRef) {
            this.limitRef = limitRef;
        }

        public PolicyLimit getLimit() {
            return limit;
        }
    }

    /** Static limit of a policy; mutually exclusive with {@code limit-ref}. */
    public static class PolicyLimit {

        private Long capacity;
        private Long refillAmount;
        private Duration refillPeriod;

        public Long getCapacity() {
            return capacity;
        }

        public void setCapacity(Long capacity) {
            this.capacity = capacity;
        }

        public Long getRefillAmount() {
            return refillAmount;
        }

        public void setRefillAmount(Long refillAmount) {
            this.refillAmount = refillAmount;
        }

        public Duration getRefillPeriod() {
            return refillPeriod;
        }

        public void setRefillPeriod(Duration refillPeriod) {
            this.refillPeriod = refillPeriod;
        }
    }
}
