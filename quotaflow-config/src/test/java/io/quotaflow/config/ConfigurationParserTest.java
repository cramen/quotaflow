package io.quotaflow.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.PolicyConfigurationException;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.RateLimitPolicy;
import io.quotaflow.core.Reaction;
import io.quotaflow.core.Scope;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigurationParserTest {

    private static Map<String, String> map(String... keyValues) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private static Map<String, String> minimalPolicy(String id) {
        return map(
                "quotaflow.policies." + id + ".scope", "user",
                "quotaflow.policies." + id + ".limit.capacity", "10",
                "quotaflow.policies." + id + ".limit.refill-amount", "5",
                "quotaflow.policies." + id + ".limit.refill-period", "PT1S");
    }

    @Test
    void defaultsAreInheritedAndOverridesApply() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.defaults.algorithm", "gcra");
        properties.put("quotaflow.defaults.reaction", "throttle");
        properties.put("quotaflow.policies.web.priority", "3");
        properties.put("quotaflow.policies.web.algorithm", "token-bucket");

        PolicySet set = ConfigurationParser.parse(properties).policySet();
        RateLimitPolicy web = set.policy("web");
        assertEquals(Scope.USER, web.scope());
        assertEquals(new Limit(10, 5, Duration.ofSeconds(1)), web.limit().orElseThrow());
        assertEquals(Algorithm.TOKEN_BUCKET, web.algorithm(), "policy override wins over default");
        assertEquals(Reaction.THROTTLE, web.reaction(), "default inherited when not overridden");
        assertEquals(3, web.priority());
    }

    @Test
    void policyDefaultsMatchPolicyModelDefaults() {
        PolicySet set = ConfigurationParser.parse(minimalPolicy("web")).policySet();
        RateLimitPolicy web = set.policy("web");
        assertEquals(Algorithm.TOKEN_BUCKET, web.algorithm());
        assertEquals(Reaction.REJECT, web.reaction());
    }

    @Test
    void refillPeriodFallsBackToDefaults() {
        Map<String, String> properties = map(
                "quotaflow.defaults.refill-period", "PT1M",
                "quotaflow.policies.web.scope", "global",
                "quotaflow.policies.web.limit.capacity", "100",
                "quotaflow.policies.web.limit.refill-amount", "100");
        PolicySet set = ConfigurationParser.parse(properties).policySet();
        assertEquals(new Limit(100, 100, Duration.ofMinutes(1)),
                set.policy("web").limit().orElseThrow());
    }

    @Test
    void missingRefillPeriodEverywhereFailsNamingTheKeys() {
        Map<String, String> properties = map(
                "quotaflow.policies.web.scope", "global",
                "quotaflow.policies.web.limit.capacity", "100",
                "quotaflow.policies.web.limit.refill-amount", "100");
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(properties));
        assertTrue(e.getMessage().contains("quotaflow.policies.web.limit.refill-period"));
        assertTrue(e.getMessage().contains("quotaflow.defaults.refill-period"));
    }

    @Test
    void unknownPolicyKeyFailsNamingTheKey() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.policies.web.limt.capacity", "5");
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(properties));
        assertTrue(e.getMessage().contains("quotaflow.policies.web.limt.capacity"));
    }

    @Test
    void unknownDefaultsKeyFailsNamingTheKey() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.defaults.algoritm", "gcra");
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(properties));
        assertTrue(e.getMessage().contains("quotaflow.defaults.algoritm"));
    }

    @Test
    void malformedNamespacedKeyFailsNamingTheKey() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.policies.web.limit.capacity.extra", "1");
        assertThrows(PolicyConfigurationException.class, () -> ConfigurationParser.parse(properties));
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(map("quotaflow.unknown", "1")));
        assertTrue(e.getMessage().contains("quotaflow.unknown"));
    }

    @Test
    void keysOutsideTheNamespaceAreIgnored() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("server.port", "8080");
        properties.put("spring.application.name", "orders");
        assertEquals(1, ConfigurationParser.parse(properties).policySet().size());
    }

    @Test
    void limitRefPolicyParsesWithoutLimitFields() {
        PolicySet set = ConfigurationParser.parse(map(
                "quotaflow.policies.web.scope", "user",
                "quotaflow.policies.web.limit-ref", "tariff")).policySet();
        RateLimitPolicy web = set.policy("web");
        assertTrue(web.limit().isEmpty());
        assertEquals("tariff", web.limitRef().orElseThrow());
    }

    @Test
    void limitRefWithStaticLimitFieldsFailsNamingThePolicy() {
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(map(
                        "quotaflow.policies.web.scope", "user",
                        "quotaflow.policies.web.limit-ref", "tariff",
                        "quotaflow.policies.web.limit.capacity", "10",
                        "quotaflow.policies.web.limit.refill-amount", "10",
                        "quotaflow.policies.web.limit.refill-period", "PT1S")));
        assertTrue(e.getMessage().contains("'web'"));
    }

    @Test
    void missingCapacityFailsNamingTheMissingKey() {
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(map(
                        "quotaflow.policies.web.scope", "user",
                        "quotaflow.policies.web.limit.refill-amount", "10",
                        "quotaflow.policies.web.limit.refill-period", "PT1S")));
        assertTrue(e.getMessage().contains("quotaflow.policies.web.limit.capacity"));
    }

    @Test
    void missingRefillAmountFailsNamingTheMissingKey() {
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(map(
                        "quotaflow.policies.web.scope", "user",
                        "quotaflow.policies.web.limit.capacity", "10",
                        "quotaflow.policies.web.limit.refill-period", "PT1S")));
        assertTrue(e.getMessage().contains("quotaflow.policies.web.limit.refill-amount"));
    }

    @Test
    void missingScopeFailsFromPolicyModelValidation() {
        assertThrows(Exception.class, () -> ConfigurationParser.parse(map(
                "quotaflow.policies.web.limit.capacity", "10",
                "quotaflow.policies.web.limit.refill-amount", "10",
                "quotaflow.policies.web.limit.refill-period", "PT1S")));
    }

    @Test
    void invalidEnumValueFailsNamingKeyAndAllowedValues() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.policies.web.scope", "org");
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(properties));
        assertTrue(e.getMessage().contains("quotaflow.policies.web.scope"));
        assertTrue(e.getMessage().contains("GLOBAL"));
    }

    @Test
    void invalidNumberFailsNamingTheKey() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.policies.web.limit.capacity", "ten");
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(properties));
        assertTrue(e.getMessage().contains("quotaflow.policies.web.limit.capacity"));
    }

    @Test
    void nonPositiveNumberFailsNamingTheKey() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.policies.web.limit.capacity", "0");
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(properties));
        assertTrue(e.getMessage().contains("quotaflow.policies.web.limit.capacity"));
    }

    @Test
    void invalidDurationFailsNamingTheKey() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.policies.web.limit.refill-period", "1s");
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(properties));
        assertTrue(e.getMessage().contains("quotaflow.policies.web.limit.refill-period"));
    }

    @Test
    void invalidPriorityFailsNamingTheKey() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.policies.web.priority", "high");
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(properties));
        assertTrue(e.getMessage().contains("quotaflow.policies.web.priority"));
    }

    @Test
    void hierarchyAndNamedFieldsParse() {
        PolicySet set = ConfigurationParser.parse(map(
                "quotaflow.policies.g.scope", "global",
                "quotaflow.policies.g.limit.capacity", "1000",
                "quotaflow.policies.g.limit.refill-amount", "1000",
                "quotaflow.policies.g.limit.refill-period", "PT1S",
                "quotaflow.policies.t.scope", "tenant",
                "quotaflow.policies.t.limit.capacity", "100",
                "quotaflow.policies.t.limit.refill-amount", "100",
                "quotaflow.policies.t.limit.refill-period", "PT1S",
                "quotaflow.policies.t.parent", "g",
                "quotaflow.policies.t.key-resolver", "billing",
                "quotaflow.policies.t.default-key", "anonymous")).policySet();
        RateLimitPolicy tenant = set.policy("t");
        assertEquals("g", tenant.parentId().orElseThrow());
        assertEquals("billing", tenant.keyResolverId().orElseThrow());
        assertEquals("anonymous", tenant.defaultKey().orElseThrow());
        assertEquals(2, set.chainFromLeaf("t").size());
    }

    @Test
    void unknownParentFailsFromPolicySetValidation() {
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(map(
                        "quotaflow.policies.t.scope", "tenant",
                        "quotaflow.policies.t.limit.capacity", "100",
                        "quotaflow.policies.t.limit.refill-amount", "100",
                        "quotaflow.policies.t.limit.refill-period", "PT1S",
                        "quotaflow.policies.t.parent", "ghost")));
        assertTrue(e.getMessage().contains("ghost"));
    }

    @Test
    void expectedInstancesParsesFromDefaults() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.defaults.expected-instances", "4");
        QuotaFlowConfiguration config = ConfigurationParser.parse(properties);
        assertEquals(4, config.expectedInstances().orElseThrow());
        assertTrue(ConfigurationParser.parse(minimalPolicy("web")).expectedInstances().isEmpty());
    }

    @Test
    void invalidExpectedInstancesFailsNamingTheKey() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.defaults.expected-instances", "0");
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(properties));
        assertTrue(e.getMessage().contains("quotaflow.defaults.expected-instances"));
    }

    @Test
    void emptyPayloadFailsFromPolicySetValidation() {
        assertThrows(PolicyConfigurationException.class,
                () -> ConfigurationParser.parse(Map.of("server.port", "8080")));
    }

    @Test
    void enumValuesAreCaseAndSeparatorInsensitive() {
        Map<String, String> properties = minimalPolicy("web");
        properties.put("quotaflow.policies.web.algorithm", "Token_Bucket");
        properties.put("quotaflow.policies.web.scope", "USER");
        PolicySet set = ConfigurationParser.parse(properties).policySet();
        assertEquals(Algorithm.TOKEN_BUCKET, set.policy("web").algorithm());
        assertEquals(Scope.USER, set.policy("web").scope());
    }
}
