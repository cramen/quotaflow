package io.quotaflow.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.quotaflow.config.ConfigurationParser;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class EnvironmentConfigSourceTest {

    @Test
    void collectsPolicyAndDefaultsKeysOnly() {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("quotaflow.policies.user-api.scope", "user")
                .withProperty("quotaflow.policies.user-api.limit.capacity", "5")
                .withProperty("quotaflow.policies.user-api.limit.refill-amount", "5")
                .withProperty("quotaflow.policies.user-api.limit.refill-period", "PT1M")
                .withProperty("quotaflow.defaults.refill-period", "PT1S")
                .withProperty("quotaflow.redis.url", "redis://example:6379")
                .withProperty("quotaflow.enabled", "true")
                .withProperty("unrelated.property", "x");

        Map<String, String> payload = new EnvironmentConfigSource(environment).load();

        assertThat(payload)
                .containsOnly(
                        Map.entry("quotaflow.policies.user-api.scope", "user"),
                        Map.entry("quotaflow.policies.user-api.limit.capacity", "5"),
                        Map.entry("quotaflow.policies.user-api.limit.refill-amount", "5"),
                        Map.entry("quotaflow.policies.user-api.limit.refill-period", "PT1M"),
                        Map.entry("quotaflow.defaults.refill-period", "PT1S"));
        assertThat(ConfigurationParser.parse(payload).policySet().size()).isEqualTo(1);
    }

    @Test
    void camelCasePropertyNamesAreNormalizedToKebabAfterThePolicyId() {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("quotaflow.policies.my-Policy.scope", "user")
                .withProperty("quotaflow.policies.my-Policy.keyResolver", "principal")
                .withProperty("quotaflow.policies.my-Policy.limit.capacity", "5")
                .withProperty("quotaflow.policies.my-Policy.limit.refillAmount", "5")
                .withProperty("quotaflow.policies.my-Policy.limit.refillPeriod", "PT1M");

        Map<String, String> payload = new EnvironmentConfigSource(environment).load();

        // the policy id segment is preserved verbatim; field names are kebab-cased
        assertThat(payload)
                .containsEntry("quotaflow.policies.my-Policy.key-resolver", "principal")
                .containsEntry("quotaflow.policies.my-Policy.limit.refill-amount", "5")
                .containsEntry("quotaflow.policies.my-Policy.limit.refill-period", "PT1M");
        assertThat(ConfigurationParser.parse(payload).policySet().policy("my-Policy").id())
                .isEqualTo("my-Policy");
    }
}
