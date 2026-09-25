package io.quotaflow.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quotaflow.config.ConfigurationParser;
import io.quotaflow.config.QuotaFlowConfiguration;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.PolicyConfigurationException;
import io.quotaflow.core.Reaction;
import io.quotaflow.core.Scope;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.handler.NoUnboundElementsBindHandler;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class QuotaFlowPropertiesTest {

    private static QuotaFlowProperties bind(Map<String, String> source) {
        Binder binder = new Binder(new MapConfigurationPropertySource(source));
        return binder.bind("quotaflow", QuotaFlowProperties.class).get();
    }

    private static Map<String, String> policyProperties() {
        Map<String, String> source = new LinkedHashMap<>();
        source.put("quotaflow.defaults.algorithm", "token-bucket");
        source.put("quotaflow.defaults.reaction", "reject");
        source.put("quotaflow.defaults.expected-instances", "4");
        source.put("quotaflow.policies.user-api.scope", "user");
        source.put("quotaflow.policies.user-api.limit.capacity", "100");
        source.put("quotaflow.policies.user-api.limit.refill-amount", "50");
        source.put("quotaflow.policies.user-api.limit.refill-period", "PT1M");
        source.put("quotaflow.policies.user-api.priority", "3");
        return source;
    }

    @Test
    void bindsPoliciesAndConvertsToConfigurationMapShape() {
        QuotaFlowProperties properties = bind(policyProperties());

        Map<String, String> map = properties.toConfigurationMap();
        assertThat(map)
                .containsEntry("quotaflow.defaults.algorithm", "TOKEN_BUCKET")
                .containsEntry("quotaflow.defaults.reaction", "REJECT")
                .containsEntry("quotaflow.defaults.expected-instances", "4")
                .containsEntry("quotaflow.policies.user-api.scope", "USER")
                .containsEntry("quotaflow.policies.user-api.limit.capacity", "100")
                .containsEntry("quotaflow.policies.user-api.limit.refill-amount", "50")
                .containsEntry("quotaflow.policies.user-api.limit.refill-period", "PT1M")
                .containsEntry("quotaflow.policies.user-api.priority", "3")
                .doesNotContainKey("quotaflow.redis.url")
                .doesNotContainKey("quotaflow.enabled");

        QuotaFlowConfiguration configuration = ConfigurationParser.parse(map);
        assertThat(configuration.policySet().policy("user-api").limit().orElseThrow().capacity()).isEqualTo(100);
        assertThat(configuration.expectedInstances()).contains(4);
    }

    @Test
    void relaxedBindingAcceptsCamelCaseYamlStyleKeys() {
        Map<String, String> source = new LinkedHashMap<>();
        source.put("quotaflow.policies.user-api.scope", "user");
        source.put("quotaflow.policies.user-api.keyResolver", "principal");
        source.put("quotaflow.policies.user-api.defaultKey", "anonymous");
        source.put("quotaflow.policies.user-api.limit.capacity", "10");
        source.put("quotaflow.policies.user-api.limit.refillAmount", "10");
        source.put("quotaflow.policies.user-api.limit.refillPeriod", "10s");

        QuotaFlowProperties properties = bind(source);

        assertThat(properties.getPolicies().get("user-api").getKeyResolver()).isEqualTo("principal");
        assertThat(properties.getPolicies().get("user-api").getDefaultKey()).isEqualTo("anonymous");
        assertThat(properties.getPolicies().get("user-api").getLimit().getRefillPeriod())
                .isEqualTo(Duration.ofSeconds(10));

        Map<String, String> map = properties.toConfigurationMap();
        assertThat(map)
                .containsEntry("quotaflow.policies.user-api.key-resolver", "principal")
                .containsEntry("quotaflow.policies.user-api.default-key", "anonymous")
                .containsEntry("quotaflow.policies.user-api.limit.refill-amount", "10")
                .containsEntry("quotaflow.policies.user-api.limit.refill-period", "PT10S");
        assertThat(ConfigurationParser.parse(map).policySet().policy("user-api").scope())
                .isEqualTo(Scope.USER);
    }

    @Test
    void bindsEnumsLenientlyAndDurationsInSimpleForm() {
        Map<String, String> source = policyProperties();
        source.put("quotaflow.defaults.reaction", "Throttle");
        source.put("quotaflow.redis.connect-timeout", "250ms");

        QuotaFlowProperties properties = bind(source);

        assertThat(properties.getDefaults().getReaction()).isEqualTo(Reaction.THROTTLE);
        assertThat(properties.getDefaults().getAlgorithm()).isEqualTo(Algorithm.TOKEN_BUCKET);
        assertThat(properties.getRedis().getConnectTimeout()).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    void dynamicLimitReferenceBindsAsLimitRef() {
        Map<String, String> source = new LinkedHashMap<>();
        source.put("quotaflow.policies.tenant-plan.scope", "tenant");
        source.put("quotaflow.policies.tenant-plan.limitRef", "tariff:standard");

        QuotaFlowProperties properties = bind(source);

        Map<String, String> map = properties.toConfigurationMap();
        assertThat(map).containsEntry("quotaflow.policies.tenant-plan.limit-ref", "tariff:standard");
        assertThat(ConfigurationParser.parse(map).policySet().policy("tenant-plan").limitRef())
                .contains("tariff:standard");
    }

    @Test
    void invalidPoliciesFailCompilationWithActionableError() {
        QuotaFlowProperties properties = bind(Map.of("quotaflow.policies.broken.scope", "user"));

        assertThatThrownBy(() -> ConfigurationParser.parse(properties.toConfigurationMap()))
                .isInstanceOf(PolicyConfigurationException.class)
                .hasMessageContaining("limit.capacity");
    }

    @Test
    void unknownKeysInsideNamespaceFailBinding() {
        assertThatThrownBy(() -> bindIgnoringUnknown(Map.of(
                        "quotaflow.policies.user-api.scope", "user", "quotaflow.unknown-key", "1")))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining("quotaflow.unknown-key");
    }

    @Test
    void unknownKeysInsidePolicyFailBinding() {
        assertThatThrownBy(() -> bindIgnoringUnknown(
                        Map.of("quotaflow.policies.user-api.bogus-field", "1")))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining("quotaflow.policies.user-api.bogus-field");
    }

    /** Mirrors how Boot binds @ConfigurationProperties with ignoreUnknownFields=false. */
    private static QuotaFlowProperties bindIgnoringUnknown(Map<String, String> source) {
        Binder binder = new Binder(java.util.List.of(new MapConfigurationPropertySource(source)),
                null, null, null, new NoUnboundElementsBindHandler(BindHandler.DEFAULT));
        return binder.bind("quotaflow", QuotaFlowProperties.class).get();
    }

    @Test
    void defaultsAreSensible() {
        QuotaFlowProperties properties = new QuotaFlowProperties();

        assertThat(properties.isEnabled()).isTrue();
        assertThat(properties.isFailOnRedisMissing()).isFalse();
        assertThat(properties.getRedis().getUrl()).isEqualTo("redis://localhost:6379");
        assertThat(properties.getMaxWaitersPerPolicy()).isEqualTo(1000);
    }
}
