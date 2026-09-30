package io.quotaflow.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quotaflow.config.ConfigReloader;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.Limit;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.QuotaFlow;
import io.quotaflow.core.RateLimitContext;
import io.quotaflow.core.RateLimitPolicy;
import io.quotaflow.core.Scope;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.RateLimitStore;
import io.quotaflow.fallback.CoordinatedFallbackStore;
import io.quotaflow.micrometer.MicrometerThrottleMetrics;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.ReactiveWebApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class QuotaFlowAutoConfigurationTest {

    private static final String[] POLICY = {
        "quotaflow.policies.user-api.scope=user",
        "quotaflow.policies.user-api.limit.capacity=5",
        "quotaflow.policies.user-api.limit.refill-amount=5",
        "quotaflow.policies.user-api.limit.refill-period=PT1S",
        "quotaflow.redis.url=redis://localhost:6390",
        "quotaflow.redis.connect-timeout=100ms"
    };

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(QuotaFlowAutoConfiguration.class))
            .withPropertyValues(POLICY);

    @Test
    void customFacadeWithRegistryOmitsUnsupportedAuxiliaryMetrics() {
        runner.withUserConfiguration(UserFacadeConfiguration.class, MetricsConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(MicrometerThrottleMetrics.class);
        });
    }

    @Test
    void optionalSecurityTracingAndWebLibrariesCanBeAbsent() {
        runner.withClassLoader(new FilteredClassLoader("org.springframework.security", "org.springframework.web", "jakarta.servlet", "io.micrometer.tracing"))
                .withUserConfiguration(UserStoreConfiguration.class).run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void assemblesFullStackByDefault() {
        runner.run(context -> {
            assertThat(context)
                    .hasSingleBean(CoordinatedFallbackStore.class)
                    .hasSingleBean(DefaultQuotaFlow.class)
                    .hasSingleBean(ConfigReloader.class)
                    .hasSingleBean(RateLimitInterceptor.class)
                    .hasSingleBean(RateLimitedAdvisor.class);
            assertThat(context)
                    .doesNotHaveBean(RateLimitExceptionHandler.class)
                    .doesNotHaveBean(RateLimitWebExceptionHandler.class);

            QuotaFlow flow = context.getBean(QuotaFlow.class);
            RateLimitContext alice =
                    RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "alice").build();
            assertThat(flow.tryAcquire("user-api", alice).isAllowed()).isFalse();
        });
    }

    @Test
    void disabledViaPropertyCreatesNothing() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(QuotaFlowAutoConfiguration.class))
                .withPropertyValues("quotaflow.enabled=false")
                .run(context -> assertThat(context)
                        .doesNotHaveBean(QuotaFlow.class)
                        .doesNotHaveBean(ConfigReloader.class)
                        .doesNotHaveBean(RateLimitInterceptor.class)
                        .doesNotHaveBean(CoordinatedFallbackStore.class));
    }

    @Test
    void userStoreWins() {
        runner.withUserConfiguration(UserStoreConfiguration.class).run(context -> {
            assertThat(context).doesNotHaveBean(CoordinatedFallbackStore.class);
            assertThat(context).hasSingleBean(RateLimitStore.class);
            assertThat(context).hasSingleBean(DefaultQuotaFlow.class);
        });
    }

    @Test
    void userFacadeWins() {
        runner.withUserConfiguration(UserFacadeConfiguration.class).run(context -> {
            assertThat(context).hasSingleBean(QuotaFlow.class);
            assertThat(context.getBean(QuotaFlow.class))
                    .isSameAs(context.getBean("customQuotaFlow"));
            // the reload pipeline targets the auto-configured facade type only
            assertThat(context).doesNotHaveBean(ConfigReloader.class);
            assertThat(context).hasSingleBean(RateLimitInterceptor.class);
        });
    }

    @Test
    void lettuceAbsentStartsWithZeroCreditAndVisibleDegradation() {
        runner.withClassLoader(new FilteredClassLoader("io.lettuce")).run(context -> {
            assertThat(context).hasSingleBean(CoordinatedFallbackStore.class);
            assertThat(context).hasSingleBean(DefaultQuotaFlow.class);
            QuotaFlow flow = context.getBean(QuotaFlow.class);
            RateLimitContext alice =
                    RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "alice").build();
            assertThat(flow.tryAcquire("user-api", alice).isAllowed()).isFalse();
        });
    }

    @Test void missingDriverHonorsFailFast() {
        runner.withClassLoader(new FilteredClassLoader("io.lettuce"))
                .withPropertyValues("quotaflow.fail-on-redis-missing=true")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void meterRegistryEnablesMetricBridges() {
        runner.withUserConfiguration(MetricsConfiguration.class).run(context -> {
            assertThat(context).hasSingleBean(MicrometerThrottleMetrics.class);

            QuotaFlow flow = context.getBean(QuotaFlow.class);
            RateLimitContext alice =
                    RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "alice").build();
            flow.tryAcquire("user-api", alice);
            ((io.quotaflow.core.DefaultQuotaFlow) flow).flushObservations().toCompletableFuture().join();

            MeterRegistry registry = context.getBean(MeterRegistry.class);
            assertThat(registry.get("quotaflow.decisions")
                            .tags("result", "reject", "policy", "user-api", "key-group", "principal")
                            .counter()
                            .count())
                    .isOne();
            assertThat(registry.get("quotaflow.degraded").gauge().value()).isOne();
        });
    }

    @Test
    void noMeterRegistrySkipsMetricBridges() {
        runner.run(context -> assertThat(context).doesNotHaveBean(MicrometerThrottleMetrics.class));
    }

    @Test
    void failFastWhenRedisMissingAndFlagSet() {
        runner.withPropertyValues("quotaflow.fail-on-redis-missing=true").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasStackTraceContaining("quotaflow.fail-on-redis-missing=true")
                    .hasStackTraceContaining("Redis is unavailable");
        });
    }

    @Test
    void invalidPoliciesFailStartupWithActionableError() {
        runner.withPropertyValues("quotaflow.policies.broken.scope=user").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("limit.capacity");
        });
    }

    @Test
    void unknownPropertyInsideNamespaceFailsStartup() {
        runner.withPropertyValues("quotaflow.unknown-key=1").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("quotaflow.unknown-key");
        });
    }

    @Test
    void servletHandlerOnlyInServletApps() {
        new WebApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(QuotaFlowAutoConfiguration.class))
                .withPropertyValues(POLICY)
                .run(context -> assertThat(context)
                        .hasSingleBean(RateLimitExceptionHandler.class)
                        .doesNotHaveBean(RateLimitWebExceptionHandler.class));
    }

    @Test
    void reactiveHandlerOnlyInReactiveApps() {
        new ReactiveWebApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(QuotaFlowAutoConfiguration.class))
                .withPropertyValues(POLICY)
                .run(context -> assertThat(context)
                        .hasSingleBean(RateLimitWebExceptionHandler.class)
                        .doesNotHaveBean(RateLimitExceptionHandler.class));
    }

    @Configuration(proxyBeanMethods = false)
    static class UserStoreConfiguration {

        @Bean
        RateLimitStore customRateLimitStore() {
            return new LocalRateLimitStore();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class UserFacadeConfiguration {

        @Bean
        QuotaFlow customQuotaFlow() {
            PolicySet policySet = PolicySet.compile(List.of(RateLimitPolicy.builder("user-api")
                    .scope(Scope.USER)
                    .limit(new Limit(5, 5, Duration.ofSeconds(1)))
                    .build()));
            return DefaultQuotaFlow.builder(policySet, new LocalRateLimitStore()).build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MetricsConfiguration {

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }
}
