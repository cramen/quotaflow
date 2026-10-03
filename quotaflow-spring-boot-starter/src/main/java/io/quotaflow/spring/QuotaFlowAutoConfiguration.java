package io.quotaflow.spring;

import io.micrometer.core.instrument.MeterRegistry;
import io.quotaflow.config.CachingLimitResolver;
import io.quotaflow.config.ConfigReloader;
import io.quotaflow.config.ConfigurationParser;
import io.quotaflow.config.QuotaFlowConfiguration;
import io.quotaflow.core.DecisionListener;
import io.quotaflow.core.KeyResolvers;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.LimitResolver;
import io.quotaflow.core.QuotaFlow;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.RateLimitStore;
import io.quotaflow.core.store.StateSeeder;
import io.quotaflow.fallback.DegradationListener;
import io.quotaflow.fallback.FallbackConfig;
import io.quotaflow.fallback.CoordinatedFallbackStore;
import io.quotaflow.fallback.ReconnectingRecoveryPrimary;
import io.quotaflow.fallback.RecoverySettings;
import io.quotaflow.micrometer.MicrometerDecisionListener;
import io.quotaflow.micrometer.MicrometerDegradationListener;
import io.quotaflow.micrometer.MicrometerThrottleMetrics;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.config.AopConfigUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Role;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.ClassUtils;

/**
 * Assembles the full rate limiting stack from the QuotaFlow modules: a Redis
 * store when Lettuce is on the classpath and reachable (local-only mode with a
 * warning otherwise), the conservative fallback wrapper always, the
 * {@link DefaultQuotaFlow} facade over the compiled policy set, a hot-reload
 * pipeline re-reading the Spring {@link ConfigurableEnvironment}, the
 * {@link RateLimited} annotation machinery, stack-specific 429 handlers, and
 * Micrometer bridges when a {@link MeterRegistry} bean exists.
 *
 * <p>Everything is gated by {@code quotaflow.enabled} (default true) and every
 * bean backs off for a user-defined bean of the same type: declare an own
 * {@link RateLimitStore}, {@link QuotaFlow}, {@link ConfigReloader},
 * {@link RateLimitInterceptor} or handler bean to replace that layer.
 */
@AutoConfiguration(
        beforeName = "org.springframework.boot.autoconfigure.aop.AopAutoConfiguration",
        afterName = {
            "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration",
            "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration"
        })
@ConditionalOnProperty(name = "quotaflow.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(QuotaFlowProperties.class)
public class QuotaFlowAutoConfiguration implements org.springframework.beans.factory.BeanClassLoaderAware {

    private static final Logger log = LoggerFactory.getLogger(QuotaFlowAutoConfiguration.class);

    private ClassLoader beanClassLoader = QuotaFlowAutoConfiguration.class.getClassLoader();
    @Override public void setBeanClassLoader(ClassLoader loader) { beanClassLoader = loader; }

    private final PolicySetReference policySets = new PolicySetReference();

    /** Compiled startup configuration; invalid policies fail startup here with the standard errors. */
    @Bean
    @ConditionalOnMissingBean
    QuotaFlowConfiguration quotaFlowConfiguration(QuotaFlowProperties properties) {
        QuotaFlowConfiguration configuration =
                ConfigurationParser.parse(properties.toConfigurationMap());
        policySets.set(configuration.policySet());
        return configuration;
    }

    /** Owned transport is reconnectable; an unenrolled startup never receives quota credit. */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(RateLimitStore.class)
    ReconnectingRecoveryPrimary quotaFlowPrimaryStore(QuotaFlowProperties properties) {
        boolean driverPresent = ClassUtils.isPresent("io.lettuce.core.RedisClient", beanClassLoader);
        if (!driverPresent && properties.isFailOnRedisMissing())
            throw new IllegalStateException("Redis driver is absent and quotaflow.fail-on-redis-missing=true");
        var capturedEndpoint = new QuotaFlowProperties.Redis();
        capturedEndpoint.setUrl(properties.getRedis().getUrl());
        capturedEndpoint.setConnectTimeout(properties.getRedis().getConnectTimeout());
        capturedEndpoint.setCommandTimeout(properties.getRedis().getCommandTimeout());
        capturedEndpoint.setBusinessTimeout(properties.getRedis().getBusinessTimeout());
        String capturedNamespace = properties.getNamespace();
        ReconnectingRecoveryPrimary primary = new ReconnectingRecoveryPrimary(() -> {
            if (!driverPresent) throw new io.quotaflow.core.store.PrimaryDispatchException(
                    io.quotaflow.core.store.PrimaryDispatchException.Outcome.NOT_DISPATCHED);
            return RedisStoreFactory.recoveryConnection(capturedEndpoint, capturedNamespace);
        }, capturedEndpoint.getConnectTimeout());
        if (properties.isFailOnRedisMissing()) {
            try { primary.probe().toCompletableFuture().join(); }
            catch (RuntimeException failure) {
                primary.close();
                // Endpoint URLs and nested driver failures may contain credentials.
                throw new IllegalStateException("Redis is unavailable and quotaflow.fail-on-redis-missing=true at "
                        + io.quotaflow.store.redis.SafeRedisDiagnostics.endpoint(capturedEndpoint.getUrl()) + "; verify credentials, TLS and driver configuration");
            }
        }
        return primary;
    }

    /** Fixed-cohort conservative recovery, including provisional startup with zero credit. */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(RateLimitStore.class)
    CoordinatedFallbackStore rateLimitStore(
            ReconnectingRecoveryPrimary primary, QuotaFlowProperties properties, QuotaFlowConfiguration configuration,
            ObjectProvider<DegradationListener> degradationListeners,
            ObjectProvider<MeterRegistry> meterRegistry) {
        List<DegradationListener> listeners = new ArrayList<>(degradationListeners.orderedStream().toList());
        meterRegistry.ifAvailable(registry -> listeners.add(new MicrometerDegradationListener(registry)));
        var accounting = configuration.accounting();
        QuotaFlowProperties.Fallback fallback = properties.getFallback();
        RecoverySettings settings = new RecoverySettings(accounting.namespace(), accounting.deploymentId(),
                accounting.instanceId(), accounting.cohort(), fallback.getMaxSeedEntries(), 4096,
                fallback.getOpenDuration(), properties.getRedis().getBusinessTimeout());
        return new CoordinatedFallbackStore(primary, settings, listeners);
    }

    /**
     * The limiter facade. Registered {@link DecisionListener} beans are
     * attached, plus the Micrometer bridge when a registry exists; the
     * well-known named key resolvers are available to policies; a user
     * {@link LimitResolver} bean enables dynamic limit references (cached with
     * the configured TTL).
     */
    @Bean
    @ConditionalOnMissingBean(QuotaFlow.class)
    DefaultQuotaFlow quotaFlow(
            QuotaFlowConfiguration configuration, RateLimitStore store, QuotaFlowProperties properties,
            ObjectProvider<DecisionListener> decisionListeners,
            ObjectProvider<io.quotaflow.core.WaitListener> waitListeners,
            ObjectProvider<io.quotaflow.core.observation.ObservationListener> observationListeners,
            ObjectProvider<LimitResolver> limitResolvers) {
        policySets.initialize(configuration.policySet());
        DefaultQuotaFlow.Builder builder = DefaultQuotaFlow.builder(configuration.policySet(), store)
                .namespace(properties.getNamespace())
                .maxWaitersPerPolicy(properties.getMaxWaitersPerPolicy())
                .addResolver(KeyResolvers.PRINCIPAL_ID, KeyResolvers.principal())
                .addResolver(KeyResolvers.TENANT_ID_ID, KeyResolvers.tenantId())
                .addResolver(KeyResolvers.API_KEY_ID, KeyResolvers.apiKey());
        decisionListeners.orderedStream().forEach(builder::addListener);
        waitListeners.orderedStream().forEach(builder::addWaitListener);
        observationListeners.orderedStream().forEach(builder::addObservationListener);
        limitResolvers.ifAvailable(builder::limitResolver);
        return builder.build();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean(MicrometerDecisionListener.class)
    MicrometerDecisionListener quotaFlowDecisionMetrics(MeterRegistry registry) {
        return new MicrometerDecisionListener(registry);
    }

    /** TTL-caching wrapper for a user-provided tariff resolver. */
    @Bean
    @Primary
    @ConditionalOnBean(LimitResolver.class)
    @ConditionalOnMissingBean
    CachingLimitResolver quotaFlowCachingLimitResolver(
            LimitResolver delegate, QuotaFlowProperties properties) {
        return CachingLimitResolver.wrap(delegate, properties.getReload().getResolverTtl());
    }

    /** Throttle queue depth gauges, kept in sync with every applied reload. */
    @Bean
    @ConditionalOnBean({MeterRegistry.class, DefaultQuotaFlow.class})
    @ConditionalOnMissingBean
    MicrometerThrottleMetrics quotaFlowThrottleMetrics(MeterRegistry registry, DefaultQuotaFlow quotaFlow) {
        return new MicrometerThrottleMetrics(registry, quotaFlow, policySets);
    }

    /** Hot-reload pipeline re-reading the Spring environment on watch and on {@code reload()}. */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnBean(DefaultQuotaFlow.class)
    @ConditionalOnMissingBean
    ConfigReloader quotaFlowConfigReloader(
            ConfigurableEnvironment environment, DefaultQuotaFlow quotaFlow, QuotaFlowProperties properties,
            QuotaFlowConfiguration configuration,
            ObjectProvider<CachingLimitResolver> cachingResolver,
            ObjectProvider<MicrometerThrottleMetrics> throttleMetrics) {
        EnvironmentConfigSource source = new EnvironmentConfigSource(environment);
        return ConfigReloader.builder(source, quotaFlow)
                .startupAccounting(configuration.accounting())
                .pollInterval(properties.getReload().getPollInterval())
                .onApplied(() -> policySets.set(quotaFlow.policySet()))
                .onApplied(() -> cachingResolver.ifAvailable(CachingLimitResolver::clear))
                .onApplied(() -> throttleMetrics.ifAvailable(MicrometerThrottleMetrics::sync))
                .build();
    }

    /** Annotation interceptor; replace with an own bean to customize context seeding. */
    @Bean
    @ConditionalOnMissingBean
    RateLimitInterceptor rateLimitInterceptor(QuotaFlow quotaFlow, QuotaFlowConfiguration configuration) {
        policySets.initialize(configuration.policySet());
        return new RateLimitInterceptor(quotaFlow, policySets);
    }

    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    @ConditionalOnBean(RateLimitInterceptor.class)
    @ConditionalOnMissingBean
    RateLimitedAdvisor rateLimitedAdvisor(ObjectProvider<RateLimitInterceptor> interceptor) {
        return RateLimitedAdvisor.deferred(interceptor::getObject);
    }

    /**
     * Registers the infrastructure auto-proxy creator through the canonical
     * {@link AopConfigUtils} utility, so {@link RateLimited} methods are
     * proxied even in applications with no AOP setup of their own (plain
     * Spring AOP, no AspectJ weaver required). An application enabling
     * {@code @EnableAspectJAutoProxy} (or Boot's AOP auto-configuration with
     * the AspectJ weaver present) escalates the registration to the
     * AspectJ-aware creator instead of double-proxying.
     */
    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    static BeanDefinitionRegistryPostProcessor rateLimitingAutoProxyCreatorRegistrar() {
        return new BeanDefinitionRegistryPostProcessor() {
            @Override
            public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
                AopConfigUtils.registerAutoProxyCreatorIfNecessary(registry);
            }

            @Override
            public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
            }
        };
    }

    /** Default 429 mapping for servlet applications. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    static class ServletRateLimitHandlingConfiguration {

        @Bean
        @ConditionalOnMissingBean
        RateLimitExceptionHandler rateLimitExceptionHandler() {
            return new RateLimitExceptionHandler();
        }
    }

    /** Default 429 mapping for reactive applications. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
    static class ReactiveRateLimitHandlingConfiguration {

        @Bean
        @ConditionalOnMissingBean
        RateLimitWebExceptionHandler rateLimitWebExceptionHandler() {
            return new RateLimitWebExceptionHandler();
        }
    }
}
