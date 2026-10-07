package io.quotaflow.verification.published;

import io.quotaflow.config.ConfigurationParser;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.fallback.*;
import io.quotaflow.store.redis.*;
import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

/** Fresh, single-owner distributed setup for each external starter execution. */
public final class ConsumerRecovery {
    public static Map<String,String> properties(String url,String namespace,String stack) {
        Map<String,String> values=new LinkedHashMap<>();
        values.put("server.port","0");values.put("server.address","127.0.0.1");
        values.put("spring.main.web-application-type",stack);values.put("verification.stack",stack);
        values.put("logging.level.io.lettuce.core.protocol","INFO");
        values.put("quotaflow.redis.url",url);values.put("quotaflow.namespace",namespace);
        values.put("quotaflow.defaults.expected-instances","1");values.put("quotaflow.recovery.deployment-id","fixture");
        values.put("quotaflow.recovery.members","single");values.put("quotaflow.recovery.instance-id","single");
        values.put("quotaflow.policies.consumer.scope","user");values.put("quotaflow.policies.consumer.limit.capacity","2");
        values.put("quotaflow.policies.consumer.limit.refill-amount","1");values.put("quotaflow.policies.consumer.limit.refill-period","PT1H");
        return values;
    }
    public static ConfigurableApplicationContext start(Class<?> application,Map<String,String> settings) {
        Map<String,String> values=new LinkedHashMap<>(settings);
        values.put("quotaflow.redis.command-timeout","PT0.1S");values.put("quotaflow.redis.business-timeout","PT0.5S");
        values.put("quotaflow.fallback.open-duration","PT0.05S");
        return new SpringApplication(application).run(values.entrySet().stream().map(e->"--"+e.getKey()+"="+e.getValue()).toArray(String[]::new));
    }
    public static ConfigurableApplicationContext startProbe(Class<?> application,Map<String,String> settings) {
        var probe=new LinkedHashMap<>(settings);probe.put("spring.main.web-application-type","none");
        return start(application,probe);
    }
    public static void refused(Supplier<ConfigurableApplicationContext> start) throws Exception {
        try (var context=start.get()) {
            Thread.sleep(1000);
            if (context.getBean(CoordinatedFallbackStore.class).state()==DegradationState.CLOSED
                    || context.getBean(DefaultQuotaFlow.class).tryAcquire("consumer",RateLimitContext.builder()
                        .put(RateLimitContext.PRINCIPAL,"startup-check").build()).isAllowed())
                throw new AssertionError("Unready or duplicate owner granted quota");
        } catch (PolicyConfigurationException explicitRefusal) { }
        catch (org.springframework.beans.factory.BeanCreationException failure) {
            if (!configurationRefusal(failure)) throw failure;
        }
    }
    public static boolean configurationRefusal(Throwable failure) {
        for (int depth=0;failure!=null && depth<32;depth++,failure=failure.getCause())
            if (failure instanceof PolicyConfigurationException) return true;
        return false;
    }
    public static void provision(Map<String,String> settings) {
        var config=ConfigurationParser.parse(settings);var policies=config.policySet();String namespace=config.accounting().namespace();
        var cohort=config.accounting().cohort();var client=RedisClientFactory.createClient(settings.get("quotaflow.redis.url"),Duration.ofSeconds(2));
        try (var connection=client.connect();var store=new RedisRateLimitStore(connection,RedisStoreConfig.defaults())) {
            new RedisNamespaceAdmin(connection).provisionFresh(namespace,true,4096);
            var bindings=policies.policies().stream().map(p->new PolicyBinding(new QuotaDomain(namespace,policies.rootPolicyId(p.id())),p.id(),p.scope(),p.algorithm())).toList();
            store.registerPolicies(bindings).toCompletableFuture().join();
            var admin=new RedisRecoveryController(connection,Duration.ofSeconds(2));
            admin.provisionCohort(namespace,cohort,"initial",true,true).toCompletableFuture().join();
            for (var domain:bindings.stream().map(PolicyBinding::domain).distinct().toList())
                admin.provisionDomain(domain,cohort,"initial",policies.recoveryFingerprint(domain.rootPolicyId()),true,true).toCompletableFuture().join();
        } finally {client.shutdown();}
    }
    public static void healthy(ConfigurableApplicationContext context) throws Exception {
        var store=context.getBean(CoordinatedFallbackStore.class);long end=System.nanoTime()+Duration.ofSeconds(20).toNanos();
        while (store.state()!=DegradationState.CLOSED && System.nanoTime()<end) Thread.sleep(20);
        if (store.state()!=DegradationState.CLOSED) throw new AssertionError("Default distributed starter did not recover");
    }
}
