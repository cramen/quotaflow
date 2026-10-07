package io.quotaflow.verification.published;

import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.config.ConfigurationParser;
import io.quotaflow.fallback.*;
import io.quotaflow.store.redis.*;
import io.quotaflow.spring.RateLimited;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.web.bind.annotation.*;

/** Executable harness: annotation and configuration placeholders come from README. */
@Configuration(proxyBeanMethods=false)
@EnableAutoConfiguration
@Import(ReadmeApplication.Chat.class)
public class ReadmeApplication {
    static final AtomicLong clock=new AtomicLong();
    static final String TENANT="example-secret-tenant";
    @Bean @ConditionalOnProperty(name="verification.local",havingValue="true",matchIfMissing=true)
    RateLimitStore localStore() { return new LocalRateLimitStore(clock::get); }
    @Bean MeterRegistry meters() { return new SimpleMeterRegistry(); }
    public record Prompt(String text) {}
    public record Answer(String text) {}
    @RestController public static class Chat {
        @PostMapping("/chat/{tenantId}")
        __REJECT_ANNOTATION__
        public CompletionStage<Answer> chat(@PathVariable("tenantId") String tenantId,@RequestBody Prompt prompt) {
            return CompletableFuture.completedFuture(new Answer(prompt.text()));
        }
        __THROTTLE_ANNOTATION__
        public CompletionStage<Answer> queued(String tenantId,Prompt prompt) {
            return CompletableFuture.completedFuture(new Answer(prompt.text()));
        }
    }
    static RateLimitContext tenant(String id) { return RateLimitContext.builder().put(RateLimitContext.TENANT_ID,id).build(); }
    static ConfigurableApplicationContext start(Map<String,String> properties) {
        var app=new SpringApplication(ReadmeApplication.class);
        Map<String,Object> settings=new HashMap<>(properties);
        settings.put("server.port","0"); settings.put("spring.main.web-application-type","servlet");
        settings.put("server.address","127.0.0.1");
        settings.put("logging.level.io.lettuce.core.protocol","INFO");
        app.setDefaultProperties(settings); return app.run();
    }
    static void rejected(java.util.function.Supplier<CompletionStage<?>> call) {
        try { call.get().toCompletableFuture().join(); throw new AssertionError("Exhausted request was allowed"); }
        catch (io.quotaflow.spring.RateLimitExceededException expected) { }
        catch (CompletionException failure) {
            if (!(failure.getCause() instanceof io.quotaflow.spring.RateLimitExceededException)) throw failure;
        }
    }
    static void awaitState(CoordinatedFallbackStore store,DegradationState wanted) throws Exception {
        long end=System.nanoTime()+Duration.ofSeconds(20).toNanos();
        while (store.state()!=wanted && System.nanoTime()<end) Thread.sleep(25);
        if (store.state()!=wanted) throw new AssertionError("Recovery state did not reach "+wanted);
    }
    static void refusedNamespace(Map<String,String> props) throws Exception {
        try (var context=start(props)) {
            Thread.sleep(1500);
            var store=context.getBean(CoordinatedFallbackStore.class);
            if (store.state()==DegradationState.CLOSED || context.getBean(DefaultQuotaFlow.class)
                    .tryAcquire("tenant-gold",tenant(TENANT)).isAllowed()) throw new AssertionError("Unready or conflicting namespace granted quota");
        } catch (PolicyConfigurationException explicitFailure) {
            // An explicit configuration refusal also satisfies the startup contract.
        } catch (org.springframework.beans.factory.BeanCreationException failure) {
            if (!ConsumerRecovery.configurationRefusal(failure)) throw failure;
        }
    }
    static void outage(Map<String,String> props) throws Exception {
        props.put("verification.local","false");
        props.put("quotaflow.namespace",System.getProperty("verification.namespace"));
        props.put("quotaflow.redis.url",System.getProperty("verification.redisUrl"));
        var policies=ConfigurationParser.parse(props).policySet();
        String namespace=props.get("quotaflow.namespace");
        props.put("quotaflow.redis.command-timeout","PT0.1S");
        props.put("quotaflow.redis.business-timeout","PT0.5S");
        props.put("quotaflow.fallback.open-duration","PT0.05S");
        refusedNamespace(props);
        var cohort=RecoveryCohort.single();
        var client=RedisClientFactory.createClient(props.get("quotaflow.redis.url"),Duration.ofSeconds(2));
        try (var connection=client.connect(); var adminStore=new RedisRateLimitStore(connection,RedisStoreConfig.defaults())) {
            new RedisNamespaceAdmin(connection).provisionFresh(namespace,true,4096);
            var bindings=policies.policies().stream().map(policy -> new PolicyBinding(new QuotaDomain(namespace,
                    policies.rootPolicyId(policy.id())),policy.id(),policy.scope(),policy.algorithm())).toList();
            adminStore.registerPolicies(bindings).toCompletableFuture().join();
            var admin=new RedisRecoveryController(connection,Duration.ofSeconds(2));
            admin.provisionCohort(namespace,cohort,"initial",true,true).toCompletableFuture().join();
            for (var domain:bindings.stream().map(PolicyBinding::domain).distinct().toList())
                admin.provisionDomain(domain,cohort,"initial",policies.recoveryFingerprint(domain.rootPolicyId()),true,true).toCompletableFuture().join();
        } finally { client.shutdown(); }
        try (var context=start(props)) {
            var store=context.getBean(CoordinatedFallbackStore.class); var flow=context.getBean(DefaultQuotaFlow.class);
            awaitState(store,DegradationState.CLOSED); Thread.sleep(100);
            if (!flow.tryAcquire("tenant-gold",tenant(TENANT)).isAllowed()) throw new AssertionError("Provisioned namespace remained unusable");
            refusedNamespace(props);
            if (store.state()!=DegradationState.CLOSED) throw new AssertionError("Duplicate owner displaced active owner");
            var input=new BufferedReader(new InputStreamReader(System.in));
            System.out.println("OUTAGE_READY"); System.out.flush();
            if (!"continue".equals(input.readLine())) throw new AssertionError("Missing outage control");
            flow.tryAcquire("tenant-gold",tenant(TENANT)); awaitState(store,DegradationState.OPEN);
            flow.flushObservations().toCompletableFuture().get(5,TimeUnit.SECONDS);
            if (context.getBean(MeterRegistry.class).get("quotaflow.degraded").gauge().value()!=1) throw new AssertionError("Degradation is not observable");
            System.out.println("RECOVERY_READY"); System.out.flush();
            if (!"continue".equals(input.readLine())) throw new AssertionError("Missing recovery control");
            awaitState(store,DegradationState.CLOSED); Thread.sleep(100);
            if (!flow.tryAcquire("tenant-gold",tenant(TENANT)).isAllowed()) throw new AssertionError("Automatic recovery failed");
        }
    }
    public static void main(String[] args) throws Exception {
        if (Runtime.version().feature()!=Integer.getInteger("verification.jdk")) throw new AssertionError("Wrong example JDK");
        if (!Chat.class.getMethod("chat",String.class,Prompt.class).getParameters()[0].isNamePresent()) throw new AssertionError("Named key requires -parameters");
        Properties loaded=new Properties(); try (var reader=Files.newBufferedReader(Path.of(System.getProperty("verification.properties")))) { loaded.load(reader); }
        Map<String,String> props=new HashMap<>(); loaded.forEach((k,v)->props.put(k.toString(),v.toString()));
        String scenario=System.getProperty("verification.case");
        if (scenario.equals("outage-recovery")) outage(props);
        else {
            if (scenario.equals("throttle")) props.put("quotaflow.policies.tenant-gold.reaction","__THROTTLE_REACTION__");
            try (var context=start(props)) {
                var flow=context.getBean(DefaultQuotaFlow.class); var chat=context.getBean(Chat.class);
                long capacity=Long.parseLong(props.get("quotaflow.policies.tenant-gold.limit.capacity"));
                if (!flow.tryAcquire("tenant-gold",tenant(TENANT),capacity).isAllowed()) throw new AssertionError("README hierarchy failed");
                switch (scenario) {
                    case "reject" -> rejected(()->chat.chat(TENANT,new Prompt("test")));
                    case "keys" -> { chat.chat("other-tenant",new Prompt("test")).toCompletableFuture().get(); rejected(()->chat.chat(TENANT,new Prompt("test"))); }
                    case "throttle" -> {
                        var future=CompletableFuture.supplyAsync(()->chat.queued(TENANT,new Prompt("test")).toCompletableFuture().join());
                        long deadline=System.nanoTime()+Duration.ofSeconds(1).toNanos();
                        while (flow.waitQueueDepth("tenant-gold")==0 && System.nanoTime()<deadline) Thread.sleep(5);
                        if (flow.waitQueueDepth("tenant-gold")!=1) throw new AssertionError("Throttle did not enter the queue");
                        if (future.isDone()) throw new AssertionError("Throttle did not wait");
                        clock.addAndGet(Duration.ofSeconds(1).toNanos()); future.get(3,TimeUnit.SECONDS);
                    }
                    case "http-429" -> {
                        String url="http://127.0.0.1:"+context.getEnvironment().getRequiredProperty("local.server.port")+"/chat/"+TENANT;
                        var request=HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5))
                                .header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString("{\"text\":\"test\"}")).build();
                        var response=HttpClient.newHttpClient().send(request,HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode()!=429 || response.headers().firstValue("Retry-After").isEmpty()
                                || !response.headers().firstValue("Content-Type").orElse("").contains("application/problem+json")
                                || response.body().contains(TENANT)) throw new AssertionError("README rejection HTTP contract failed");
                    }
                    default -> throw new AssertionError("Unknown example");
                }
            }
        }
        System.out.println("PUBLISHED EXAMPLES VERIFIED case="+scenario);
    }
}
