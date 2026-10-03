package io.quotaflow.verification.consumer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.RateLimitStore;
import io.quotaflow.spring.RateLimited;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

/** External consumer: real HTTP, AOP, SpEL and metrics using published POM/module metadata. */
@Configuration(proxyBeanMethods = false)
@EnableAutoConfiguration
@Import({CompatibilityApplication.ServletController.class, CompatibilityApplication.ReactiveController.class})
public class CompatibilityApplication {
    @Bean RateLimitStore localStore() { return new LocalRateLimitStore(); }
    @Bean MeterRegistry registry() { return new SimpleMeterRegistry(); }

    @RestController
    @ConditionalOnProperty(name="verification.stack", havingValue="servlet")
    public static class ServletController {
        @GetMapping("/quota/{identity}")
        @RateLimited(policy="consumer", key="#p0")
        public String limited(@PathVariable("identity") String identity) { return "ok"; }
    }
    @RestController
    @ConditionalOnProperty(name="verification.stack", havingValue="reactive")
    public static class ReactiveController {
        @GetMapping("/quota/{identity}")
        @RateLimited(policy="consumer", key="#p0")
        public Mono<String> limited(@PathVariable("identity") String identity) { return Mono.just("ok"); }
    }

    public static void main(String[] args) throws Exception {
        if (Runtime.version().feature() != Integer.getInteger("verification.jdk", 17)) throw new IllegalStateException("Unexpected consumer JDK");
        String expectedBoot = System.getProperty("verification.boot");
        if (!SpringBootVersion.getVersion().equals(expectedBoot)) throw new IllegalStateException("Unexpected resolved Spring Boot version");
        String redisUrl = System.getProperty("verification.redisUrl");
        if (redisUrl != null) {
            var redis = io.quotaflow.store.redis.RedisClientFactory.createClient(redisUrl, Duration.ofSeconds(5));
            try (var connection = redis.connect()) {
                if (!"PONG".equals(connection.sync().ping())) throw new AssertionError("Redis transport is unusable");
            } finally { redis.shutdown(); }
        }
        String stack = System.getProperty("verification.stack", "servlet");
        var application = new SpringApplication(CompatibilityApplication.class);
        application.setDefaultProperties(Map.of(
                "server.port", "0", "spring.main.web-application-type", stack,
                "verification.stack", stack,
                "quotaflow.policies.consumer.scope", "user",
                "quotaflow.policies.consumer.limit.capacity", "2",
                "quotaflow.policies.consumer.limit.refill-amount", "1",
                "quotaflow.policies.consumer.limit.refill-period", "PT1H"));
        try (var context = application.run(args)) {
            var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            String base = "http://localhost:" + context.getEnvironment().getRequiredProperty("local.server.port");
            for (int attempt = 0; attempt < 3; attempt++) {
                var request = HttpRequest.newBuilder(URI.create(base + "/quota/private-fixture-identity"))
                        .timeout(Duration.ofSeconds(5)).build();
                var response = client.send(request, HttpResponse.BodyHandlers.ofString());
                int expected = attempt < 2 ? 200 : 429;
                if (response.statusCode() != expected) throw new AssertionError("HTTP " + response.statusCode() + ", expected " + expected);
                if (expected == 429 && (!response.headers().firstValue("content-type").orElse("").contains("application/problem+json")
                        || response.headers().firstValue("retry-after").isEmpty()
                        || response.body().contains("private-fixture-identity"))) throw new AssertionError("Invalid rejection response: " + response.headers().map() + " body=" + response.body());
            }
            var flow = context.getBean(DefaultQuotaFlow.class);
            flow.flushObservations().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
            var meters = context.getBean(MeterRegistry.class);
            if (meters.get("quotaflow.decisions").tag("result", "allow").counter().count() != 2
                    || meters.get("quotaflow.decisions").tag("result", "reject").counter().count() != 1
                    || flow.observationFailures() != 0) throw new AssertionError("Incomplete observation delivery");
            System.out.println("CONSUMER VERIFIED boot=" + expectedBoot + " stack=" + stack + " jdk=" + Runtime.version() + " redisVerified=" + (redisUrl != null));
        }
    }
}
