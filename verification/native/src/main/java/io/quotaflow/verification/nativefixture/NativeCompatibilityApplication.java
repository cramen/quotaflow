package io.quotaflow.verification.nativefixture;

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

/** Executable native Spring certification for HTTP, AOP, SpEL, startup recovery and metrics. */
@Configuration(proxyBeanMethods = false)
@EnableAutoConfiguration
@Import({NativeCompatibilityApplication.ServletController.class, NativeCompatibilityApplication.ReactiveController.class})
public class NativeCompatibilityApplication {
    @Bean io.quotaflow.testing.RecoveryPrimaryFixture primary() {
        var policies = io.quotaflow.core.PolicySet.compile(java.util.List.of(io.quotaflow.core.RateLimitPolicy.builder("consumer")
                .scope(io.quotaflow.core.Scope.USER).limit(new io.quotaflow.core.Limit(2, 1, Duration.ofHours(1))).build()));
        var primary = new io.quotaflow.testing.RecoveryPrimaryFixture(policies);
        primary.available = false;
        primary.startGather = true;
        var accounting = new LocalRateLimitStore();
        primary.accounting = accounting::tryAcquireAll;
        return primary;
    }
    @Bean io.quotaflow.fallback.FallbackRateLimitStore recoveryStore(io.quotaflow.testing.RecoveryPrimaryFixture primary) {
        return new io.quotaflow.fallback.FallbackRateLimitStore(primary, io.quotaflow.fallback.RecoverySettings.single(), java.util.List.of());
    }
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
        String stack = System.getProperty("verification.stack", "servlet");
        var application = new SpringApplication(NativeCompatibilityApplication.class);
        application.setDefaultProperties(Map.of(
                "server.port", "0", "spring.main.web-application-type", stack,
                "verification.stack", stack,
                "quotaflow.policies.consumer.scope", "user",
                "quotaflow.policies.consumer.limit.capacity", "2",
                "quotaflow.policies.consumer.limit.refill-amount", "1",
                "quotaflow.policies.consumer.limit.refill-period", "PT1H"));
        try (var context = application.run(args)) {
            if (!"runtime".equals(System.getProperty("org.graalvm.nativeimage.imagecode"))) throw new IllegalStateException("Native executable required");
            var owner = context.getBean(io.quotaflow.fallback.FallbackRateLimitStore.class);
            var flow = context.getBean(DefaultQuotaFlow.class);
            if (owner.state() != io.quotaflow.fallback.DegradationState.OPEN) throw new AssertionError("Startup outage not exercised");
            if (flow.tryAcquire("consumer", io.quotaflow.core.RateLimitContext.builder()
                    .put(io.quotaflow.core.RateLimitContext.PRINCIPAL, "startup").build()).isAllowed())
                throw new AssertionError("Unvalidated startup granted credit");
            context.getBean(io.quotaflow.testing.RecoveryPrimaryFixture.class).available = true;
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (owner.state() != io.quotaflow.fallback.DegradationState.CLOSED && System.nanoTime() < deadline) Thread.sleep(10);
            if (owner.state() != io.quotaflow.fallback.DegradationState.CLOSED) throw new AssertionError("Native recovery did not complete");
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
            flow.flushObservations().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
            var meters = context.getBean(MeterRegistry.class);
            if (meters.get("quotaflow.decisions").tag("result", "allow").counter().count() != 2
                    || meters.get("quotaflow.decisions").tag("result", "reject").counter().count() != 2
                    || flow.observationFailures() != 0) throw new AssertionError("Incomplete observation delivery");
            System.out.println("NATIVE VERIFIED boot=" + SpringBootVersion.getVersion() + " stack=" + stack + " jdk=" + Runtime.version());
        }
    }
}
