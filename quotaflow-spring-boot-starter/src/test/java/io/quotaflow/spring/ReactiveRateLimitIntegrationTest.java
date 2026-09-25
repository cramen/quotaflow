package io.quotaflow.spring;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.boot.web.reactive.context.AnnotationConfigReactiveWebApplicationContext;
import reactor.core.publisher.Mono;

/**
 * WebFlux end-to-end over an in-memory reactive application context (a real
 * reactive context is required for the reactive condition to match; with both
 * web stacks on the test classpath a {@code @SpringBootTest} web environment
 * would silently pick the servlet stack). {@link WebTestClient} drives the
 * full {@code DispatcherHandler} chain, including the auto-configured
 * {@link RateLimitWebExceptionHandler}.
 */
class ReactiveRateLimitIntegrationTest {

    private static final String[] PROPERTIES = {
        "quotaflow.policies.user-api.scope=user",
        "quotaflow.policies.user-api.limit.capacity=2",
        "quotaflow.policies.user-api.limit.refill-amount=2",
        "quotaflow.policies.user-api.limit.refill-period=PT1M",
        "quotaflow.policies.throttled-api.scope=user",
        "quotaflow.policies.throttled-api.reaction=throttle",
        "quotaflow.policies.throttled-api.limit.capacity=1",
        "quotaflow.policies.throttled-api.limit.refill-amount=1",
        "quotaflow.policies.throttled-api.limit.refill-period=PT0.15S",
        "quotaflow.policies.slow-api.scope=user",
        "quotaflow.policies.slow-api.reaction=throttle",
        "quotaflow.policies.slow-api.limit.capacity=1",
        "quotaflow.policies.slow-api.limit.refill-amount=1",
        "quotaflow.policies.slow-api.limit.refill-period=PT30S",
        "quotaflow.redis.url=redis://localhost:6390",
        "quotaflow.redis.connect-timeout=100ms"
    };

    private AnnotationConfigReactiveWebApplicationContext context;
    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        context = new AnnotationConfigReactiveWebApplicationContext();
        context.register(TestApplication.class);
        TestPropertyValues.of(PROPERTIES).applyTo(context);
        context.refresh();
        webTestClient = WebTestClient.bindToApplicationContext(context).build();
    }

    @AfterEach
    void tearDown() {
        context.close();
    }

    @Test
    void reactiveHandlerIsTheActiveMapping() {
        assertThat(context.getBean(RateLimitWebExceptionHandler.class)).isNotNull();
        assertThat(context.getBeanNamesForType(RateLimitExceptionHandler.class)).isEmpty();
    }

    @Test
    void allowsWithinQuota() {
        webTestClient.get().uri("/api/alice")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo("hello alice");
    }

    @Test
    void rejectsWith429SemanticsWhenQuotaExhausted() {
        webTestClient.get().uri("/api/frank").exchange().expectStatus().isOk();
        webTestClient.get().uri("/api/frank").exchange().expectStatus().isOk();

        webTestClient.get().uri("/api/frank")
                .exchange()
                .expectStatus().isEqualTo(429)
                .expectHeader().exists("Retry-After")
                .expectHeader().value("Retry-After", value -> assertThat(value)
                        .matches("[1-9][0-9]*"))
                .expectHeader().contentType("application/problem+json")
                .expectBody()
                .jsonPath("$.type").isEqualTo("urn:quotaflow:rate-limit-exceeded")
                .jsonPath("$.title").isEqualTo("Rate limit exceeded")
                .jsonPath("$.status").isEqualTo(429)
                .jsonPath("$.detail").value(detail -> assertThat((String) detail)
                        .contains("user-api")
                        .contains("user")
                        .doesNotContain("frank"));
    }

    @Test
    void quotasAreTrackedPerKey() {
        webTestClient.get().uri("/api/grace").exchange().expectStatus().isOk();
        webTestClient.get().uri("/api/grace").exchange().expectStatus().isOk();
        webTestClient.get().uri("/api/grace").exchange().expectStatus().isEqualTo(429);

        webTestClient.get().uri("/api/heidi").exchange().expectStatus().isOk();
    }

    @Test
    void throttledCallWaitsWithoutBlockingAndSucceeds() {
        webTestClient.get().uri("/throttled/ivan").exchange().expectStatus().isOk();
        // the throttle wait composes into the reactive chain; the event loop is never parked
        webTestClient.get().uri("/throttled/ivan")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo("hello ivan");
    }

    @Test
    void waitTimeoutRejectionHasDistinctProblemType() {
        webTestClient.get().uri("/slow/judy").exchange().expectStatus().isOk();

        webTestClient.get().uri("/slow/judy")
                .exchange()
                .expectStatus().isEqualTo(429)
                .expectHeader().exists("Retry-After")
                .expectHeader().contentType("application/problem+json")
                .expectBody()
                .jsonPath("$.type").isEqualTo("urn:quotaflow:rate-limit-wait-timeout")
                .jsonPath("$.title").isEqualTo("Rate limit wait timeout")
                .jsonPath("$.detail").value(detail -> assertThat((String) detail)
                        .contains("slow-api")
                        .doesNotContain("judy"));
    }

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    static class TestApplication {

        @RestController
        static class LimitedController {

            @GetMapping("/api/{user}")
            @RateLimited(policy = "user-api", key = "#user")
            public Mono<String> limited(@PathVariable String user) {
                return Mono.just("hello " + user);
            }

            @GetMapping("/throttled/{user}")
            @RateLimited(policy = "throttled-api", key = "#user", waitTimeout = "2s")
            public Mono<String> throttled(@PathVariable String user) {
                return Mono.just("hello " + user);
            }

            @GetMapping("/slow/{user}")
            @RateLimited(policy = "slow-api", key = "#user", waitTimeout = "50ms")
            public Mono<String> slow(@PathVariable String user) {
                return Mono.just("hello " + user);
            }
        }
    }
}
