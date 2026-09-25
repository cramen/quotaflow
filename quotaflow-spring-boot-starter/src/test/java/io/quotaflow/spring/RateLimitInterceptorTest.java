package io.quotaflow.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.Limit;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.Reaction;
import io.quotaflow.core.RateLimitPolicy;
import io.quotaflow.core.Scope;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class RateLimitInterceptorTest {

    private PolicySet policySet;
    private Service service;

    @BeforeEach
    void setUp() {
        policySet = PolicySet.compile(List.of(
                RateLimitPolicy.builder("user-api")
                        .scope(Scope.USER)
                        .limit(new Limit(1, 1, Duration.ofMinutes(1)))
                        .build(),
                RateLimitPolicy.builder("weighted-api")
                        .scope(Scope.USER)
                        .limit(new Limit(3, 3, Duration.ofMinutes(1)))
                        .build(),
                RateLimitPolicy.builder("throttled-api")
                        .scope(Scope.USER)
                        .reaction(Reaction.THROTTLE)
                        .limit(new Limit(1, 1, Duration.ofMillis(150)))
                        .build(),
                RateLimitPolicy.builder("fallback-api")
                        .scope(Scope.USER)
                        .limit(new Limit(1, 1, Duration.ofMinutes(1)))
                        .defaultKey("anonymous")
                        .build()));
        DefaultQuotaFlow quotaFlow = DefaultQuotaFlow.builder(policySet, new LocalRateLimitStore()).build();
        RateLimitInterceptor interceptor = new RateLimitInterceptor(quotaFlow, () -> policySet);
        ProxyFactory factory = new ProxyFactory(new Service());
        factory.addAdvisor(new RateLimitedAdvisor(interceptor));
        service = (Service) factory.getProxy();
        SecurityContextHolder.clearContext();
    }

    @Test
    void limitsUnderKeyEvaluatedFromArguments() {
        assertThat(service.call("alice")).isEqualTo("ok:alice");
        // capacity 1 per key: a shared bucket would reject bob's first call
        assertThat(service.call("bob")).isEqualTo("ok:bob");
        assertThatThrownBy(() -> service.call("alice")).isInstanceOf(RateLimitExceededException.class);
    }

    @Test
    void weightConsumesMultipleTokens() {
        assertThat(service.heavy("alice")).isEqualTo("ok");
        // capacity 3, weight 2: one token left, not enough for another weight-2 call
        assertThatThrownBy(() -> service.heavy("alice")).isInstanceOf(RateLimitExceededException.class);
    }

    @Test
    void unresolvableKeyRejectsWithoutTouchingTheStore() {
        assertThatThrownBy(() -> service.noKey())
                .isInstanceOf(RateLimitExceededException.class)
                .satisfies(e -> assertThat(((RateLimitExceededException) e).decision().retryAfter())
                        .isEmpty());
        // the rejection happened before any acquisition: alice's bucket is untouched
        assertThat(service.call("alice")).isEqualTo("ok:alice");
    }

    @Test
    void blankKeyResultCountsAsUnresolvable() {
        assertThatThrownBy(() -> service.call("  ")).isInstanceOf(RateLimitExceededException.class);
    }

    @Test
    void policyDefaultKeyAppliesWhenOptedIn() {
        assertThat(service.defaulted()).isEqualTo("ok");
        assertThatThrownBy(() -> service.defaulted()).isInstanceOf(RateLimitExceededException.class);
    }

    @Test
    void throttledCallWaitsForQuotaInsteadOfRejecting() {
        assertThat(service.throttled("alice")).isEqualTo("ok");
        long start = System.nanoTime();
        assertThat(service.throttled("alice")).isEqualTo("ok");
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMillis).isGreaterThanOrEqualTo(80);
    }

    @Test
    void reactiveMonoComposesDecisionWithoutBlockingAssembly() {
        Mono<String> first = service.reactive("alice");
        assertThat(first.block()).isEqualTo("ok:alice");
        assertThatThrownBy(() -> service.reactive("alice").block())
                .isInstanceOf(RateLimitExceededException.class);
    }

    @Test
    void reactiveFluxComposesDecision() {
        assertThat(service.reactiveMany("carol").collectList().block()).containsExactly("ok");
        assertThatThrownBy(() -> service.reactiveMany("carol").collectList().block())
                .isInstanceOf(RateLimitExceededException.class);
    }

    @Test
    void securityPrincipalSeedsUserScopeKey() {
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken("dave", "n/a", List.of()));
        try {
            assertThat(service.principalBased()).isEqualTo("ok");
            assertThatThrownBy(() -> service.principalBased())
                    .isInstanceOf(RateLimitExceededException.class);
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    @Test
    void noPrincipalAndNoKeyStillRejects() {
        assertThatThrownBy(() -> service.principalBased())
                .isInstanceOf(RateLimitExceededException.class);
    }

    static class Service {

        @RateLimited(policy = "user-api", key = "#userId")
        public String call(String userId) {
            return "ok:" + userId;
        }

        @RateLimited(policy = "weighted-api", key = "#userId", weight = "2")
        public String heavy(String userId) {
            return "ok";
        }

        @RateLimited(policy = "user-api")
        public String noKey() {
            return "ok";
        }

        @RateLimited(policy = "fallback-api", onMissingKey = OnMissingKey.USE_DEFAULT_KEY)
        public String defaulted() {
            return "ok";
        }

        @RateLimited(policy = "throttled-api", key = "#userId", waitTimeout = "2s")
        public String throttled(String userId) {
            return "ok";
        }

        @RateLimited(policy = "user-api", key = "#userId")
        public Mono<String> reactive(String userId) {
            return Mono.just("ok:" + userId);
        }

        @RateLimited(policy = "user-api", key = "#userId")
        public Flux<String> reactiveMany(String userId) {
            return Flux.just("ok");
        }

        @RateLimited(policy = "user-api", onMissingKey = OnMissingKey.USE_DEFAULT_KEY)
        public String principalBased() {
            return "ok";
        }
    }
}
