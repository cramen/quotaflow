package io.quotaflow.spring;

import io.quotaflow.core.*;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.*;
import reactor.core.publisher.Mono;
import static org.junit.jupiter.api.Assertions.*;

class KeyPrecedenceTest {
    static class Service {
        @RateLimited(policy="global") public String global() { return "ok"; }
        @RateLimited(policy="global") public Mono<String> reactiveGlobal() { return Mono.just("ok"); }
        @RateLimited(policy="tenant") public String tenant() { return "ok"; }
        @RateLimited(policy="tenant") public Mono<String> reactiveTenant() { return Mono.just("ok"); }
        @RateLimited(policy="key") public String key() { return "ok"; }
        @RateLimited(policy="key") public Mono<String> reactiveKey() { return Mono.just("ok"); }
        @RateLimited(policy="user", key="null", onMissingKey=OnMissingKey.USE_DEFAULT_KEY) public String defaultUser() { return "ok"; }
        @RateLimited(policy="user", key="null", onMissingKey=OnMissingKey.USE_DEFAULT_KEY) public Mono<String> reactiveDefaultUser() { return Mono.just("ok"); }
        @RateLimited(policy="user", key="'explicit'") public Mono<String> explicitUser() { return Mono.just("ok"); }
        @RateLimited(policy="user", key="#[") public Mono<String> syntaxError() { return Mono.just("bad"); }
    }
    @Test void bothStacksRespectScopeAndExplicitDefaultRatherThanSubstitutingAuthentication() {
        var policies=PolicySet.compile(List.of(policy("global",Scope.GLOBAL),policy("tenant",Scope.TENANT),policy("key",Scope.KEY),policy("user",Scope.USER)));
        try(var flow=DefaultQuotaFlow.builder(policies,new LocalRateLimitStore()).build()) {
            var factory=new ProxyFactory(new Service());factory.addAdvisor(new RateLimitedAdvisor(new RateLimitInterceptor(flow,()->policies)));
            var service=(Service)factory.getProxy();
            var auth=new UsernamePasswordAuthenticationToken("alice","unused",List.of());
            SecurityContextHolder.getContext().setAuthentication(auth);
            try {
                assertEquals("ok",service.global());
                assertEquals("ok",service.reactiveGlobal().block());
                assertThrows(RateLimitExceededException.class,service::tenant);
                assertThrows(RateLimitExceededException.class,service::key);
                assertThrows(RateLimitExceededException.class,()->service.reactiveTenant().contextWrite(ReactiveSecurityContextHolder.withAuthentication(auth)).block());
                assertThrows(RateLimitExceededException.class,()->service.reactiveKey().contextWrite(ReactiveSecurityContextHolder.withAuthentication(auth)).block());
                assertEquals("ok",service.defaultUser());
                assertEquals("ok",service.reactiveDefaultUser().contextWrite(ReactiveSecurityContextHolder.withAuthentication(auth)).block());
                // Both explicit nulls debit the same configured default, leaving Alice untouched.
                assertFalse(flow.tryAcquire("user",RateLimitContext.empty()).isAllowed());
                assertTrue(flow.tryAcquire("user",RateLimitContext.builder().put(RateLimitContext.PRINCIPAL,"alice").build()).isAllowed());
                assertEquals("ok",service.explicitUser().block());
                assertThrows(org.springframework.expression.ParseException.class,()->service.syntaxError().block());
            } finally { SecurityContextHolder.clearContext(); }
        }
    }
    private static RateLimitPolicy policy(String id,Scope scope) {
        return RateLimitPolicy.builder(id).scope(scope).defaultKey("shared")
                .limit(new Limit(2,1,Duration.ofHours(1))).build();
    }
}
