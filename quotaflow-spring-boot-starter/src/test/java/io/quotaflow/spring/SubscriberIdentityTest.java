package io.quotaflow.spring;

import io.quotaflow.core.*;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.security.authentication.*;
import org.springframework.security.core.context.*;
import reactor.core.publisher.*;
import static org.junit.jupiter.api.Assertions.*;

class SubscriberIdentityTest {
    static class Service {
        final AtomicInteger calls = new AtomicInteger();
        @RateLimited(policy="user") public Mono<String> omitted() { calls.incrementAndGet(); return Mono.just("ok"); }
        @RateLimited(policy="user",key="#authentication.name") public Flux<String> explicit() { calls.incrementAndGet(); return Flux.just("ok"); }
        @RateLimited(policy="user",key="null") public Mono<String> missing() { calls.incrementAndGet(); return Mono.just("bad"); }
        @RateLimited(policy="user") public Mono<String> retryable() { return calls.incrementAndGet() == 1 ? Mono.error(new IllegalStateException("retry")) : Mono.just("ok"); }
        @RateLimited(policy="user",key="#p0.value",waitTimeout="200ms") public Mono<String> slowKey(SlowKey key) { calls.incrementAndGet(); return Mono.just("bad"); }
        @RateLimited(policy="user") public String servlet() { calls.incrementAndGet(); return "ok"; }
    }
    public static class SlowKey {
        final java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        public String getValue() {
            entered.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return "alice";
        }
    }
    record Fixture(Service proxy,Service target,AtomicInteger downstream) { }
    private Fixture fixture() { return fixture(1); }
    private Fixture fixture(long capacity) {
        var policies=PolicySet.compile(List.of(RateLimitPolicy.builder("user").scope(Scope.USER)
                .limit(new Limit(capacity,1,Duration.ofHours(1))).build()));
        var flow=DefaultQuotaFlow.builder(policies,new LocalRateLimitStore()).build();
        var target=new Service();var factory=new ProxyFactory(target);var downstream=new AtomicInteger();
        factory.addAdvisor(new RateLimitedAdvisor(new RateLimitInterceptor(flow,()->policies)));
        factory.addAdvice((org.aopalliance.intercept.MethodInterceptor) invocation->{downstream.incrementAndGet();return invocation.proceed();});
        return new Fixture((Service)factory.getProxy(),target,downstream);
    }
    private static UsernamePasswordAuthenticationToken user(String name) { return new UsernamePasswordAuthenticationToken(name,"unused",List.of()); }
    @AfterEach void clear(){SecurityContextHolder.clearContext();}
    @Test void subscriberAuthenticationAndFreshInvocationAreUsedForEverySubscription(){
        var f=fixture();var publisher=f.proxy.omitted();assertEquals(0,f.target.calls.get());
        assertEquals("ok",publisher.contextWrite(ReactiveSecurityContextHolder.withAuthentication(user("alice"))).block());
        assertEquals("ok",publisher.contextWrite(ReactiveSecurityContextHolder.withAuthentication(user("bob"))).block());
        assertThrows(RateLimitExceededException.class,()->publisher.contextWrite(ReactiveSecurityContextHolder.withAuthentication(user("alice"))).block());
        assertEquals(2,f.target.calls.get());assertEquals(2,f.downstream.get());assertNull(SecurityContextHolder.getContext().getAuthentication());
    }
    @Test void explicitFluxExpressionSeesReactiveAuthentication(){
        var f=fixture();assertEquals(List.of("ok"),f.proxy.explicit().contextWrite(ReactiveSecurityContextHolder.withAuthentication(user("alice"))).collectList().block());
    }
    @Test void servletOmittedUserKeyUsesOnlyAuthenticatedIdentity(){
        var f=fixture();SecurityContextHolder.getContext().setAuthentication(user("alice"));assertEquals("ok",f.proxy.servlet());
        SecurityContextHolder.getContext().setAuthentication(new AnonymousAuthenticationToken("key","anonymous",List.of(new org.springframework.security.core.authority.SimpleGrantedAuthority("ROLE_ANONYMOUS"))));
        assertThrows(RateLimitExceededException.class,()->f.proxy.servlet());
    }
    @Test void explicitNullAndAnonymousSubscriptionNeverInvokeBusiness(){
        var f=fixture();assertThrows(RateLimitExceededException.class,()->f.proxy.missing().contextWrite(ReactiveSecurityContextHolder.withAuthentication(user("alice"))).block());
        assertThrows(RateLimitExceededException.class,()->f.proxy.omitted().block());assertEquals(0,f.target.calls.get());
    }

    @Test void retryAndRepeatAcquireAndCloneTheInvocationForEverySubscription() {
        var f = fixture(10);
        assertEquals(List.of("ok", "ok", "ok"), f.proxy.retryable().retry(1).repeat(2)
                .contextWrite(ReactiveSecurityContextHolder.withAuthentication(user("alice")))
                .collectList().block());
        assertEquals(4, f.target.calls.get());
        assertEquals(4, f.downstream.get());
    }
    @Test void cancelledAcquisitionCannotInvokeBusinessAfterLateAllowance() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var pending = new java.util.concurrent.CompletableFuture<io.quotaflow.core.store.StoreResult>();
        var store = new io.quotaflow.core.store.RateLimitStore() {
            public java.util.concurrent.CompletionStage<Void> registerPolicies(List<io.quotaflow.core.store.PolicyBinding> bindings) { return java.util.concurrent.CompletableFuture.completedFuture(null); }
            public io.quotaflow.core.store.StoreResult tryAcquire(io.quotaflow.core.store.BucketIdentity key, Limit limit, Algorithm algorithm, long weight) { throw new AssertionError("blocking path"); }
            public java.util.concurrent.CompletionStage<io.quotaflow.core.store.StoreResult> tryAcquireAsync(io.quotaflow.core.store.BucketIdentity key, Limit limit, Algorithm algorithm, long weight) { entered.countDown(); return pending; }
        };
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("user").scope(Scope.USER).limit(new Limit(10,1,Duration.ofHours(1))).build()));
        try (var flow = DefaultQuotaFlow.builder(policies, store).build()) {
            var target = new Service(); var factory = new ProxyFactory(target);
            factory.addAdvisor(new RateLimitedAdvisor(new RateLimitInterceptor(flow, () -> policies)));
            var result = ((Service)factory.getProxy()).omitted()
                    .contextWrite(ReactiveSecurityContextHolder.withAuthentication(user("alice"))).toFuture();
            assertTrue(entered.await(2, java.util.concurrent.TimeUnit.SECONDS));
            assertTrue(result.cancel(true));
            pending.complete(io.quotaflow.core.store.StoreResult.acquired(9));
            flow.flushObservations().toCompletableFuture().get(2, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(0, target.calls.get());
        }
    }

    @Test void blockingExpressionCannotParkReactiveSchedulerOrInvokeAfterTimeout() throws Exception {
        var f = fixture(); var key = new SlowKey(); var scheduler = reactor.core.scheduler.Schedulers.newSingle("expression-progress");
        try {
            var result = f.proxy.slowKey(key).subscribeOn(scheduler).toFuture();
            assertTrue(key.entered.await(1, java.util.concurrent.TimeUnit.SECONDS));
            assertEquals(1, Mono.just(1).subscribeOn(scheduler).toFuture().get(1, java.util.concurrent.TimeUnit.SECONDS));
            var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> result.get(2, java.util.concurrent.TimeUnit.SECONDS));
            assertInstanceOf(RateLimitExceededException.class, failure.getCause());
            key.release.countDown();
            assertEquals(0, f.target.calls.get());
        } finally { key.release.countDown(); scheduler.dispose(); }
    }
}
