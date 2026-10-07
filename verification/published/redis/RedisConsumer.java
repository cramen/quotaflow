package io.quotaflow.verification.published;

import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.fallback.*;
import io.quotaflow.store.redis.*;
import java.time.Duration;
import java.util.List;

public final class RedisConsumer {
    public static void main(String[] args) throws Exception {
        if (Runtime.version().feature()!=Integer.getInteger("verification.jdk")) throw new AssertionError("Unexpected JDK");
        String url=System.getProperty("verification.redisUrl"), namespace=System.getProperty("verification.namespace");
        var policies=PolicySet.compile(List.of(RateLimitPolicy.builder("redis").scope(Scope.GLOBAL)
                .limit(new Limit(2,2,Duration.ofHours(1))).build()));
        var cohort=new RecoveryCohort(List.of("owner"));
        var client=RedisClientFactory.createClient(url,Duration.ofSeconds(2));
        try (var connection=client.connect(); var adminStore=new RedisRateLimitStore(connection,RedisStoreConfig.defaults())) {
            new RedisNamespaceAdmin(connection).provisionFresh(namespace,true,4096);
            var domain=new QuotaDomain(namespace,"redis");
            adminStore.registerPolicies(List.of(new PolicyBinding(domain,"redis",Scope.GLOBAL,Algorithm.TOKEN_BUCKET))).toCompletableFuture().join();
            var admin=new RedisRecoveryController(connection,Duration.ofSeconds(2));
            admin.provisionCohort(namespace,cohort,"initial",true,true).toCompletableFuture().join();
            admin.provisionDomain(domain,cohort,"initial",policies.recoveryFingerprint("redis"),true,true).toCompletableFuture().join();
            try (var redis=RedisRateLimitStore.connect(url,RedisStoreConfig.defaults(),Duration.ofSeconds(2));
                 var store=new FallbackRateLimitStore(redis.recoveryPrimary(namespace,Duration.ofMillis(200),true),
                         new RecoverySettings(namespace,"consumer","owner",cohort,100,100,Duration.ofMillis(20),Duration.ofSeconds(2)),List.of());
                 var flow=DefaultQuotaFlow.builder(policies,store).namespace(namespace).build()) {
                long deadline=System.nanoTime()+Duration.ofSeconds(15).toNanos();
                while (store.state()!=DegradationState.CLOSED && System.nanoTime()<deadline) Thread.sleep(20);
                if (store.state()!=DegradationState.CLOSED) throw new AssertionError("Published recovery did not become healthy");
                if (!flow.tryAcquire("redis",RateLimitContext.empty(),2).isAllowed()
                        || flow.tryAcquire("redis",RateLimitContext.empty(),1).isAllowed()) throw new AssertionError("Published atomic Redis quota failed");
            }
        } finally { client.shutdown(); }
        System.out.println("PUBLISHED CONSUMERS VERIFIED case="+System.getProperty("verification.case"));
    }
}
