package io.quotaflow.nativesmoke;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.Scope;
import io.quotaflow.core.store.QuotaDomain;
import io.quotaflow.core.store.BucketIdentity;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;
import java.time.Duration;
import java.util.List;

/**
 * Minimal limiter workload compiled to a native image by the CI native-smoke
 * job: local-store decisions (core algorithm path) plus an atomic chain
 * evaluation through the Redis store, which exercises the Lua script resource
 * loading covered by the module reachability metadata. Exits non-zero with a
 * message on any failed expectation.
 */
public final class NativeSmoke {

    private NativeSmoke() {
    }

    public static void main(String[] args) {
        Limit limit = new Limit(2, 1, Duration.ofSeconds(1));
        String namespace = "native-smoke-" + java.util.UUID.randomUUID();

        LocalRateLimitStore local = new LocalRateLimitStore();
        require(local.tryAcquire(new BucketIdentity(new QuotaDomain(namespace, "local"), "smoke-tb", Scope.KEY, "one"), limit, Algorithm.TOKEN_BUCKET, 1).acquired(),
                "local allow expected");
        require(local.tryAcquire(new BucketIdentity(new QuotaDomain(namespace, "local"), "smoke-tb", Scope.KEY, "one"), limit, Algorithm.TOKEN_BUCKET, 1).acquired(),
                "local allow expected");
        require(!local.tryAcquire(new BucketIdentity(new QuotaDomain(namespace, "local"), "smoke-tb", Scope.KEY, "one"), limit, Algorithm.TOKEN_BUCKET, 1).acquired(),
                "local reject expected once the budget is spent");
        require(local.tryAcquire(new BucketIdentity(new QuotaDomain(namespace, "local"), "smoke-gcra", Scope.KEY, "one"), limit, Algorithm.GCRA, 1).acquired(),
                "gcra local allow expected");
        require(local.tryAcquire(new BucketIdentity(new QuotaDomain(namespace, "local"), "smoke-gcra", Scope.KEY, "one"), limit, Algorithm.GCRA, 1).acquired(),
                "gcra local allow expected");
        require(!local.tryAcquire(new BucketIdentity(new QuotaDomain(namespace, "local"), "smoke-gcra", Scope.KEY, "one"), limit, Algorithm.GCRA, 1).acquired(),
                "gcra local reject expected once the budget is spent");

        String url = System.getenv().getOrDefault("REDIS_URL", "redis://localhost:6379");
        io.lettuce.core.RedisClient provisioningClient = io.quotaflow.store.redis.RedisClientFactory.createClient(url, Duration.ofSeconds(10));
        try (var provisioning = provisioningClient.connect()) {
            new io.quotaflow.store.redis.RedisNamespaceAdmin(provisioning).provisionFresh(namespace, true);
        } finally { provisioningClient.shutdown(); }
        try (RedisRateLimitStore store =
                RedisRateLimitStore.connect(url, RedisStoreConfig.defaults(), Duration.ofSeconds(10))) {
            ChainResult result = store.tryAcquireAll(List.of(
                    new LevelRequest(new BucketIdentity(new QuotaDomain(namespace, "root"), "root", Scope.GLOBAL, "global"), limit, Algorithm.TOKEN_BUCKET, 1),
                    new LevelRequest(new BucketIdentity(new QuotaDomain(namespace, "root"), "tenant", Scope.TENANT, "t1"), limit, Algorithm.GCRA, 1)))
                    .toCompletableFuture().join();
            require(result.acquired(), "distributed chain allow expected");
        }
        System.out.println("native-smoke OK");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException("native-smoke failed: " + message);
        }
    }
}
