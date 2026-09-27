package io.quotaflow.nativesmoke;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
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

        LocalRateLimitStore local = new LocalRateLimitStore();
        require(local.tryAcquire("smoke-tb", limit, Algorithm.TOKEN_BUCKET, 1).acquired(),
                "local allow expected");
        require(local.tryAcquire("smoke-tb", limit, Algorithm.TOKEN_BUCKET, 1).acquired(),
                "local allow expected");
        require(!local.tryAcquire("smoke-tb", limit, Algorithm.TOKEN_BUCKET, 1).acquired(),
                "local reject expected once the budget is spent");
        require(local.tryAcquire("smoke-gcra", limit, Algorithm.GCRA, 1).acquired(),
                "gcra local allow expected");
        require(local.tryAcquire("smoke-gcra", limit, Algorithm.GCRA, 1).acquired(),
                "gcra local allow expected");
        require(!local.tryAcquire("smoke-gcra", limit, Algorithm.GCRA, 1).acquired(),
                "gcra local reject expected once the budget is spent");

        String url = System.getenv().getOrDefault("REDIS_URL", "redis://localhost:6379");
        try (RedisRateLimitStore store =
                RedisRateLimitStore.connect(url, RedisStoreConfig.defaults(), Duration.ofSeconds(10))) {
            ChainResult result = store.tryAcquireAll(List.of(
                    new LevelRequest("native-smoke:global:root", limit, Algorithm.TOKEN_BUCKET, 1),
                    new LevelRequest("native-smoke:tenant:t1", limit, Algorithm.GCRA, 1)))
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
