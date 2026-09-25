package io.quotaflow.tck;

import io.lettuce.core.RedisClient;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;
import org.junit.jupiter.api.Test;

/**
 * Throttle conformance against the Redis-backed store (Testcontainers, margins
 * for network round-trips). The waiter queue lives in the client facade, so
 * the distributed store changes the retry-after source but not the semantics.
 */
class ThrottleRedisTckTest extends TckContainers {

    private final ThrottleConformance.Profile profile = ThrottleConformance.Profile.redis();

    @Test
    void oversubscribedPolicyServesAllWaitersOnRedis() throws Exception {
        try (RedisRateLimitStore store = redisStore()) {
            ThrottleConformance.oversubscribedPolicyServesAllWaiters(
                    store, uniqueKey("throttle:oversubscribed:redis"), profile);
        }
    }

    @Test
    void queueOverflowRejectsImmediatelyOnRedis() throws Exception {
        try (RedisRateLimitStore store = redisStore()) {
            ThrottleConformance.queueOverflowRejectsImmediately(
                    store, uniqueKey("throttle:overflow:redis"), profile);
        }
    }

    @Test
    void highPriorityWaiterServedFirstOnRedis() throws Exception {
        try (RedisRateLimitStore store = redisStore()) {
            ThrottleConformance.highPriorityWaiterServedFirst(
                    store, uniqueKey("throttle:priority:redis"), profile);
        }
    }

    @Test
    void waitTimeoutRejectsOnRedis() throws Exception {
        try (RedisRateLimitStore store = redisStore()) {
            ThrottleConformance.waitTimeoutRejects(
                    store, uniqueKey("throttle:timeout:redis"), profile);
        }
    }

    private static RedisRateLimitStore redisStore() {
        RedisClient client = newClient(redisUri());
        return RedisRateLimitStore.create(client, RedisStoreConfig.defaults());
    }
}
