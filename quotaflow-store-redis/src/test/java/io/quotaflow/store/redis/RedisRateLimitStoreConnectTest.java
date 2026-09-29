package io.quotaflow.store.redis;

import static io.quotaflow.testing.TestIdentities.key;
import io.quotaflow.core.store.BucketIdentity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * {@link RedisRateLimitStore#connect} end to end: creates and owns its client
 * (guarded against the broken epoll mix this test classpath carries) and
 * closes it cleanly.
 */
class RedisRateLimitStoreConnectTest extends RedisContainerSupport {

    private static String redisUri() {
        return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    @Test
    void connectCreatesAWorkingStoreAndOwnsTheClient() {
        RedisRateLimitStore store = RedisRateLimitStore.connect(
                redisUri(), RedisStoreConfig.defaults(), Duration.ofSeconds(2));
        try {
            assertTrue(store.tryAcquire(
                            key(storageKey("connect", "global", "k")), new Limit(1, 1, Duration.ofSeconds(1)),
                            Algorithm.TOKEN_BUCKET, 1)
                    .acquired());
        } finally {
            assertDoesNotThrow(store::close);
        }
    }

    @Test
    void connectRunsTheNativeTransportGuard() {
        System.clearProperty(RedisClientFactory.EPOLL_PROPERTY);
        try (RedisRateLimitStore ignored = RedisRateLimitStore.connect(
                redisUri(), RedisStoreConfig.defaults(), Duration.ofSeconds(2))) {
            // the broken epoll mix on this test classpath must have been disabled
            // before Lettuce initialized
            org.junit.jupiter.api.Assertions.assertEquals(
                    "false", System.getProperty(RedisClientFactory.EPOLL_PROPERTY));
        } finally {
            System.clearProperty(RedisClientFactory.EPOLL_PROPERTY);
        }
    }
}
