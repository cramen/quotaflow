package io.quotaflow.store.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared Redis 6.2 container (singleton pattern, one container per test JVM)
 * plus helpers for store construction and unique test keys.
 */
abstract class RedisContainerSupport {

    protected static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);

    private static RedisClient client;

    @BeforeAll
    static void startRedis() {
        Assumptions.assumeTrue(
                DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
        REDIS.start();
        client = RedisClient.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
    }

    @AfterAll
    static void stopRedis() {
        if (client != null) {
            client.shutdown();
        }
    }

    protected static RedisClient client() {
        return client;
    }

    protected static RedisRateLimitStore newStore(StatefulRedisConnection<String, String> connection) {
        return new RedisRateLimitStore(connection, RedisStoreConfig.defaults());
    }

    protected static RedisRateLimitStore newStore(
            StatefulRedisConnection<String, String> connection, Duration commandTimeout) {
        return new RedisRateLimitStore(
                connection, new RedisStoreConfig(commandTimeout, Duration.ofSeconds(30)));
    }

    /** Unique storage key per test run so tests never interfere through leftover state. */
    protected static String storageKey(String policyId, String scope, String rawKey) {
        return policyId + ':' + scope + ':' + rawKey + '-' + UUID.randomUUID();
    }
}
