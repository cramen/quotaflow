package io.quotaflow.tck;

import io.lettuce.core.RedisClient;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared standalone Redis 6.2 and Valkey containers (singleton pattern, one
 * pair per test JVM) for the conformance tests.
 */
abstract class TckContainers {

    protected static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);

    protected static final GenericContainer<?> VALKEY =
            new GenericContainer<>(DockerImageName.parse("valkey/valkey:7.2-alpine")).withExposedPorts(6379);

    private static final List<RedisClient> CLIENTS = new ArrayList<>();

    @BeforeAll
    static void startContainers() {
        Assumptions.assumeTrue(
                DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
        REDIS.start();
        VALKEY.start();
    }

    @AfterAll
    static void shutdownClients() {
        CLIENTS.forEach(RedisClient::shutdown);
    }

    protected static String redisUri() {
        return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    protected static String valkeyUri() {
        return "redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379);
    }

    /** A fresh client tracked for shutdown after the test class. */
    protected static RedisClient newClient(String uri) {
        RedisClient client = RedisClient.create(uri);
        CLIENTS.add(client);
        return client;
    }

    protected static String uniqueKey(String prefix) {
        return prefix + ':' + UUID.randomUUID();
    }
}
