package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Smoke test: the module can start a real Redis 6.2 via Testcontainers and talk to it over Lettuce. */
@Testcontainers(disabledWithoutDocker = true)
class RedisContainerSmokeTest {

    @Container
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);

    @Test
    void pingsRealRedis() {
        String uri = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
        try (RedisClient client = RedisClient.create(uri);
                StatefulRedisConnection<String, String> connection = client.connect()) {
            assertEquals("PONG", connection.sync().ping());
        }
    }
}
