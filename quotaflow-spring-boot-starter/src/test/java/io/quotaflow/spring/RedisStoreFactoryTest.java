package io.quotaflow.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.ClassUtils;

/**
 * The starter's test classpath deliberately reproduces the broken netty mix a
 * WebFlux application gets (netty 4.2 core via Lettuce, 4.1 epoll jars via
 * reactor-netty). The guard itself lives in the store module now; these tests
 * pin the classpath mix and verify that store creation through the starter
 * delegates to it.
 */
class RedisStoreFactoryTest {

    @AfterEach
    void clearTransportProperties() {
        System.clearProperty("io.lettuce.core.epoll");
        System.clearProperty("io.lettuce.core.kqueue");
    }

    @Test
    void testClasspathCarriesTheIncompleteEpollStack() {
        ClassLoader classLoader = getClass().getClassLoader();
        assertThat(ClassUtils.isPresent("io.netty.channel.epoll.Epoll", classLoader)).isTrue();
        assertThat(ClassUtils.isPresent("io.netty.channel.epoll.EpollIoHandler", classLoader)).isFalse();
    }

    @Test
    void connectDelegatesToTheStoreModuleGuard() {
        System.clearProperty("io.lettuce.core.epoll");

        assertThatThrownBy(() -> RedisStoreFactory.recoveryConnection(unreachableRedis(), "default"))
                // unreachable endpoint: connection fails, but only after the delegated guard ran
                .isInstanceOf(Exception.class);

        assertThat(System.getProperty("io.lettuce.core.epoll")).isEqualTo("false");
    }

    @Test
    void delegationRespectsAnExplicitUserSetting() {
        System.setProperty("io.lettuce.core.epoll", "true");

        assertThatThrownBy(() -> RedisStoreFactory.recoveryConnection(unreachableRedis(), "default"))
                .isInstanceOf(Exception.class);

        assertThat(System.getProperty("io.lettuce.core.epoll")).isEqualTo("true");
    }

    private static QuotaFlowProperties.Redis unreachableRedis() {
        QuotaFlowProperties.Redis properties = new QuotaFlowProperties.Redis();
        // nothing listens here; the connection attempt fails fast
        properties.setUrl("redis://localhost:1");
        properties.setConnectTimeout(Duration.ofMillis(200));
        return properties;
    }
}
