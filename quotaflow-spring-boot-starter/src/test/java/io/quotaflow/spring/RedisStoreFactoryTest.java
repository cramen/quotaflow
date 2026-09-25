package io.quotaflow.spring;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.ClassUtils;

/**
 * The starter's test classpath deliberately reproduces the broken netty mix a
 * WebFlux application gets (netty 4.2 core via Lettuce, 4.1 epoll jars via
 * reactor-netty), so the guard can be verified against the real artifacts.
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
    void disablesEpollWhenTheNativeTransportStackIsIncomplete() {
        System.clearProperty("io.lettuce.core.epoll");

        RedisStoreFactory.disableIncompleteNativeTransports();

        assertThat(System.getProperty("io.lettuce.core.epoll")).isEqualTo("false");
    }

    @Test
    void respectsAnExplicitUserSetting() {
        System.setProperty("io.lettuce.core.epoll", "true");

        RedisStoreFactory.disableIncompleteNativeTransports();

        assertThat(System.getProperty("io.lettuce.core.epoll")).isEqualTo("true");
    }
}
