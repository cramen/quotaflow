package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Guard tests for incomplete netty native-transport stacks. The module's test
 * classpath carries the real broken epoll mix (4.1 transport classes against
 * the 4.2 core Lettuce resolves, no native library), so the epoll scenarios
 * run against real artifacts. The kqueue branch shares the same code path and
 * is covered for the absent case (no kqueue transport on this classpath); the
 * Linux x86_64 broken-mix reproduction with real native libraries runs in a
 * Docker container during change verification.
 */
class RedisClientFactoryTest {

    private static final String EPOLL = "io.netty.channel.epoll.Epoll";
    private static final String EPOLL_IO_HANDLER = "io.netty.channel.epoll.EpollIoHandler";

    @AfterEach
    void clearTransportProperties() {
        System.clearProperty(RedisClientFactory.EPOLL_PROPERTY);
        System.clearProperty(RedisClientFactory.KQUEUE_PROPERTY);
    }

    @Test
    void testClasspathCarriesTheIncompleteEpollStack() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        Class.forName(EPOLL, false, classLoader);
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName(EPOLL_IO_HANDLER, false, classLoader),
                EPOLL_IO_HANDLER + " must be absent from the test classpath");
    }

    @Test
    void disablesEpollWhenTheNativeTransportStackIsIncomplete() {
        RedisClientFactory.disableIncompleteNativeTransports();

        assertEquals("false", System.getProperty(RedisClientFactory.EPOLL_PROPERTY));
    }

    @Test
    void respectsAnExplicitUserSetting() {
        System.setProperty(RedisClientFactory.EPOLL_PROPERTY, "true");

        RedisClientFactory.disableIncompleteNativeTransports();

        assertEquals("true", System.getProperty(RedisClientFactory.EPOLL_PROPERTY));
    }

    @Test
    void leavesEpollAloneWhenTheTransportIsAbsent() {
        RedisClientFactory.disableIncompleteNativeTransports(
                hiding(getClass().getClassLoader(), EPOLL, EPOLL_IO_HANDLER));

        assertNull(System.getProperty(RedisClientFactory.EPOLL_PROPERTY));
    }

    @Test
    void leavesKqueueAloneWhenTheTransportIsAbsent() {
        // no kqueue transport on this module's classpath at all
        RedisClientFactory.disableIncompleteNativeTransports();

        assertNull(System.getProperty(RedisClientFactory.KQUEUE_PROPERTY));
    }

    @Test
    void safetyNetDisablesBothTransportsUnconditionally() {
        RedisClientFactory.disableNativeTransports();

        assertEquals("false", System.getProperty(RedisClientFactory.EPOLL_PROPERTY));
        assertEquals("false", System.getProperty(RedisClientFactory.KQUEUE_PROPERTY));
    }

    private static ClassLoader hiding(ClassLoader parent, String... hidden) {
        Set<String> names = Set.of(hidden);
        return new ClassLoader(parent) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (names.contains(name)) {
                    throw new ClassNotFoundException(name);
                }
                return super.loadClass(name, resolve);
            }
        };
    }
}
