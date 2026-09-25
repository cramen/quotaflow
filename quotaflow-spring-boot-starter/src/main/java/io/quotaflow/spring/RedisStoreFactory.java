package io.quotaflow.spring;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SocketOptions;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

/**
 * Isolates every direct Lettuce reference of the auto-configuration. This
 * class must only be touched after a presence check for
 * {@code io.lettuce.core.RedisClient}, so applications without the driver
 * never load it.
 */
final class RedisStoreFactory {

    private static final Logger log = LoggerFactory.getLogger(RedisStoreFactory.class);

    private RedisStoreFactory() {
    }

    /** Opens a client and a dedicated connection; both are closed by the returned holder. */
    static PrimaryStoreHolder connect(QuotaFlowProperties.Redis properties) {
        disableIncompleteNativeTransports();
        RedisClient client = RedisClient.create(properties.getUrl());
        try {
            client.setOptions(ClientOptions.builder()
                    .socketOptions(SocketOptions.builder()
                            .connectTimeout(properties.getConnectTimeout())
                            .build())
                    .build());
            RedisStoreConfig config = new RedisStoreConfig(
                    properties.getCommandTimeout(), properties.getBusinessTimeout());
            RedisRateLimitStore store = RedisRateLimitStore.create(client, config);
            return new PrimaryStoreHolder(store, store, () -> {
                store.close();
                client.shutdown();
            });
        } catch (RuntimeException e) {
            client.shutdown();
            throw e;
        }
    }

    /**
     * Lettuce picks a native transport by probing only for the presence of
     * netty's {@code Epoll}/{@code KQueue} classes, but instantiating the
     * event loop needs the netty 4.2 {@code *IoHandler} classes of Lettuce's
     * own netty line. A classpath mixing a netty 4.2 core with 4.1
     * native-transport jars (for example reactor-netty's line on a WebFlux
     * application) passes the probe yet fails instantiation with a
     * {@link NoClassDefFoundError} — on Linux x86_64, where the bundled
     * native library loads successfully. Detecting the incomplete stack here,
     * before any Lettuce class initializes, and forcing NIO through Lettuce's
     * documented switches keeps the Redis client fully functional; an
     * explicit user setting of either property is always respected.
     */
    static void disableIncompleteNativeTransports() {        ClassLoader classLoader = RedisStoreFactory.class.getClassLoader();
        disableIfIncomplete(classLoader, "io.netty.channel.epoll.Epoll",
                "io.netty.channel.epoll.EpollIoHandler", "io.lettuce.core.epoll", "epoll");
        disableIfIncomplete(classLoader, "io.netty.channel.kqueue.KQueue",
                "io.netty.channel.kqueue.KQueueIoHandler", "io.lettuce.core.kqueue", "kqueue");
    }

    private static void disableIfIncomplete(
            ClassLoader classLoader, String probeClass, String requiredClass, String property, String transport) {
        if (!ClassUtils.isPresent(probeClass, classLoader)
                || ClassUtils.isPresent(requiredClass, classLoader)
                || System.getProperty(property) != null) {
            return;
        }
        System.setProperty(property, "false");
        log.warn("incomplete netty {} native transport on the classpath ({} is present but {} is"
                + " missing, a netty version mix Lettuce cannot use); setting {}=false so the Redis"
                + " client falls back to NIO", transport, probeClass, requiredClass, property);
    }
}
