package io.quotaflow.store.redis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client creation for {@link RedisRateLimitStore}, hardened against an
 * incomplete netty native-transport stack.
 *
 * <p>Lettuce picks a native transport by probing only for the presence of
 * netty's {@code Epoll}/{@code KQueue} classes, but instantiating the event
 * loop needs the netty 4.2 {@code *IoHandler} classes of Lettuce's own netty
 * line. A classpath mixing a netty 4.2 core with 4.1 native-transport jars
 * (for example reactor-netty's line on a WebFlux application) passes the probe
 * yet fails instantiation with a {@link NoClassDefFoundError} — on Linux
 * x86_64, where the bundled native library loads successfully. Detecting the
 * incomplete stack here, before any Lettuce class initializes, and forcing NIO
 * through Lettuce's documented switches keeps the Redis client fully
 * functional for every consumer of this module; an explicit user setting of
 * either property is always respected.
 */
public final class RedisClientFactory {

    /** Lettuce switch selecting the epoll transport. */
    public static final String EPOLL_PROPERTY = "io.lettuce.core.epoll";

    /** Lettuce switch selecting the kqueue transport. */
    public static final String KQUEUE_PROPERTY = "io.lettuce.core.kqueue";

    private static final Logger log = LoggerFactory.getLogger(RedisClientFactory.class);

    private RedisClientFactory() {
    }

    /**
     * Creates a standalone client with the given connect timeout, after
     * disabling any incomplete native transport. The caller owns the client.
     */
    public static RedisClient createClient(String url, Duration connectTimeout) {
        return createClient(url, connectTimeout, connectTimeout);
    }

    /** Bounded, disconnected-rejecting transport with replay disabled for all library commands. */
    public static RedisClient createClient(String url, Duration connectTimeout, Duration commandTimeout) {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(connectTimeout, "connectTimeout");
        Objects.requireNonNull(commandTimeout, "commandTimeout");
        disableIncompleteNativeTransports();
        RedisClient client = RedisClient.create(url);
        client.setOptions(ClientOptions.builder()
                .socketOptions(SocketOptions.builder().connectTimeout(connectTimeout).build())
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .requestQueueSize(4096)
                .replayFilter(command -> false)
                .timeoutOptions(TimeoutOptions.enabled(commandTimeout))
                .build());
        return client;
    }

    /** Applies the incomplete-transport detection against the module's own classloader. */
    public static void disableIncompleteNativeTransports() {
        disableIncompleteNativeTransports(RedisClientFactory.class.getClassLoader());
    }

    static void disableIncompleteNativeTransports(ClassLoader classLoader) {
        disableIfIncomplete(classLoader, "io.netty.channel.epoll.Epoll",
                "io.netty.channel.epoll.EpollIoHandler", EPOLL_PROPERTY, "epoll");
        disableIfIncomplete(classLoader, "io.netty.channel.kqueue.KQueue",
                "io.netty.channel.kqueue.KQueueIoHandler", KQUEUE_PROPERTY, "kqueue");
    }

    /**
     * Last-resort safety net: unconditionally forces both native transports
     * off, used when a broken stack slipped past the presence probe and
     * surfaced as a {@link LinkageError} during client initialization.
     */
    static void disableNativeTransports() {
        System.setProperty(EPOLL_PROPERTY, "false");
        System.setProperty(KQUEUE_PROPERTY, "false");
    }

    private static void disableIfIncomplete(
            ClassLoader classLoader, String probeClass, String requiredClass, String property, String transport) {
        if (!isPresent(probeClass, classLoader)
                || isPresent(requiredClass, classLoader)
                || System.getProperty(property) != null) {
            return;
        }
        System.setProperty(property, "false");
        log.warn("incomplete netty {} native transport on the classpath ({} is present but {} is"
                + " missing, a netty version mix Lettuce cannot use); setting {}=false so the Redis"
                + " client falls back to NIO", transport, probeClass, requiredClass, property);
    }

    private static boolean isPresent(String className, ClassLoader classLoader) {
        try {
            Class.forName(className, false, classLoader);
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }
}
