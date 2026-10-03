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
        RedisClient client;
        try { client = RedisClient.create(new DiagnosticRedisURI(io.lettuce.core.RedisURI.create(url))); }
        catch (RuntimeException failure) { throw SafeRedisDiagnostics.failure(url, failure); }
        client.setOptions(ClientOptions.builder()
                .socketOptions(SocketOptions.builder().connectTimeout(connectTimeout).build())
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .requestQueueSize(4096)
                .replayFilter(command -> false)
                .timeoutOptions(TimeoutOptions.enabled(commandTimeout))
                .build());
        return client;
    }

    /** URI copies used by the driver must not render userinfo through a static credentials provider. */
    private static final class DiagnosticRedisURI extends io.lettuce.core.RedisURI {
        DiagnosticRedisURI(io.lettuce.core.RedisURI source) {
            // Boot consumers can manage Lettuce 6.6/6.8, which have no copy constructor.
            // Preserve URI settings through the public API shared with Lettuce 7.
            if (source.getHost() != null) setHost(source.getHost());
            setPort(source.getPort());
            if (source.getSocket() != null) setSocket(source.getSocket());
            setTimeout(source.getTimeout());
            setDatabase(source.getDatabase());
            if (source.getSentinelMasterId() != null) setSentinelMasterId(source.getSentinelMasterId());
            if (source.getClientName() != null) setClientName(source.getClientName());
            if (source.getLibraryName() != null) setLibraryName(source.getLibraryName());
            if (source.getLibraryVersion() != null) setLibraryVersion(source.getLibraryVersion());
            applySsl(source);
            applyAuthentication(source);
            var provider = source.getCredentialsProvider();
            if (provider != null) setCredentialsProvider(new SafeCredentialsProvider(provider));
            source.getSentinels().forEach(sentinel -> getSentinels().add(new DiagnosticRedisURI(sentinel)));
        }
        @Override public String toString() {
            String host = getHost();
            if (host == null || !host.matches("[a-zA-Z0-9.\\[\\]:_-]+")) host = "configured-hosts";
            return (isSsl() ? "rediss" : "redis") + "://" + host + ":" + getPort()
                    + (getSentinels().isEmpty() ? " (standalone)" : " (sentinel)");
        }
    }
    private static final class SafeCredentialsProvider implements io.lettuce.core.RedisCredentialsProvider {
        private final io.lettuce.core.RedisCredentialsProvider delegate;
        SafeCredentialsProvider(io.lettuce.core.RedisCredentialsProvider delegate) { this.delegate = delegate; }
        @Override public reactor.core.publisher.Mono<io.lettuce.core.RedisCredentials> resolveCredentials() {
            return reactor.core.publisher.Mono.defer(delegate::resolveCredentials).map(SafeCredentials::new)
                    .cast(io.lettuce.core.RedisCredentials.class)
                    .onErrorMap(failure -> new IllegalStateException("Redis credentials unavailable"));
        }
        @Override public boolean supportsStreaming() { return delegate.supportsStreaming(); }
        @Override public reactor.core.publisher.Flux<io.lettuce.core.RedisCredentials> credentials() {
            return reactor.core.publisher.Flux.defer(delegate::credentials).map(SafeCredentials::new)
                    .cast(io.lettuce.core.RedisCredentials.class)
                    .onErrorMap(failure -> new IllegalStateException("Redis credentials unavailable"));
        }
        @Override public String toString() { return "RedisCredentialsProvider[redacted]"; }
    }
    private static final class SafeCredentials implements io.lettuce.core.RedisCredentials {
        private final String username;
        private final char[] password;
        private final boolean hasUsername, hasPassword;
        SafeCredentials(io.lettuce.core.RedisCredentials value) {
            username = value.getUsername(); hasUsername = value.hasUsername(); hasPassword = value.hasPassword();
            var password = value.getPassword(); this.password = password == null ? null : password.clone();
        }
        @Override public String getUsername() { return username; }
        @Override public boolean hasUsername() { return hasUsername; }
        @Override public char[] getPassword() { return password == null ? null : password.clone(); }
        @Override public boolean hasPassword() { return hasPassword; }
        @Override public String toString() { return "RedisCredentials[redacted]"; }
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
