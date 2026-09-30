package io.quotaflow.store.redis;

import java.net.URI;
import java.util.Locale;

/** Credential-free endpoint summaries and failures. Never retain a driver cause or suppressed exception. */
public final class SafeRedisDiagnostics {
    private SafeRedisDiagnostics() { }
    public static String endpoint(String value) {
        try {
            URI uri = new URI(value);
            String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
            if (!java.util.Set.of("redis", "rediss", "redis+ssl", "redis+tls", "redis-sentinel", "redis-sentinel+ssl", "redis-socket", "redis+socket").contains(scheme))
                return "invalid Redis endpoint";
            String topology = scheme.contains("sentinel") ? "sentinel" : scheme.contains("socket") ? "socket" : "standalone";
            String host = uri.getHost();
            return scheme + "://" + (host == null ? "configured-hosts" : host) + (uri.getPort() < 0 ? "" : ":" + uri.getPort()) + " (" + topology + ")";
        } catch (Exception invalid) { return "invalid Redis endpoint"; }
    }
    public static RuntimeException failure(String url, Throwable failure) {
        String category = failure instanceof LinkageError ? "driver-linkage" : failure instanceof IllegalArgumentException ? "configuration" : "connection";
        String message = "Redis " + category + " failure at " + endpoint(url) + "; verify quotaflow.redis.url, credentials, TLS and driver configuration";
        if (failure instanceof IllegalArgumentException) return new IllegalArgumentException(message);
        if (failure instanceof NullPointerException) return new NullPointerException(message);
        return new IllegalStateException(message);
    }
}
