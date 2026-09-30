package io.quotaflow.config;

import io.quotaflow.core.store.QuotaDomain;
import io.quotaflow.core.store.RecoveryCohort;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Normalized immutable accounting target. Endpoint credentials are represented only by a digest. */
public record StartupAccounting(String namespace, String deploymentId, String instanceId,
                                RecoveryCohort cohort, int expectedInstances, String endpointFingerprint, String recoveryMode) {
    public static final String DEFAULT_ENDPOINT = "redis://localhost:6379";
    public StartupAccounting(String namespace, String deploymentId, String instanceId, RecoveryCohort cohort, int expectedInstances) {
        this(namespace, deploymentId, instanceId, cohort, expectedInstances, fingerprintEndpoint(DEFAULT_ENDPOINT), "coordinated");
    }
    public StartupAccounting {
        new QuotaDomain(namespace, deploymentId);
        Objects.requireNonNull(instanceId, "instanceId");
        Objects.requireNonNull(cohort, "cohort").validate(expectedInstances, instanceId);
        if (!Objects.requireNonNull(endpointFingerprint, "endpointFingerprint").matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("invalid distributed endpoint fingerprint");
        recoveryMode = Objects.requireNonNull(recoveryMode, "recoveryMode").trim().toLowerCase(Locale.ROOT);
        if (!recoveryMode.equals("coordinated")) throw new IllegalArgumentException("only coordinated recovery mode is supported");
    }
    public static StartupAccounting defaults() {
        return new StartupAccounting("default", "default", RecoveryCohort.DEFAULT_INSTANCE_ID, RecoveryCohort.single(), 1);
    }
    /** Normalize ordinary Redis endpoint defaults without DNS resolution or exposing credentials. */
    public static String fingerprintEndpoint(String value) {
        try {
            URI endpoint = new URI(Objects.requireNonNull(value, "endpoint").trim()).normalize();
            if (endpoint.getScheme() == null) throw new IllegalArgumentException();
            String canonical = endpoint.toString();
            if (endpoint.getHost() != null) {
                String scheme = endpoint.getScheme().toLowerCase(Locale.ROOT);
                int port = endpoint.getPort();
                if (port < 0 && (scheme.equals("redis") || scheme.equals("rediss"))) port = 6379;
                String path = endpoint.getPath();
                if (path == null || path.equals("/") || path.equals("/0")) path = "";
                canonical = new URI(scheme, endpoint.getUserInfo(), endpoint.getHost().toLowerCase(Locale.ROOT),
                        port, path, endpoint.getQuery(), endpoint.getFragment()).toString();
            }
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception invalid) {
            throw new IllegalArgumentException("invalid distributed endpoint identity");
        }
    }
}
