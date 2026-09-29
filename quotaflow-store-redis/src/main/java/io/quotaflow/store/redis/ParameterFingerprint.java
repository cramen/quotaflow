package io.quotaflow.store.redis;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Opaque schedule checksum, not encryption of low-entropy policy parameters. */
final class ParameterFingerprint {
    private ParameterFingerprint() { }
    static String of(Algorithm algorithm, Limit limit) {
        String value = "qf-state-v3:" + algorithm.name() + ":" + limit.capacity() + ":" + limit.emissionIntervalNanos();
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java platform", e);
        }
    }
}
