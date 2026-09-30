package io.quotaflow.core;

import io.quotaflow.core.store.QuotaDomain;
import io.quotaflow.core.store.RecoverySession;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Complete immutable tariff snapshot; its digest covers effective quota semantics, not map order. */
public final class LimitSnapshot implements LimitResolver {
    /** Reserved identity for a domain with no active dynamic resolver. */
    public static final String NONE_FINGERPRINT = "0".repeat(64);
    public record Key(String limitRef, String keyGroup) {
        public Key { new QuotaDomain(limitRef, keyGroup); }
    }
    private final long revision;
    private final Map<Key, Limit> limits;
    private final String fingerprint;

    public LimitSnapshot(long revision, Map<Key, Limit> limits) {
        if (revision < 1 || revision > RecoverySession.MAX_COUNTER)
            throw new IllegalArgumentException("snapshot revision must be positive and within the recovery counter range");
        this.revision = revision;
        this.limits = Map.copyOf(limits);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            field(digest, "quotaflow-limit-snapshot-v1");
            this.limits.entrySet().stream().sorted(Map.Entry.comparingByKey(Comparator.comparing(Key::limitRef).thenComparing(Key::keyGroup)))
                    .forEach(entry -> {
                        field(digest, entry.getKey().limitRef()); field(digest, entry.getKey().keyGroup());
                        field(digest, Long.toString(entry.getValue().capacity()));
                        field(digest, Long.toString(entry.getValue().emissionIntervalNanos()));
                    });
            fingerprint = HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 is required", impossible); }
    }
    public long revision() { return revision; }
    public String fingerprint() { return fingerprint; }
    public Map<Key, Limit> limits() { return limits; }
    @Override public Optional<Limit> resolve(String limitRef, String keyGroup) { return Optional.ofNullable(limits.get(new Key(limitRef, keyGroup))); }
    @Override public String toString() { return "LimitSnapshot[revision=" + revision + ", entries=" + limits.size() + "]"; }
    private static void field(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array()); digest.update(bytes);
    }
}
