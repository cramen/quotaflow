package io.quotaflow.core.store;

import io.quotaflow.core.LimitSnapshot;
import java.util.Objects;

/** Captured policy and provider target; a local reload counter cannot authorize a provider rollback. */
public record RecoveryConfiguration(String policyFingerprint, long localRevision,
                                    long resolverRevision, String resolverFingerprint) {
    public RecoveryConfiguration {
        Objects.requireNonNull(policyFingerprint, "policyFingerprint");
        if (!policyFingerprint.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("invalid policy fingerprint");
        if (localRevision < 0 || localRevision > RecoverySession.MAX_COUNTER) throw new IllegalArgumentException("invalid local configuration revision");
        validateResolver(resolverRevision, resolverFingerprint);
    }
    public RecoveryConfiguration(String fingerprint, long revision) { this(fingerprint, revision, 0, LimitSnapshot.NONE_FINGERPRINT); }
    public boolean matches(RecoveryContext context) {
        return policyFingerprint.equals(context.configurationFingerprint()) && resolverRevision == context.resolverRevision()
                && resolverFingerprint.equals(context.resolverFingerprint());
    }
    public static void validateResolver(long revision, String fingerprint) {
        Objects.requireNonNull(fingerprint, "resolverFingerprint");
        if (revision == 0 && fingerprint.equals(LimitSnapshot.NONE_FINGERPRINT)) return;
        if (revision <= 0 || revision > RecoverySession.MAX_COUNTER || fingerprint.length() != 64
                || fingerprint.equals(LimitSnapshot.NONE_FINGERPRINT))
            throw new IllegalArgumentException("invalid resolver snapshot identity");
        for (int i = 0; i < fingerprint.length(); i++) {
            char c = fingerprint.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
                throw new IllegalArgumentException("invalid resolver snapshot fingerprint");
        }
    }
}
