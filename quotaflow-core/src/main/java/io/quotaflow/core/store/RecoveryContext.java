package io.quotaflow.core.store;

import java.util.Objects;

/** Captured context checked atomically with quota mutation by a recovery-capable primary. */
public record RecoveryContext(QuotaDomain domain, RecoverySession session, long epoch,
                              long dispatchGeneration, long configurationVersion,
                              String configurationFingerprint, RecoveryPhase phase,
                              long resolverRevision, String resolverFingerprint) {
    public RecoveryContext(QuotaDomain domain, RecoverySession session, long epoch, long dispatchGeneration,
                           long configurationVersion, String configurationFingerprint, RecoveryPhase phase) {
        this(domain, session, epoch, dispatchGeneration, configurationVersion, configurationFingerprint, phase,
                0, io.quotaflow.core.LimitSnapshot.NONE_FINGERPRINT);
    }
    public RecoveryContext {
        RecoveryConfiguration.validateResolver(resolverRevision, resolverFingerprint);
        Objects.requireNonNull(domain, "domain"); Objects.requireNonNull(session, "session");
        Objects.requireNonNull(phase, "phase");
        for (long value : new long[]{epoch, dispatchGeneration, configurationVersion}) {
            if (value < 0 || value > RecoverySession.MAX_COUNTER) throw new IllegalArgumentException("invalid recovery counter");
        }
        Objects.requireNonNull(configurationFingerprint, "configurationFingerprint");
        if (!configurationFingerprint.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("invalid configuration fingerprint");
    }
    @Override public String toString() {
        return "RecoveryContext[epoch=" + epoch + ", dispatch=" + dispatchGeneration
                + ", configuration=" + configurationVersion + ", resolver=" + resolverRevision + ", phase=" + phase + "]";
    }
}
