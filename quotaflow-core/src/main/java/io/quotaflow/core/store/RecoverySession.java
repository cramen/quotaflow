package io.quotaflow.core.store;

import java.util.Objects;

/** Driver-issued ownership context. A configured instance ID alone is not ownership proof. */
public record RecoverySession(String cohortIncarnation, String cohortDigest, String instanceId,
                              int slot, long generation, String token) {
    public static final long MAX_COUNTER = 9_007_199_254_740_991L;
    public RecoverySession {
        QuotaDomain.requireIdentity(cohortIncarnation, "cohort incarnation");
        QuotaDomain.requireIdentity(instanceId, "instance ID");
        Objects.requireNonNull(cohortDigest, "cohortDigest");
        if (!cohortDigest.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("invalid cohort digest");
        if (slot < 0) throw new IllegalArgumentException("negative cohort slot");
        if (generation < 1 || generation > MAX_COUNTER) throw new IllegalArgumentException("invalid session generation");
        QuotaDomain.requireIdentity(token, "session token");
    }
    @Override public String toString() {
        return "RecoverySession[slot=" + slot + ", generation=" + generation + ", ownership=redacted]";
    }
}
