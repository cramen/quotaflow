package io.quotaflow.core.store;

/** A failed compare-and-set carries current context, including provider ordering retained across static views. */
public record RecoveryControlResult(boolean applied, RecoveryContext context, int joinedMembers, int readyMembers,
                                    long retiredEpoch, long resolverFloor, String resolverFloorFingerprint) {
    public RecoveryControlResult(boolean applied, RecoveryContext context, int joinedMembers, int readyMembers, long retiredEpoch) {
        this(applied, context, joinedMembers, readyMembers, retiredEpoch, context.resolverRevision(), context.resolverFingerprint());
    }
}
