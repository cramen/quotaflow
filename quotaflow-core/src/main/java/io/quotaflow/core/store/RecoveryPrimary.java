package io.quotaflow.core.store;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Primary adapter contract for fixed-cohort recovery. All context-bound quota operations
 * validate controller/session/generation/phase in the same atomic operation as their debit.
 * Implementations must bound dispatch queues/timeouts and must not replay acquisitions.
 */
public interface RecoveryPrimary {
    CompletionStage<Void> probe();
    CompletionStage<RecoverySession> enroll(RecoveryCohort cohort, String instanceId, String nonce);
    CompletionStage<RecoveryControlResult> attach(QuotaDomain domain, RecoverySession session);
    CompletionStage<RecoveryControlResult> read(QuotaDomain domain, RecoverySession session);
    CompletionStage<RecoveryControlResult> begin(RecoveryContext observed, RecoveryConfiguration configuration);
    default CompletionStage<RecoveryControlResult> begin(RecoveryContext observed, String fingerprint, long revision) {
        return begin(observed, new RecoveryConfiguration(fingerprint, revision, observed.resolverRevision(), observed.resolverFingerprint()));
    }
    CompletionStage<RecoveryControlResult> configure(RecoveryContext observed, RecoveryConfiguration configuration);
    default CompletionStage<RecoveryControlResult> configure(RecoveryContext observed, String fingerprint, long revision) {
        return configure(observed, new RecoveryConfiguration(fingerprint, revision, observed.resolverRevision(), observed.resolverFingerprint()));
    }
    CompletionStage<RecoveryControlResult> join(RecoveryContext context);
    CompletionStage<RecoveryControlResult> ready(RecoveryContext context);
    CompletionStage<RecoveryControlResult> abort(RecoveryContext context);
    CompletionStage<ChainResult> acquire(RecoveryContext context, List<LevelRequest> chain, boolean guarded, RecoveryPending pending);
    CompletionStage<Boolean> seed(RecoveryContext context, List<BucketState> buckets);
    CompletionStage<Void> registerPolicies(List<PolicyBinding> bindings);
}
