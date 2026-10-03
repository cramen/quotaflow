package io.quotaflow.testing;

import io.quotaflow.core.PolicySet;
import io.quotaflow.core.store.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Deterministic control-plane fixture for local routing tests. Remote peers are considered ready.
 * It is not an implementation of distributed quota accounting; Redis conformance tests cover that.
 */
public final class RecoveryPrimaryFixture implements RecoveryPrimary {
    public volatile boolean available = true;
    public volatile boolean startGather;
    public volatile boolean allow = true;
    public volatile boolean rejectConfigurationProposals;
    public volatile long reportedRemaining;
    public volatile boolean awaitOtherMembers;
    public volatile boolean awaitOtherReadiness;
    public final AtomicInteger joins = new AtomicInteger();
    public final AtomicInteger configurations = new AtomicInteger();
    public volatile java.util.function.Function<List<LevelRequest>, CompletionStage<ChainResult>> accounting;
    public final AtomicInteger acquisitions = new AtomicInteger();
    public final AtomicInteger probes = new AtomicInteger();
    public final AtomicInteger enrollments = new AtomicInteger();
    public volatile CompletableFuture<Void> delayedProbe;
    public final AtomicInteger seeds = new AtomicInteger();
    public final AtomicInteger readiness = new AtomicInteger();
    public volatile CompletableFuture<RecoveryControlResult> delayedReady;
    public volatile RecoveryControlResult lastReady;
    public volatile Runnable onReady = () -> { };
    public volatile List<BucketState> lastSeed = List.of();
    public volatile CompletableFuture<ChainResult> delayedAcquisition;
    public volatile CompletableFuture<Boolean> delayedSeed;
    private final PolicySet policies;
    private volatile int cohortSize = 1;
    private final Map<QuotaDomain, RecoveryControlResult> contexts = new ConcurrentHashMap<>();
    public RecoveryPrimaryFixture(PolicySet policies) { this.policies = policies; }
    public RecoveryControlResult current(QuotaDomain domain) { return contexts.get(domain); }
    private CompletionStage<RecoveryControlResult> rejected(RecoveryContext observed) {
        var current = contexts.get(observed.domain());
        return reply(new RecoveryControlResult(false, current.context(), current.joinedMembers(), current.readyMembers(),
                current.retiredEpoch(), current.resolverFloor(), current.resolverFloorFingerprint()));
    }
    private <T> CompletionStage<T> reply(T value) {
        return available ? CompletableFuture.completedFuture(value) : CompletableFuture.failedFuture(
                new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED));
    }
    @Override public CompletionStage<Void> probe() { probes.incrementAndGet(); return delayedProbe == null ? reply(null) : delayedProbe; }
    @Override public CompletionStage<Void> registerPolicies(List<PolicyBinding> bindings) { return reply(null); }
    @Override public CompletionStage<RecoverySession> enroll(RecoveryCohort cohort, String instance, String nonce) {
        enrollments.incrementAndGet(); cohortSize = cohort.size();
        return reply(new RecoverySession("fixture", cohort.digest(), instance, cohort.slot(instance), 1, nonce));
    }
    @Override public CompletionStage<RecoveryControlResult> attach(QuotaDomain domain, RecoverySession session) {
        if (!available) return reply(null);
        return reply(contexts.computeIfAbsent(domain, ignored -> new RecoveryControlResult(true,
                new RecoveryContext(domain, session, 1, 1, 0, policies.recoveryFingerprint(domain.rootPolicyId()), startGather ? RecoveryPhase.GATHER : RecoveryPhase.NORMAL),
                startGather ? 0 : cohortSize, startGather ? 0 : cohortSize, startGather ? 0 : 1)));
    }
    @Override public CompletionStage<RecoveryControlResult> read(QuotaDomain domain, RecoverySession session) { return reply(contexts.get(domain)); }
    private CompletionStage<RecoveryControlResult> move(RecoveryContext old, String fingerprint, RecoveryPhase phase, boolean advanceEpoch) {
        return move(old, new RecoveryConfiguration(fingerprint, 0, old.resolverRevision(), old.resolverFingerprint()), phase, advanceEpoch);
    }
    private CompletionStage<RecoveryControlResult> move(RecoveryContext old, RecoveryConfiguration configuration, RecoveryPhase phase, boolean advanceEpoch) {
        String fingerprint = configuration.policyFingerprint();
        if (!available) return reply(null);
        var existing = contexts.get(old.domain());
        if (!existing.context().equals(old)) return reply(new RecoveryControlResult(false, existing.context(), existing.joinedMembers(),
                existing.readyMembers(), existing.retiredEpoch(), existing.resolverFloor(), existing.resolverFloorFingerprint()));
        if (configuration.resolverRevision() > 0 && configuration.resolverRevision() < existing.resolverFloor())
            return reply(new RecoveryControlResult(false, existing.context(), existing.joinedMembers(), existing.readyMembers(),
                    existing.retiredEpoch(), existing.resolverFloor(), existing.resolverFloorFingerprint()));
        if (configuration.resolverRevision() > 0 && configuration.resolverRevision() == existing.resolverFloor()
                && !configuration.resolverFingerprint().equals(existing.resolverFloorFingerprint()))
            return CompletableFuture.failedFuture(new StateCompatibilityException("fixture snapshot revision conflict"));
        long epoch = old.epoch() + (advanceEpoch ? 1 : 0);
        var context = new RecoveryContext(old.domain(), old.session(), epoch, old.dispatchGeneration() + 1,
                old.configurationVersion() + (configuration.matches(old) ? 0 : 1), fingerprint, phase,
                configuration.resolverRevision(), configuration.resolverFingerprint());
        var next = new RecoveryControlResult(true, context, phase == RecoveryPhase.GATHER ? 0 : cohortSize,
                phase == RecoveryPhase.NORMAL ? cohortSize : 0, phase == RecoveryPhase.NORMAL ? epoch : existing.retiredEpoch(),
                Math.max(existing.resolverFloor(), configuration.resolverRevision()),
                configuration.resolverRevision() > existing.resolverFloor() ? configuration.resolverFingerprint() : existing.resolverFloorFingerprint());
        contexts.put(old.domain(), next); return reply(next);
    }
    @Override public CompletionStage<RecoveryControlResult> begin(RecoveryContext old, RecoveryConfiguration configuration) {
        if (rejectConfigurationProposals) return rejected(old);
        return move(old, configuration, RecoveryPhase.GATHER, true);
    }
    @Override public CompletionStage<RecoveryControlResult> configure(RecoveryContext old, RecoveryConfiguration configuration) {
        configurations.incrementAndGet();
        if (rejectConfigurationProposals) return rejected(old);
        return move(old, configuration, old.phase(), false);
    }
    @Override public CompletionStage<RecoveryControlResult> join(RecoveryContext old) {
        joins.incrementAndGet();
        if (awaitOtherMembers) {
            var result = new RecoveryControlResult(true, old, 1, 0, contexts.get(old.domain()).retiredEpoch(),
                    contexts.get(old.domain()).resolverFloor(), contexts.get(old.domain()).resolverFloorFingerprint());
            contexts.put(old.domain(), result); return reply(result);
        }
        return move(old, old.configurationFingerprint(), RecoveryPhase.DRAIN, false);
    }
    @Override public CompletionStage<RecoveryControlResult> ready(RecoveryContext old) {
        if (awaitOtherReadiness) {
            if (!available) return reply(null);
            var current = contexts.get(old.domain());
            if (!current.context().equals(old)) return rejected(old);
            var result = new RecoveryControlResult(true, old, cohortSize, 1, current.retiredEpoch(),
                    current.resolverFloor(), current.resolverFloorFingerprint());
            contexts.put(old.domain(), result); lastReady = result; onReady.run(); readiness.incrementAndGet();
            return delayedReady != null ? delayedReady : reply(result);
        }
        return move(old, old.configurationFingerprint(), RecoveryPhase.NORMAL, false).thenCompose(result -> {
            lastReady = result; onReady.run(); readiness.incrementAndGet();
            return delayedReady != null ? delayedReady : CompletableFuture.completedFuture(result);
        });
    }
    @Override public CompletionStage<RecoveryControlResult> abort(RecoveryContext old) { return move(old, old.configurationFingerprint(), RecoveryPhase.GATHER, false); }
    @Override public CompletionStage<ChainResult> acquire(RecoveryContext context, List<LevelRequest> chain, boolean guarded, RecoveryPending pending) {
        if (!available) return reply(null);
        acquisitions.incrementAndGet();
        if (delayedAcquisition != null) return delayedAcquisition;
        if (accounting != null) return accounting.apply(chain);
        return reply(allow ? ChainResult.acquired(chain.size() - 1, reportedRemaining) : ChainResult.rejected(0, 0, 1));
    }
    @Override public CompletionStage<Boolean> seed(RecoveryContext context, List<BucketState> buckets) {
        if (!available) return reply(null);
        seeds.incrementAndGet(); lastSeed = List.copyOf(buckets);
        return delayedSeed != null ? delayedSeed : reply(true);
    }
}
