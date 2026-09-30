package io.quotaflow.store.redis;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;

/** Bounded asynchronous membership and same-slot recovery control operations. Owns no connections. */
public final class RedisRecoveryController {
    private final RedisScriptingAsyncCommands<String, String> commands;
    private final long timeoutNanos;
    private final LuaScript membership = LuaScript.load("/lua/recovery_membership.lua");
    private final LuaScript control = LuaScript.load("/lua/recovery_control.lua");
    private final RedisKeyScheme keys = RedisKeyScheme.defaults();

    public RedisRecoveryController(StatefulRedisConnection<String, String> connection, Duration timeout) {
        this(connection.async(), timeout);
    }
    public RedisRecoveryController(StatefulRedisClusterConnection<String, String> connection, Duration timeout) {
        this(connection.async(), timeout);
    }
    private RedisRecoveryController(RedisScriptingAsyncCommands<String,String> commands, Duration timeout) {
        this.commands = Objects.requireNonNull(commands, "commands");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative() || timeout.compareTo(Duration.ofHours(1)) > 0)
            throw new IllegalArgumentException("control timeout must be positive and at most one hour");
        this.timeoutNanos = timeout.toNanos();
    }

    /** Offline administration only; attestations must cover all old distributed and local writers. */
    public CompletionStage<Void> provisionCohort(String namespace, RecoveryCohort cohort, String incarnation,
                                                boolean writersQuiesced, boolean debtDrained) {
        requireMaintenance(writersQuiesced, debtDrained);
        Objects.requireNonNull(incarnation, "incarnation");
        if (incarnation.isBlank()) throw new IllegalArgumentException("cohort incarnation must not be blank");
        List<String> args = new ArrayList<>(List.of("provision", incarnation, cohort.digest(), Integer.toString(cohort.size())));
        cohort.members().forEach(id -> args.add(memberDigest(id)));
        return eval(membership, keys.manifestKey(namespace), args).thenApply(reply -> null);
    }

    /**
     * Replaces owners or membership only after externally proven quiescence and drained debt.
     * Supply every provisioned domain, including retired policies. Retry with the same token after
     * an uncertain outcome. Runtime clients never call this method or infer these attestations from timeouts.
     * All same-slot fences are advanced before namespace enrollment becomes available again.
     */
    public CompletionStage<String> replaceCohort(String namespace, String expectedIncarnation,
            RecoveryCohort replacement, List<QuotaDomain> domains, String maintenanceToken,
            boolean writersQuiesced, boolean debtDrained) {
        requireMaintenance(writersQuiesced, debtDrained);
        Objects.requireNonNull(expectedIncarnation, "expectedIncarnation");
        if (maintenanceToken == null || maintenanceToken.isBlank()) throw new IllegalArgumentException("maintenance token is required");
        List<QuotaDomain> captured = domains.stream().sorted(Comparator.comparing(RedisKeyScheme::domainDigest)).toList();
        if (captured.stream().distinct().count() != captured.size()
                || captured.stream().anyMatch(domain -> !namespace.equals(domain.namespace())))
            throw new IllegalArgumentException("maintenance domains must be unique and share the namespace");
        List<String> prepare = new ArrayList<>(List.of("maintenance_prepare", expectedIncarnation, maintenanceToken,
                replacement.digest(), Integer.toString(replacement.size()), Integer.toString(captured.size())));
        replacement.members().forEach(id -> prepare.add(memberDigest(id)));
        captured.forEach(domain -> prepare.add(RedisKeyScheme.domainDigest(domain)));
        long started = System.nanoTime();
        return eval(membership, keys.manifestKey(namespace), prepare).thenCompose(reply -> {
            String nextIncarnation = (String) reply.get(1);
            if ("active".equals(reply.get(2))) return CompletableFuture.completedFuture(nextIncarnation);
            List<String> change = new ArrayList<>(List.of("maintenance", expectedIncarnation, nextIncarnation,
                    replacement.digest(), maintenanceToken, Integer.toString(replacement.size())));
            replacement.members().forEach(id -> change.add(memberDigest(id)));
            CompletionStage<Void> preceding = CompletableFuture.completedFuture(null);
            for (QuotaDomain domain : captured) preceding = preceding.thenCompose(ignored -> {
                if (System.nanoTime() - started >= timeoutNanos)
                    return CompletableFuture.failedFuture(new TimeoutException("cohort maintenance attempt timed out; retry the same token"));
                return eval(control, keys.controlKey(domain), change).thenApply(result -> null);
            });
            return preceding.thenCompose(ignored -> eval(membership, keys.manifestKey(namespace),
                    List.of("maintenance_commit", maintenanceToken))).thenApply(ignored -> nextIncarnation);
        });
    }

    /** A fresh nonce can claim only an unused slot. Reusing a nonce requires proven ownership continuity. */
    public CompletionStage<RecoverySession> enroll(String namespace, RecoveryCohort cohort, String instanceId, String nonce) {
        int slot = cohort.slot(instanceId);
        if (nonce == null || nonce.isBlank()) throw new IllegalArgumentException("session nonce must not be blank");
        return eval(membership, keys.manifestKey(namespace), List.of("enroll", cohort.digest(), Integer.toString(slot),
                memberDigest(instanceId), nonce)).thenApply(reply -> new RecoverySession((String) reply.get(1),
                cohort.digest(), instanceId, slot, number(reply, 3), nonce));
    }

    public CompletionStage<Void> provisionDomain(QuotaDomain domain, RecoveryCohort cohort, String incarnation,
                                                String configurationFingerprint, boolean writersQuiesced, boolean debtDrained) {
        requireMaintenance(writersQuiesced, debtDrained);
        if (!configurationFingerprint.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("invalid configuration fingerprint");
        List<String> args = new ArrayList<>(List.of("provision", incarnation, cohort.digest(), Integer.toString(cohort.size()), configurationFingerprint));
        for (String id : cohort.members()) args.addAll(List.of(memberDigest(id), "0", ""));
        return eval(membership, keys.manifestKey(domain.namespace()), List.of("describe")).thenCompose(reply -> {
            if (!incarnation.equals(reply.get(1)) || !cohort.digest().equals(reply.get(2)) || cohort.size() != number(reply, 3))
                throw new StateCompatibilityException("domain cohort differs from the namespace");
            return eval(membership, keys.manifestKey(domain.namespace()), List.of("domain_prepare", cohort.digest(),
                    incarnation, RedisKeyScheme.domainDigest(domain)));
        }).thenCompose(ignored -> eval(control, keys.controlKey(domain), args))
                .thenCompose(ignored -> eval(membership, keys.manifestKey(domain.namespace()), List.of("domain_commit", cohort.digest(),
                        incarnation, RedisKeyScheme.domainDigest(domain)))).thenApply(reply -> null);
    }

    /** Copies an already validated namespace ownership into a previously unused domain slot. */
    public CompletionStage<RecoveryControlResult> attach(QuotaDomain domain, RecoverySession session) {
        return eval(membership, keys.manifestKey(domain.namespace()), List.of("validate", session.cohortDigest(),
                Integer.toString(session.slot()), memberDigest(session.instanceId()), session.token(), RedisKeyScheme.domainDigest(domain)))
                .thenCompose(reply -> {
                    if (!session.cohortIncarnation().equals(reply.get(1)) || number(reply, 3) != session.generation())
                        throw new StateCompatibilityException("session ownership no longer matches namespace metadata");
                    return operation("attach", domain, session, List.of());
                });
    }
    public CompletionStage<RecoveryControlResult> read(QuotaDomain domain, RecoverySession session) {
        return operation("read", domain, session, List.of());
    }
    public CompletionStage<RecoveryControlResult> begin(QuotaDomain domain, RecoverySession session, String fingerprint, long localRevision) {
        if (!fingerprint.matches("[0-9a-f]{64}") || localRevision < 0 || localRevision > RecoverySession.MAX_COUNTER)
            throw new IllegalArgumentException("invalid recovery configuration proposal");
        return read(domain, session).thenCompose(current -> begin(current.context(), fingerprint, localRevision));
    }
    /** Opens only from the captured authoritative tuple; a delayed attempt cannot reopen a completed epoch. */
    public CompletionStage<RecoveryControlResult> begin(RecoveryContext observed, String fingerprint, long localRevision) {
        return begin(observed, new RecoveryConfiguration(fingerprint, localRevision, observed.resolverRevision(), observed.resolverFingerprint()));
    }
    public CompletionStage<RecoveryControlResult> begin(RecoveryContext observed, RecoveryConfiguration configuration) {
        return changeConfiguration("begin", observed, configuration);
    }
    public CompletionStage<RecoveryControlResult> configure(RecoveryContext observed, String fingerprint, long localRevision) {
        return configure(observed, new RecoveryConfiguration(fingerprint, localRevision, observed.resolverRevision(), observed.resolverFingerprint()));
    }
    public CompletionStage<RecoveryControlResult> configure(RecoveryContext observed, RecoveryConfiguration configuration) {
        return changeConfiguration("configure", observed, configuration);
    }
    private CompletionStage<RecoveryControlResult> changeConfiguration(String action, RecoveryContext observed, RecoveryConfiguration target) {
        return operation(action, observed.domain(), observed.session(), List.of(target.policyFingerprint(), Long.toString(target.localRevision()),
                Long.toString(observed.epoch()), Long.toString(observed.dispatchGeneration()), Long.toString(observed.configurationVersion()),
                observed.configurationFingerprint(), observed.phase().name(), Long.toString(target.resolverRevision()), target.resolverFingerprint(),
                Long.toString(observed.resolverRevision()), observed.resolverFingerprint()));
    }
    public CompletionStage<RecoveryControlResult> join(RecoveryContext context) { return compare("join", context); }
    public CompletionStage<RecoveryControlResult> ready(RecoveryContext context) { return compare("ready", context); }
    public CompletionStage<RecoveryControlResult> abort(RecoveryContext context) { return compare("abort", context); }
    private CompletionStage<RecoveryControlResult> compare(String action, RecoveryContext context) {
        return operation(action, context.domain(), context.session(), List.of(Long.toString(context.epoch()),
                Long.toString(context.dispatchGeneration()), Long.toString(context.configurationVersion()), context.configurationFingerprint(),
                Long.toString(context.resolverRevision()), context.resolverFingerprint()));
    }
    private CompletionStage<RecoveryControlResult> operation(String action, QuotaDomain domain, RecoverySession session, List<String> suffix) {
        List<String> args = new ArrayList<>(List.of(action, session.cohortIncarnation(), session.cohortDigest(),
                Integer.toString(session.slot()), Long.toString(session.generation()), session.token(), memberDigest(session.instanceId())));
        args.addAll(suffix);
        return eval(control, keys.controlKey(domain), args).thenApply(reply -> new RecoveryControlResult(number(reply, 0) == 1,
                new RecoveryContext(domain, session, number(reply, 1), number(reply, 2), number(reply, 3),
                        (String) reply.get(4), RecoveryPhase.valueOf((String) reply.get(5)), number(reply, 9), (String) reply.get(10)),
                Math.toIntExact(number(reply, 6)), Math.toIntExact(number(reply, 7)), number(reply, 8), number(reply, 11), (String) reply.get(12)));
    }
    private CompletionStage<List<Object>> eval(LuaScript script, String key, List<String> args) {
        // Timeout belongs to a detached view; the native command future is not cancelled or rewritten.
        CompletableFuture<List<Object>> nativeResult = commands.<List<Object>>eval(script.source(), ScriptOutputType.MULTI,
                new String[]{key}, args.toArray(String[]::new)).toCompletableFuture();
        return nativeResult.copy().orTimeout(timeoutNanos, TimeUnit.NANOSECONDS).handle((reply, failure) -> {
            if (failure == null) return reply;
            Throwable cause = failure instanceof CompletionException ? failure.getCause() : failure;
            if (cause instanceof io.lettuce.core.RedisCommandExecutionException && cause.getMessage() != null
                    && cause.getMessage().contains("QF_RECOVERY_")) {
                throw new StateCompatibilityException("recovery metadata or ownership is incompatible; no admission is authorized");
            }
            throw new CompletionException(cause);
        });
    }
    private static long number(List<Object> reply, int at) { return ((Number) reply.get(at)).longValue(); }
    private static void requireMaintenance(boolean quiesced, boolean drained) {
        if (!quiesced || !drained) throw new IllegalArgumentException("recovery provisioning requires quiescent writers and drained debt");
    }
    private static String memberDigest(String id) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(id.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 is required", e); }
    }
}
