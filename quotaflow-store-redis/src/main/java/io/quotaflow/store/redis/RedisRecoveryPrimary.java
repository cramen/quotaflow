package io.quotaflow.store.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

/** Recovery adapter over an explicitly configured transport. Owns neither its connection nor its store. */
public final class RedisRecoveryPrimary implements RecoveryPrimary {
    private final String namespace;
    private final java.util.function.Supplier<CompletionStage<String>> ping;
    private final RedisRateLimitStore store;
    private final RedisRecoveryController controller;
    private final long timeoutNanos;

    public RedisRecoveryPrimary(String namespace, StatefulRedisConnection<String,String> connection,
                                RedisRateLimitStore store, Duration timeout, boolean acquisitionsNeverReplayed) {
        this(namespace, store, timeout, acquisitionsNeverReplayed, connection.getOptions(),
                new RedisRecoveryController(connection, timeout), () -> connection.async().ping());
    }
    public RedisRecoveryPrimary(String namespace, io.lettuce.core.cluster.api.StatefulRedisClusterConnection<String,String> connection,
                                RedisRateLimitStore store, Duration timeout, boolean acquisitionsNeverReplayed) {
        this(namespace, store, timeout, acquisitionsNeverReplayed, connection.getOptions(),
                new RedisRecoveryController(connection, timeout), () -> connection.async().ping());
    }
    private RedisRecoveryPrimary(String namespace, RedisRateLimitStore store, Duration timeout, boolean acquisitionsNeverReplayed,
                                 io.lettuce.core.ClientOptions options, RedisRecoveryController controller,
                                 java.util.function.Supplier<CompletionStage<String>> ping) {
        this.namespace = new QuotaDomain(namespace, "validation").namespace();
        this.store = Objects.requireNonNull(store, "store");
        if (!acquisitionsNeverReplayed || options.getDisconnectedBehavior() != io.lettuce.core.ClientOptions.DisconnectedBehavior.REJECT_COMMANDS
                || options.getRequestQueueSize() <= 0 || options.getRequestQueueSize() == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("recovery requires bounded disconnected-rejecting transport and explicit no-replay capability");
        }
        this.controller = controller; this.ping = ping;
        timeoutNanos = timeout.toNanos();
    }
    @Override public CompletionStage<Void> probe() {
        return ping.get().toCompletableFuture().copy().orTimeout(timeoutNanos, TimeUnit.NANOSECONDS)
                .thenApply(reply -> null);
    }
    @Override public CompletionStage<RecoverySession> enroll(RecoveryCohort cohort, String instance, String nonce) {
        return controller.enroll(namespace, cohort, instance, nonce);
    }
    @Override public CompletionStage<RecoveryControlResult> attach(QuotaDomain domain, RecoverySession session) { return controller.attach(domain, session); }
    @Override public CompletionStage<RecoveryControlResult> read(QuotaDomain domain, RecoverySession session) { return controller.read(domain, session); }
    @Override public CompletionStage<RecoveryControlResult> begin(RecoveryContext observed, RecoveryConfiguration configuration) {
        return controller.begin(observed, configuration);
    }
    @Override public CompletionStage<RecoveryControlResult> configure(RecoveryContext observed, RecoveryConfiguration configuration) {
        return controller.configure(observed, configuration);
    }
    @Override public CompletionStage<RecoveryControlResult> join(RecoveryContext context) { return controller.join(context); }
    @Override public CompletionStage<RecoveryControlResult> ready(RecoveryContext context) { return controller.ready(context); }
    @Override public CompletionStage<RecoveryControlResult> abort(RecoveryContext context) { return controller.abort(context); }
    @Override public CompletionStage<ChainResult> acquire(RecoveryContext context, List<LevelRequest> chain, boolean guarded, RecoveryPending pending) {
        return store.tryAcquireAll(context, chain, guarded, pending);
    }
    @Override public CompletionStage<Boolean> seed(RecoveryContext context, List<BucketState> buckets) { return store.seed(context, buckets); }
    @Override public CompletionStage<Void> registerPolicies(List<PolicyBinding> bindings) { return store.registerPolicies(bindings); }
}
