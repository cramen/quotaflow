package io.quotaflow.testing;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.quotaflow.core.store.*;
import io.quotaflow.store.redis.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Explicit offline provisioning for numeric/healthy-store tests. Connections represent one known owner;
 * fixed-cohort recovery tests use the production controller and independent sessions directly.
 */
public final class RecoveryStoreFixture extends RedisRateLimitStore {
    private static final Object ADMIN = new Object();
    private static final String FP = "a".repeat(64);
    private final RedisRecoveryController controller;
    private final RedisHashAsyncCommands<String, String> hashes;
    private final AutoCloseable owned;
    private final Set<QuotaDomain> prepared = ConcurrentHashMap.newKeySet();
    public RecoveryStoreFixture(StatefulRedisConnection<String, String> connection, RedisStoreConfig config) {
        this(connection, config, null);
    }
    private RecoveryStoreFixture(StatefulRedisConnection<String, String> connection, RedisStoreConfig config, AutoCloseable owned) {
        super(connection, config); controller = new RedisRecoveryController(connection, Duration.ofSeconds(5));
        hashes = connection.async(); this.owned = owned;
    }
    public RecoveryStoreFixture(StatefulRedisClusterConnection<String, String> connection, RedisStoreConfig config) {
        this(connection, config, null);
    }
    private RecoveryStoreFixture(StatefulRedisClusterConnection<String, String> connection, RedisStoreConfig config, AutoCloseable owned) {
        super(connection, config); controller = new RedisRecoveryController(connection, Duration.ofSeconds(5));
        hashes = connection.async(); this.owned = owned;
    }
    public static RecoveryStoreFixture create(RedisClient client, RedisStoreConfig config) {
        var connection = client.connect(); return new RecoveryStoreFixture(connection, config, connection);
    }
    public static RecoveryStoreFixture create(RedisClusterClient client, RedisStoreConfig config) {
        var connection = client.connect(); return new RecoveryStoreFixture(connection, config, connection);
    }
    public static void prepareFor(RedisRateLimitStore store, StatefulRedisConnection<String, String> connection, List<PolicyBinding> bindings) {
        store.registerPolicies(bindings).toCompletableFuture().join();
        var fixture = new RecoveryStoreFixture(connection, RedisStoreConfig.defaults());
        synchronized (ADMIN) {
            for (QuotaDomain domain : bindings.stream().map(PolicyBinding::domain).distinct().toList())
                store.bindRecoveryContext(fixture.prepare(bindings, domain));
        }
    }
    private RecoveryContext prepare(List<PolicyBinding> bindings, QuotaDomain domain) {
        super.registerPolicies(bindings).toCompletableFuture().join();
        var keys = RedisKeyScheme.defaults(); var cohort = RecoveryCohort.single();
        if (hashes.hget(keys.manifestKey(domain.namespace()), "rc:incarnation").toCompletableFuture().join() == null)
            controller.provisionCohort(domain.namespace(), cohort, "numeric-fixture", true, true).toCompletableFuture().join();
        if (hashes.hget(keys.controlKey(domain), "version").toCompletableFuture().join() == null)
            controller.provisionDomain(domain, cohort, "numeric-fixture", FP, true, true).toCompletableFuture().join();
        var session = controller.enroll(domain.namespace(), cohort, "single", "numeric-fixture-session").toCompletableFuture().join();
        var context = controller.attach(domain, session).toCompletableFuture().join().context();
        if (context.phase() == RecoveryPhase.GATHER) context = controller.join(context).toCompletableFuture().join().context();
        if (context.phase() == RecoveryPhase.DRAIN) context = controller.ready(context).toCompletableFuture().join().context();
        bindRecoveryContext(context); prepared.add(domain); return context;
    }
    @Override public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> requests) {
        List<LevelRequest> chain = List.copyOf(requests); LevelRequest.validateChain(chain);
        QuotaDomain domain = chain.get(0).storageKey().domain();
        if (!prepared.contains(domain)) synchronized (ADMIN) {
            if (!prepared.contains(domain)) prepare(chain.stream().map(item -> PolicyBinding.of(item.storageKey(), item.algorithm())).toList(), domain);
        }
        return super.tryAcquireAll(chain).thenComposeAsync(result -> {
            if (result.recoveryPending() == null) return CompletableFuture.completedFuture(result);
            synchronized (ADMIN) {
                prepare(chain.stream().map(item -> PolicyBinding.of(item.storageKey(), item.algorithm())).toList(), domain);
            }
            return super.tryAcquireAll(chain);
        });
    }
    @Override public CompletionStage<Void> seed(QuotaDomain domain, List<BucketState> states) {
        var captured = List.copyOf(states);
        Set<BucketIdentity> seen = new HashSet<>();
        for (var state : captured) if (!state.storageKey().domain().equals(domain) || !seen.add(state.storageKey()))
            throw new IllegalArgumentException("invalid seed domain or duplicate bucket");
        if (captured.isEmpty()) return CompletableFuture.completedFuture(null);
        try { synchronized (ADMIN) {
            var context = prepare(captured.stream().map(item -> PolicyBinding.of(item.storageKey(), item.algorithm())).toList(), domain);
            var gather = controller.begin(context, FP, 0).toCompletableFuture().join().context();
            super.seed(gather, captured).toCompletableFuture().join();
            var drain = controller.join(gather).toCompletableFuture().join().context();
            bindRecoveryContext(controller.ready(drain).toCompletableFuture().join().context());
        } } catch (RuntimeException failure) { return CompletableFuture.failedFuture(failure); }
        return CompletableFuture.completedFuture(null);
    }
    @Override public void close() {
        super.close();
        if (owned != null) try { owned.close(); } catch (Exception failure) { throw new RuntimeException(failure); }
    }
}
