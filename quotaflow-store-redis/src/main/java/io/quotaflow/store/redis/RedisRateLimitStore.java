package io.quotaflow.store.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.PolicyConfigurationException;
import io.quotaflow.core.store.BatchRateLimitStore;
import io.quotaflow.core.store.BucketState;
import io.quotaflow.core.store.BucketIdentity;
import io.quotaflow.core.store.QuotaDomain;
import io.quotaflow.core.store.PolicyBinding;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.StateSeeder;
import io.quotaflow.core.store.StoreResult;
import io.quotaflow.core.store.StateCompatibilityException;
import io.quotaflow.core.store.RecoveryContext;
import io.quotaflow.core.store.RecoveryPending;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Distributed {@link BatchRateLimitStore} on Redis/Valkey via Lettuce. Every
 * decision is one atomic Lua execution whose timestamps come from the server's
 * TIME command: {@code token_bucket.lua} / {@code gcra.lua} for single-level
 * calls and {@code chain.lua} for whole-chain evaluation in one round-trip.
 * Scripts run via EVALSHA and are transparently resubmitted with EVAL when the
 * server script cache is cold (NOSCRIPT).
 *
 * <p>All I/O goes through Lettuce's async API; the synchronous SPI methods
 * join on the caller thread and hold no locks, so the store is
 * virtual-threads-safe. Store calls are bounded by the configured command
 * timeout, which is validated to be strictly below the business timeout (see
 * {@link RedisStoreConfig}).
 */
public class RedisRateLimitStore implements BatchRateLimitStore, StateSeeder, AutoCloseable {

    private static final String ALGORITHM_TOKEN_BUCKET = "tb";
    private static final String ALGORITHM_GCRA = "gcra";

    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
    private final StatefulConnection<String, String> connection;
    private final RedisScriptingAsyncCommands<String, String> async;
    private final boolean closeConnection;
    private final RedisClient ownedClient;
    private final RedisStoreConfig config;
    private final RedisKeyScheme keyScheme;
    private final LuaScript registerScript;
    private final LuaScript guardedChain = LuaScript.coordinated("/lua/chain.lua", "acquire");
    private final LuaScript guardedSeed = LuaScript.coordinated("/lua/seed.lua", "seed");
    private record BoundContext(RecoveryContext context, io.quotaflow.core.store.RecoveryPending pending,
                                CompletableFuture<Void> changed) { }
    private final ConcurrentHashMap<QuotaDomain, BoundContext> boundContexts = new ConcurrentHashMap<>();
    private final java.util.Set<PolicyBinding> verifiedBindings = ConcurrentHashMap.newKeySet();

    /** Store over a caller-managed standalone connection; {@link #close()} does not close it. */
    public RedisRateLimitStore(StatefulRedisConnection<String, String> connection, RedisStoreConfig config) {
        this(connection, connection.async(), config, RedisKeyScheme.defaults(), false, null);
    }

    /** Store over a caller-managed standalone connection with a custom key scheme. */
    public RedisRateLimitStore(
            StatefulRedisConnection<String, String> connection,
            RedisStoreConfig config,
            RedisKeyScheme keyScheme) {
        this(connection, connection.async(), config, keyScheme, false, null);
    }

    /** Store over a caller-managed Cluster connection; {@link #close()} does not close it. */
    public RedisRateLimitStore(
            StatefulRedisClusterConnection<String, String> connection, RedisStoreConfig config) {
        this(connection, connection.async(), config, RedisKeyScheme.defaults(), false, null);
    }

    /** Opens a dedicated connection on {@code client}; {@link #close()} closes it. */
    public static RedisRateLimitStore create(RedisClient client, RedisStoreConfig config) {
        Objects.requireNonNull(client, "client");
        StatefulRedisConnection<String, String> connection = client.connect();
        return new RedisRateLimitStore(connection, connection.async(), config, RedisKeyScheme.defaults(), true, null);
    }

    /** Opens a dedicated connection on the Cluster {@code client}; {@link #close()} closes it. */
    public static RedisRateLimitStore create(RedisClusterClient client, RedisStoreConfig config) {
        Objects.requireNonNull(client, "client");
        StatefulRedisClusterConnection<String, String> connection = client.connect();
        return new RedisRateLimitStore(connection, connection.async(), config, RedisKeyScheme.defaults(), true, null);
    }

    /**
     * Creates a client to {@code url} and opens a dedicated connection on it;
     * {@link #close()} closes both. Client creation is hardened against an
     * incomplete netty native-transport stack (see {@link RedisClientFactory}):
     * such transports are disabled with a warning before Lettuce initializes.
     * As a last-resort safety net, a linkage failure during initialization
     * (a broken mix the presence probe could not see, or one the user forced
     * via an explicit switch) disables both native transports for the rest of
     * the JVM — Lettuce caches its transport choice in static state, so the
     * same JVM cannot retry — and surfaces an actionable exception instead of
     * a bare {@link NoClassDefFoundError}.
     */
    public static RedisRateLimitStore connect(String url, RedisStoreConfig config, Duration connectTimeout) {
        Objects.requireNonNull(connectTimeout, "connectTimeout");
        try {
            return connectOnce(url, config, connectTimeout);
        } catch (LinkageError e) {
            RedisClientFactory.disableNativeTransports();
            throw SafeRedisDiagnostics.failure(url, e);
        } catch (RuntimeException failure) {
            throw SafeRedisDiagnostics.failure(url, failure);
        }
    }

    /** Creates a controller adapter on this store's transport; borrowed transports require an explicit no-replay attestation. */
    @SuppressWarnings("unchecked")
    public RedisRecoveryPrimary recoveryPrimary(String namespace, Duration timeout, boolean acquisitionsNeverReplayed) {
        if (connection instanceof StatefulRedisConnection<?, ?> standalone)
            return new RedisRecoveryPrimary(namespace, (StatefulRedisConnection<String, String>) standalone,
                    this, timeout, acquisitionsNeverReplayed);
        return new RedisRecoveryPrimary(namespace, (StatefulRedisClusterConnection<String, String>) connection,
                this, timeout, acquisitionsNeverReplayed);
    }

    private static RedisRateLimitStore connectOnce(String url, RedisStoreConfig config, Duration connectTimeout) {
        RedisClient client = RedisClientFactory.createClient(url, connectTimeout, config.commandTimeout());
        try {
            StatefulRedisConnection<String, String> connection = client.connect();
            return new RedisRateLimitStore(
                    connection, connection.async(), config, RedisKeyScheme.defaults(), true, client);
        } catch (RuntimeException | LinkageError e) {
            try {
                client.shutdown();
            } catch (RuntimeException shutdownFailure) {
                e.addSuppressed(shutdownFailure);
            }
            throw e;
        }
    }

    private RedisRateLimitStore(
            StatefulConnection<String, String> connection,
            RedisScriptingAsyncCommands<String, String> async,
            RedisStoreConfig config,
            RedisKeyScheme keyScheme,
            boolean closeConnection,
            RedisClient ownedClient) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.async = Objects.requireNonNull(async, "async");
        this.config = Objects.requireNonNull(config, "config");
        this.keyScheme = Objects.requireNonNull(keyScheme, "keyScheme");
        this.closeConnection = closeConnection;
        this.ownedClient = ownedClient;
        this.registerScript = LuaScript.load("/lua/register_policies.lua");
    }

    @Override
    public CompletionStage<Void> registerPolicies(List<PolicyBinding> candidate) {
        List<PolicyBinding> bindings = List.copyOf(candidate);
        if (bindings.isEmpty()) return CompletableFuture.completedFuture(null);
        String namespace = bindings.get(0).domain().namespace();
        String[] args = new String[bindings.size() * 2];
        for (int i = 0; i < bindings.size(); i++) {
            PolicyBinding binding = bindings.get(i);
            if (!namespace.equals(binding.domain().namespace())) {
                throw new IllegalArgumentException("one registration candidate must use one namespace");
            }
            args[2 * i] = RedisKeyScheme.policyDigest(binding.policyId());
            args[2 * i + 1] = binding.scope().wireName() + ":" + RedisKeyScheme.domainDigest(binding.domain()) + ":" + algorithmTag(binding.algorithm());
        }
        return evalWithFallback(registerScript, new String[] {keyScheme.manifestKey(namespace)}, args)
                .thenApply(reply -> {
                    long result = number(reply, 0);
                    if (result != 1) {
                        throw new PolicyConfigurationException(switch ((int) result) {
                            case -1 -> "quota namespace is not ready; explicit provisioning is required";
                            case -2 -> "policy algorithm, scope or root domain conflicts with the namespace binding";
                            case -3 -> "namespace policy registration budget is exhausted";
                            default -> "quota namespace manifest is incompatible or corrupt";
                        });
                    }
                    for (PolicyBinding binding : bindings) {
                        verifiedBindings.add(binding);
                    }
                    return null;
                });
    }

    private CompletionStage<List<Object>> evalRegistered(
            LuaScript script, String[] keys, String[] args, List<PolicyBinding> bindings) {
        List<PolicyBinding> missing = null;
        for (PolicyBinding binding : bindings) {
            if (!verifiedBindings.contains(binding)) {
                if (missing == null) missing = new ArrayList<>();
                missing.add(binding);
            }
        }
        if (missing == null) return evalWithFallback(script, keys, args);
        return registerPolicies(missing).thenCompose(ignored -> evalWithFallback(script, keys, args));
    }

    @Override public boolean requiresVersionedLimits() { return true; }

    @Override
    public StoreResult tryAcquire(BucketIdentity storageKey, Limit limit, Algorithm algorithm, long weight) {
        return tryAcquireAsync(storageKey, limit, algorithm, weight).toCompletableFuture().join();
    }

    /**
     * Binds an externally validated controller context for the plain store SPI. Advanced clients must
     * publish each new context explicitly; stale contexts return pending and never refresh themselves.
     * The conservative fallback coordinator uses context-bearing overloads directly instead.
     */
    public void bindRecoveryContext(RecoveryContext context) {
        if (closed.get()) throw new IllegalStateException("Redis store is closed");
        Objects.requireNonNull(context, "context");
        if (verifiedBindings.stream().noneMatch(binding -> binding.domain().equals(context.domain())))
            throw new io.quotaflow.core.PolicyConfigurationException("recovery context requires a registered domain");
        CompletableFuture<Void> changed = new CompletableFuture<>();
        var pending = new RecoveryPending(context.domain(), context.dispatchGeneration(), changed);
        BoundContext previous = boundContexts.put(context.domain(), new BoundContext(context, pending, changed));
        if (previous != null) previous.changed().complete(null);
    }
    private BoundContext bound(QuotaDomain domain) {
        BoundContext context = boundContexts.get(domain);
        if (context == null) throw new io.quotaflow.core.PolicyConfigurationException(
                "Redis acquisitions and seeds require an explicit recovery context; use the fixed-cohort fallback coordinator or bind a validated context");
        return context;
    }
    @Override public CompletionStage<StoreResult> tryAcquireAsync(
            BucketIdentity storageKey, Limit limit, Algorithm algorithm, long weight) {
        return tryAcquireAll(List.of(new LevelRequest(storageKey, limit, algorithm, weight))).thenApply(result ->
                result.singleResult());
    }
    @Override public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
        List<LevelRequest> requests = List.copyOf(chain); LevelRequest.validateChain(requests);
        BoundContext binding = bound(requests.get(0).storageKey().domain());
        return tryAcquireAll(binding.context(), requests, false, binding.pending());
    }
    /** Context-free seeders cannot establish recovery; the bound controller must authorize this write. */
    @Override public CompletionStage<Void> seed(QuotaDomain domain, List<BucketState> buckets) {
        BoundContext binding = bound(domain);
        return seed(binding.context(), buckets).thenApply(applied -> {
            if (!applied) throw new IllegalStateException("seed recovery context is stale; no bucket was changed");
            return null;
        });
    }

    /** Context-bound atomic acquisition. The coordinator supplies a shared, generation-scoped pending signal. */
    public CompletionStage<ChainResult> tryAcquireAll(RecoveryContext context, List<LevelRequest> chain,
                                                     boolean guarded, RecoveryPending pending) {
        if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("Redis store is closed"));
        List<LevelRequest> requests = List.copyOf(chain);
        LevelRequest.validateChain(requests);
        if (!context.domain().equals(pending.domain())) throw new IllegalArgumentException("pending domain mismatch");
        for (LevelRequest request : requests) {
            if (request.resolverRevision() != context.resolverRevision() || !request.resolverFingerprint().equals(context.resolverFingerprint())
                    || (request.configurationFingerprint() != null && !request.configurationFingerprint().equals(context.configurationFingerprint())))
                return CompletableFuture.completedFuture(ChainResult.pending(0, pending));
        }
        List<String> keys = new ArrayList<>();
        keys.add(keyScheme.controlKey(context.domain()));
        List<String> args = operationHeader(context, guarded);
        for (LevelRequest request : requests) {
            if (!request.storageKey().domain().equals(context.domain())) throw new IllegalArgumentException("operation domain mismatch");
            keys.add(keyScheme.singleKey(request.storageKey()));
            args.addAll(List.of(algorithmTag(request.algorithm()), Long.toString(request.limit().capacity()),
                    Long.toString(request.limit().emissionIntervalNanos()), ParameterFingerprint.of(request.algorithm(), request.limit()),
                    Long.toString(request.weight())));
        }
        return evalRegistered(guardedChain, keys.toArray(String[]::new), args.toArray(String[]::new),
                requests.stream().map(r -> PolicyBinding.of(r.storageKey(), r.algorithm())).toList())
                .thenApply(reply -> number(reply, 0) == -20 ? ChainResult.pending(0, pending) : toChainResult(reply, requests.size()));
    }

    /** A stale context returns false without normalizing or writing any bucket. */
    public CompletionStage<Boolean> seed(RecoveryContext context, List<BucketState> buckets) {
        if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("Redis store is closed"));
        List<BucketState> snapshot = List.copyOf(buckets);
        List<String> keys = new ArrayList<>(); keys.add(keyScheme.controlKey(context.domain()));
        List<String> args = operationHeader(context, false);
        java.util.Set<BucketIdentity> seen = new java.util.HashSet<>();
        for (BucketState bucket : snapshot) {
            if (!bucket.storageKey().domain().equals(context.domain()) || !seen.add(bucket.storageKey()))
                throw new IllegalArgumentException("seed domain mismatch or duplicate identity");
            keys.add(keyScheme.singleKey(bucket.storageKey()));
            args.addAll(List.of(algorithmTag(bucket.algorithm()), Long.toString(bucket.limit().capacity()),
                    Long.toString(bucket.limit().emissionIntervalNanos()), ParameterFingerprint.of(bucket.algorithm(), bucket.limit()),
                    Long.toString(bucket.remaining())));
        }
        return registerPolicies(snapshot.stream().map(b -> PolicyBinding.of(b.storageKey(), b.algorithm())).toList())
                .thenCompose(ignored -> evalWithFallback(guardedSeed, keys.toArray(String[]::new), args.toArray(String[]::new)))
                .thenApply(reply -> number(reply, 0) != -20);
    }

    private static List<String> operationHeader(RecoveryContext c, boolean guarded) {
        return new ArrayList<>(List.of("qf-recovery-v1", c.session().cohortIncarnation(), c.session().cohortDigest(),
                Integer.toString(c.session().slot()), Long.toString(c.session().generation()), c.session().token(),
                Long.toString(c.epoch()), Long.toString(c.dispatchGeneration()), Long.toString(c.configurationVersion()),
                c.configurationFingerprint(), c.phase().name(), guarded ? "1" : "0",
                Long.toString(c.resolverRevision()), c.resolverFingerprint()));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        boundContexts.values().forEach(binding -> binding.changed().completeExceptionally(new java.util.concurrent.CancellationException("Redis store closed")));
        boundContexts.clear();
        if (closeConnection) {
            connection.close();
        }
        if (ownedClient != null) {
            ownedClient.shutdown();
        }
    }

    private String[] limitArgs(Limit limit, Algorithm algorithm, long weight) {
        return new String[] {
            Long.toString(limit.capacity()),
            Long.toString(limit.emissionIntervalNanos()),
            ParameterFingerprint.of(algorithm, limit),
            Long.toString(weight)
        };
    }

    private CompletionStage<java.util.List<Object>> evalWithFallback(
            LuaScript script, String[] keys, String[] args) {
        CompletableFuture<java.util.List<Object>> attempt = evalsha(script, keys, args);
        CompletableFuture<java.util.List<Object>> recovered = attempt.handle((value, error) -> {
            if (error == null) {
                return CompletableFuture.completedFuture(value);
            }
            if (isNoScript(error)) {
                return eval(script, keys, args);
            }
            verifiedBindings.clear();
            return CompletableFuture.<java.util.List<Object>>failedFuture(error);
        }).thenCompose(stage -> stage);
        return recovered.orTimeout(config.commandTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .handle((reply, error) -> {
                    if (error == null) return reply;
                    verifiedBindings.clear();
                    Throwable cause = error;
                    while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
                    if (cause instanceof RedisCommandExecutionException && cause.getMessage() != null
                            && (cause.getMessage().contains("QF_STATE") || cause.getMessage().contains("QF_RECOVERY_"))) {
                        throw new StateCompatibilityException("quota state is incompatible or corrupt; drain migration or operator repair is required");
                    }
                    throw new CompletionException(cause);
                });
    }

    private CompletableFuture<java.util.List<Object>> evalsha(LuaScript script, String[] keys, String[] args) {
        return async.<java.util.List<Object>>evalsha(script.sha1(), ScriptOutputType.MULTI, keys, args)
                .toCompletableFuture();
    }

    private CompletableFuture<java.util.List<Object>> eval(LuaScript script, String[] keys, String[] args) {
        return async.<java.util.List<Object>>eval(script.source(), ScriptOutputType.MULTI, keys, args)
                .toCompletableFuture();
    }

    private static boolean isNoScript(Throwable error) {
        Throwable cause = error instanceof CompletionException ? error.getCause() : error;
        return cause instanceof RedisNoScriptException
                || (cause instanceof RedisCommandExecutionException execution
                        && execution.getMessage() != null
                        && execution.getMessage().startsWith("NOSCRIPT"));
    }

    private static StoreResult toStoreResult(java.util.List<Object> reply) {
        boolean acquired = number(reply, 0) == 1;
        long remaining = number(reply, 1);
        long retryAfterMillis = number(reply, 2);
        return acquired ? StoreResult.acquired(remaining) : StoreResult.rejected(remaining, retryAfterMillis);
    }

    private static ChainResult toChainResult(java.util.List<Object> reply, int levels) {
        if (reply.size() != 4 + 2 * levels) throw new io.quotaflow.core.store.StateCompatibilityException("invalid chain budget reply shape");
        var budgets = new java.util.ArrayList<io.quotaflow.core.store.LevelBudget>(levels);
        for (int i = 0; i < levels; i++) budgets.add(new io.quotaflow.core.store.LevelBudget(i,
                new io.quotaflow.core.store.StoreBudget(number(reply, 4 + 2 * i), number(reply, 5 + 2 * i), false)));
        boolean acquired = number(reply, 0) == 1;
        int firedLevelIndex = (int) number(reply, 1);
        long remaining = number(reply, 2);
        long retryAfterMillis = number(reply, 3);
        return (acquired
                ? ChainResult.acquired(firedLevelIndex, remaining)
                : ChainResult.rejected(firedLevelIndex, remaining, retryAfterMillis)).withBudgets(budgets);
    }

    private static long number(java.util.List<Object> reply, int index) {
        Object value = reply.get(index);
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalStateException(
                "unexpected Lua reply element at index " + index + ": " + value);
    }

    private static String algorithmTag(Algorithm algorithm) {
        return algorithm == Algorithm.TOKEN_BUCKET ? ALGORITHM_TOKEN_BUCKET : ALGORITHM_GCRA;
    }

}
