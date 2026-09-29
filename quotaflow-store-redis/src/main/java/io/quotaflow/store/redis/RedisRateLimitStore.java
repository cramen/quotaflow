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
public final class RedisRateLimitStore implements BatchRateLimitStore, StateSeeder, AutoCloseable {

    private static final String ALGORITHM_TOKEN_BUCKET = "tb";
    private static final String ALGORITHM_GCRA = "gcra";

    private final StatefulConnection<String, String> connection;
    private final RedisScriptingAsyncCommands<String, String> async;
    private final boolean closeConnection;
    private final RedisClient ownedClient;
    private final RedisStoreConfig config;
    private final RedisKeyScheme keyScheme;
    private final LuaScript tokenBucketScript;
    private final LuaScript gcraScript;
    private final LuaScript chainScript;
    private final LuaScript seedScript;
    private final LuaScript registerScript;
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
            throw new IllegalStateException("Redis client initialization failed with a linkage error ("
                    + e + "), indicating a broken netty native-transport mix on the classpath; both"
                    + " native transports (" + RedisClientFactory.EPOLL_PROPERTY + ", "
                    + RedisClientFactory.KQUEUE_PROPERTY + ") have been set to false, so newly created"
                    + " clients in this JVM use NIO — or align the netty native-transport jars with the"
                    + " netty core version", e);
        }
    }

    private static RedisRateLimitStore connectOnce(String url, RedisStoreConfig config, Duration connectTimeout) {
        RedisClient client = RedisClientFactory.createClient(url, connectTimeout);
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
        this.tokenBucketScript = LuaScript.load("/lua/token_bucket.lua");
        this.gcraScript = LuaScript.load("/lua/gcra.lua");
        this.chainScript = LuaScript.load("/lua/chain.lua");
        this.seedScript = LuaScript.load("/lua/seed.lua");
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
            args[2 * i + 1] = binding.scope().wireName() + ":" + RedisKeyScheme.domainDigest(binding.domain());
        }
        return evalWithFallback(registerScript, new String[] {keyScheme.manifestKey(namespace)}, args)
                .thenApply(reply -> {
                    long result = number(reply, 0);
                    if (result != 1) {
                        throw new PolicyConfigurationException(switch ((int) result) {
                            case -1 -> "quota namespace is not ready; explicit provisioning is required";
                            case -2 -> "policy scope or root domain conflicts with the namespace binding";
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
            LuaScript script, String[] keys, String[] args, List<BucketIdentity> buckets) {
        List<PolicyBinding> missing = null;
        for (BucketIdentity bucket : buckets) {
            PolicyBinding binding = PolicyBinding.of(bucket);
            if (!verifiedBindings.contains(binding)) {
                if (missing == null) missing = new ArrayList<>();
                missing.add(binding);
            }
        }
        if (missing == null) return evalWithFallback(script, keys, args);
        return registerPolicies(missing).thenCompose(ignored -> evalWithFallback(script, keys, args));
    }

    @Override
    public StoreResult tryAcquire(BucketIdentity storageKey, Limit limit, Algorithm algorithm, long weight) {
        return tryAcquireAsync(storageKey, limit, algorithm, weight).toCompletableFuture().join();
    }

    @Override
    public CompletionStage<StoreResult> tryAcquireAsync(
            BucketIdentity storageKey, Limit limit, Algorithm algorithm, long weight) {
        Objects.requireNonNull(storageKey, "storageKey");
        Objects.requireNonNull(limit, "limit");
        Objects.requireNonNull(algorithm, "algorithm");
        if (weight < 1) {
            throw new IllegalArgumentException("weight must be >= 1, got " + weight);
        }
        LuaScript script = algorithm == Algorithm.TOKEN_BUCKET ? tokenBucketScript : gcraScript;
        String[] keys = {keyScheme.singleKey(storageKey)};
        String[] args = limitArgs(limit, weight);
        return evalRegistered(script, keys, args, List.of(storageKey))
                .thenApply(RedisRateLimitStore::toStoreResult);
    }

    @Override
    public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
        LevelRequest.validateChain(chain);
        List<BucketIdentity> storageKeys = chain.stream().map(LevelRequest::storageKey).toList();
        String[] keys = keyScheme.chainKeys(storageKeys).toArray(String[]::new);
        String[] args = new String[chain.size() * 5];
        for (int i = 0; i < chain.size(); i++) {
            LevelRequest level = chain.get(i);
            int base = i * 5;
            args[base] = algorithmTag(level.algorithm());
            args[base + 1] = Long.toString(level.limit().capacity());
            args[base + 2] = Long.toString(level.limit().refillAmount());
            args[base + 3] = periodMicros(level.limit());
            args[base + 4] = Long.toString(level.weight());
        }
        return evalRegistered(chainScript, keys, args, storageKeys)
                .thenApply(RedisRateLimitStore::toChainResult);
    }

    /**
     * Best-effort recovery seeding: one pipelined {@code seed.lua} execution
     * per bucket, each merging the local remaining conservatively into the
     * stored state (never increasing remaining). Keys are mapped exactly like
     * the chain script maps them, so seeded state is what post-recovery chain
     * evaluations read. Each write is bounded by the configured command
     * timeout; the stage completes exceptionally if any write fails.
     */
    @Override
    public CompletionStage<Void> seed(QuotaDomain domain, List<BucketState> buckets) {
        Objects.requireNonNull(domain, "domain");
        Objects.requireNonNull(buckets, "buckets");
        if (buckets.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<BucketState> snapshot = List.copyOf(buckets);
        // Validate the entire batch before the first write, including its domain.
        for (BucketState bucket : snapshot) keyScheme.chainLevelKey(domain, bucket.storageKey());
        return registerPolicies(snapshot.stream().map(bucket -> PolicyBinding.of(bucket.storageKey())).distinct().toList())
                .thenCompose(ignored -> seedRegistered(snapshot));
    }

    private CompletionStage<Void> seedRegistered(List<BucketState> buckets) {
        List<CompletableFuture<java.util.List<Object>>> writes = new ArrayList<>(buckets.size());
        for (BucketState bucket : buckets) {
            String key = keyScheme.singleKey(bucket.storageKey());
            String[] args = {
                algorithmTag(bucket.algorithm()),
                Long.toString(bucket.limit().capacity()),
                Long.toString(bucket.limit().refillAmount()),
                periodMicros(bucket.limit()),
                Long.toString(bucket.remaining())
            };
            writes.add(evalWithFallback(seedScript, new String[] {key}, args).toCompletableFuture());
        }
        return CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new));
    }

    @Override
    public void close() {
        if (closeConnection) {
            connection.close();
        }
        if (ownedClient != null) {
            ownedClient.shutdown();
        }
    }

    private String[] limitArgs(Limit limit, long weight) {
        return new String[] {
            Long.toString(limit.capacity()),
            Long.toString(limit.refillAmount()),
            periodMicros(limit),
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
                .whenComplete((ignored, error) -> { if (error != null) verifiedBindings.clear(); });
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

    private static ChainResult toChainResult(java.util.List<Object> reply) {
        boolean acquired = number(reply, 0) == 1;
        int firedLevelIndex = (int) number(reply, 1);
        long remaining = number(reply, 2);
        long retryAfterMillis = number(reply, 3);
        return acquired
                ? ChainResult.acquired(firedLevelIndex, remaining)
                : ChainResult.rejected(firedLevelIndex, remaining, retryAfterMillis);
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

    private static String periodMicros(Limit limit) {
        return Long.toString(Math.max(1, limit.refillPeriod().toNanos() / 1_000));
    }
}
