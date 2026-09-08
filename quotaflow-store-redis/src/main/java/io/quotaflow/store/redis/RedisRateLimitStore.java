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
import io.quotaflow.core.store.BatchRateLimitStore;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.StoreResult;
import java.util.List;
import java.util.Objects;
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
public final class RedisRateLimitStore implements BatchRateLimitStore, AutoCloseable {

    private static final String ALGORITHM_TOKEN_BUCKET = "tb";
    private static final String ALGORITHM_GCRA = "gcra";

    private final StatefulConnection<String, String> connection;
    private final RedisScriptingAsyncCommands<String, String> async;
    private final boolean closeConnection;
    private final RedisStoreConfig config;
    private final RedisKeyScheme keyScheme;
    private final LuaScript tokenBucketScript;
    private final LuaScript gcraScript;
    private final LuaScript chainScript;

    /** Store over a caller-managed standalone connection; {@link #close()} does not close it. */
    public RedisRateLimitStore(StatefulRedisConnection<String, String> connection, RedisStoreConfig config) {
        this(connection, connection.async(), config, RedisKeyScheme.defaults(), false);
    }

    /** Store over a caller-managed standalone connection with a custom key scheme. */
    public RedisRateLimitStore(
            StatefulRedisConnection<String, String> connection,
            RedisStoreConfig config,
            RedisKeyScheme keyScheme) {
        this(connection, connection.async(), config, keyScheme, false);
    }

    /** Store over a caller-managed Cluster connection; {@link #close()} does not close it. */
    public RedisRateLimitStore(
            StatefulRedisClusterConnection<String, String> connection, RedisStoreConfig config) {
        this(connection, connection.async(), config, RedisKeyScheme.defaults(), false);
    }

    /** Opens a dedicated connection on {@code client}; {@link #close()} closes it. */
    public static RedisRateLimitStore create(RedisClient client, RedisStoreConfig config) {
        Objects.requireNonNull(client, "client");
        StatefulRedisConnection<String, String> connection = client.connect();
        return new RedisRateLimitStore(connection, connection.async(), config, RedisKeyScheme.defaults(), true);
    }

    /** Opens a dedicated connection on the Cluster {@code client}; {@link #close()} closes it. */
    public static RedisRateLimitStore create(RedisClusterClient client, RedisStoreConfig config) {
        Objects.requireNonNull(client, "client");
        StatefulRedisClusterConnection<String, String> connection = client.connect();
        return new RedisRateLimitStore(connection, connection.async(), config, RedisKeyScheme.defaults(), true);
    }

    private RedisRateLimitStore(
            StatefulConnection<String, String> connection,
            RedisScriptingAsyncCommands<String, String> async,
            RedisStoreConfig config,
            RedisKeyScheme keyScheme,
            boolean closeConnection) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.async = Objects.requireNonNull(async, "async");
        this.config = Objects.requireNonNull(config, "config");
        this.keyScheme = Objects.requireNonNull(keyScheme, "keyScheme");
        this.closeConnection = closeConnection;
        this.tokenBucketScript = LuaScript.load("/lua/token_bucket.lua");
        this.gcraScript = LuaScript.load("/lua/gcra.lua");
        this.chainScript = LuaScript.load("/lua/chain.lua");
    }

    @Override
    public StoreResult tryAcquire(String storageKey, Limit limit, Algorithm algorithm, long weight) {
        return tryAcquireAsync(storageKey, limit, algorithm, weight).toCompletableFuture().join();
    }

    @Override
    public CompletionStage<StoreResult> tryAcquireAsync(
            String storageKey, Limit limit, Algorithm algorithm, long weight) {
        Objects.requireNonNull(storageKey, "storageKey");
        Objects.requireNonNull(limit, "limit");
        Objects.requireNonNull(algorithm, "algorithm");
        if (weight < 1) {
            throw new IllegalArgumentException("weight must be >= 1, got " + weight);
        }
        LuaScript script = algorithm == Algorithm.TOKEN_BUCKET ? tokenBucketScript : gcraScript;
        String[] keys = {keyScheme.singleKey(storageKey)};
        String[] args = limitArgs(limit, weight);
        return evalWithFallback(script, keys, args).thenApply(RedisRateLimitStore::toStoreResult);
    }

    @Override
    public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
        Objects.requireNonNull(chain, "chain");
        if (chain.isEmpty()) {
            throw new IllegalArgumentException("chain must not be empty");
        }
        List<String> storageKeys = chain.stream().map(LevelRequest::storageKey).toList();
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
        return evalWithFallback(chainScript, keys, args).thenApply(RedisRateLimitStore::toChainResult);
    }

    @Override
    public void close() {
        if (closeConnection) {
            connection.close();
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
            return CompletableFuture.<java.util.List<Object>>failedFuture(error);
        }).thenCompose(stage -> stage);
        return recovered.orTimeout(config.commandTimeout().toMillis(), TimeUnit.MILLISECONDS);
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
