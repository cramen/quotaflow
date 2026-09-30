package io.quotaflow.store.redis;

import static io.quotaflow.testing.TestIdentities.key;
import io.quotaflow.core.store.BucketIdentity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.StoreResult;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Parity between the Lua algorithms and {@link LocalRateLimitStore} as the
 * reference oracle (deterministic injectable clock).
 *
 * <p>A test-only wrapper captures Redis TIME once and supplies that same value to the
 * production numeric script and the local oracle. Scheduling delays between separate
 * commands cannot change the expected result; verdict, remaining and retry are exact.
 */
class RedisLocalParityTest extends RedisContainerSupport {

    // one token per second; five seconds to refill a full bucket
    private static final Limit LIMIT = new Limit(5, 5, Duration.ofSeconds(5));

    private static final long RETRY_AFTER_TOLERANCE_MILLIS = 0;

    private record Step(long gapMillis, long weight) {
    }

    private record ServerCall(long serverMicros, List<Object> reply) {
    }

    @Test
    void tokenBucketMatchesOracleOnScriptedSequences() {
        runParity(Algorithm.TOKEN_BUCKET, List.of(
                // burst to exhaustion, then two rejections
                new Step(0, 1), new Step(0, 1), new Step(0, 1), new Step(0, 1), new Step(0, 1),
                new Step(0, 1), new Step(0, 1),
                // full-capacity weighted acquisition from a refilled bucket
                new Step(1300, 5),
                // partial refill admits smaller weights only
                new Step(1300, 2), new Step(0, 2), new Step(0, 1)));
    }

    @Test
    void gcraMatchesOracleOnScriptedSequences() {
        runParity(Algorithm.GCRA, List.of(
                new Step(0, 1), new Step(0, 1), new Step(0, 1), new Step(0, 1), new Step(0, 1),
                new Step(0, 1), new Step(0, 1),
                new Step(1300, 5),
                new Step(1300, 2), new Step(0, 2), new Step(0, 1)));
    }

    @Test
    void tokenBucketMatchesOracleOnPseudoRandomSequences() {
        runParity(Algorithm.TOKEN_BUCKET, pseudoRandomSteps(20260909L, 14));
    }

    @Test
    void gcraMatchesOracleOnPseudoRandomSequences() {
        runParity(Algorithm.GCRA, pseudoRandomSteps(20260910L, 14));
    }

    /**
     * Fixed-seed pseudo-random mixes of weights and arrival gaps. Gaps are
     * multiples of 10 ms offset from the 1 s emission grid, so no step lands
     * within 10 ms of a refill boundary while the two clocks differ by only
     * microseconds.
     */
    private static List<Step> pseudoRandomSteps(long seed, int count) {
        Random random = new Random(seed);
        long[] gaps = {0, 313, 727, 1189};
        return random.ints(count, 0, gaps.length * 3)
                .mapToObj(draw -> new Step(gaps[draw % gaps.length], draw / gaps.length + 1L))
                .toList();
    }

    private void runParity(Algorithm algorithm, List<Step> steps) {
        LuaScript script = LuaScript.load(
                algorithm == Algorithm.TOKEN_BUCKET ? "/lua/token_bucket.lua" : "/lua/gcra.lua");
        String storageKey = storageKey("parity", algorithm.name().toLowerCase(), "key");
        String redisKey = RedisKeyScheme.defaults().singleKey(key(storageKey));
        String[] keys = {redisKey};

        AtomicLong oracleClock = new AtomicLong();
        LocalRateLimitStore oracle = new LocalRateLimitStore(oracleClock::get);

        try (StatefulRedisConnection<String, String> connection = client().connect()) {
            RedisAsyncCommands<String, String> async = connection.async();
            connection.setAutoFlushCommands(false);
            try {
                for (int i = 0; i < steps.size(); i++) {
                    Step step = steps.get(i);
                    if (step.gapMillis() > 0) {
                        sleepUninterruptibly(step.gapMillis());
                    }
                    String[] args = {
                        Long.toString(LIMIT.capacity()),
                        Long.toString(LIMIT.emissionIntervalNanos()),
                        ParameterFingerprint.of(algorithm, LIMIT),
                        Long.toString(step.weight())
                    };
                    ServerCall call = timeThenEval(connection, script, keys, args);

                    oracleClock.set(call.serverMicros() * 1_000);
                    StoreResult expected =
                            oracle.tryAcquire(key(storageKey), LIMIT, algorithm, step.weight());
                    StoreResult actual = toResult(call.reply());

                    String context = "step " + i + " (gap " + step.gapMillis() + " ms, weight "
                            + step.weight() + ", " + algorithm + ')';
                    assertEquals(expected.acquired(), actual.acquired(), "verdict: " + context);
                    assertEquals(expected.remaining(), actual.remaining(), "remaining: " + context);
                    if (!expected.acquired()) {
                        long delta = Math.abs(expected.retryAfterMillis() - actual.retryAfterMillis());
                        assertTrue(delta <= RETRY_AFTER_TOLERANCE_MILLIS,
                                "retryAfter delta " + delta + " ms: " + context);
                    }
                }
            } finally {
                connection.setAutoFlushCommands(true);
            }
        }
    }

    /** Shares one server clock observation with the numeric implementation and its oracle. */
    private static ServerCall timeThenEval(
            StatefulRedisConnection<String, String> connection, LuaScript script, String[] keys, String[] args) {
        String instrumented = "local parityTime = redis.call('TIME')\nlocal function evaluate()\n"
                + script.source().replace("redis.call('TIME')", "parityTime")
                + "\nend\nlocal result = evaluate()\nreturn {parityTime[1], parityTime[2], result[1], result[2], result[3]}";
        RedisFuture<List<Object>> evaluation = connection.async().eval(instrumented, ScriptOutputType.MULTI, keys, args);
        connection.flushCommands();
        try {
            List<Object> reply = evaluation.get();
            long serverMicros = Long.parseLong((String) reply.get(0)) * 1_000_000 + Long.parseLong((String) reply.get(1));
            return new ServerCall(serverMicros, reply.subList(2, 5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting script reply", e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException("script execution failed", e.getCause());
        }
    }

    private static StoreResult toResult(List<Object> reply) {
        boolean acquired = ((Number) reply.get(0)).longValue() == 1;
        long remaining = ((Number) reply.get(1)).longValue();
        long retryAfterMillis = ((Number) reply.get(2)).longValue();
        return acquired ? StoreResult.acquired(remaining) : StoreResult.rejected(remaining, retryAfterMillis);
    }

    private static void sleepUninterruptibly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted during parity pacing", e);
        }
    }
}
