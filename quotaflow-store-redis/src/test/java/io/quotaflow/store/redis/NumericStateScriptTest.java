package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.*;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import java.math.BigInteger;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class NumericStateScriptTest extends RedisContainerSupport {
    private static BucketIdentity key(String policy) {
        return new BucketIdentity(new QuotaDomain("default", "numeric"), policy, Scope.USER, "value");
    }
    private static String[] args(Algorithm algorithm, Limit limit, long weight) {
        return new String[]{Long.toString(limit.capacity()), Long.toString(limit.emissionIntervalNanos()),
                ParameterFingerprint.of(algorithm, limit), Long.toString(weight)};
    }
    private static String script(Algorithm algorithm) {
        String source = LuaScript.load(algorithm == Algorithm.GCRA ? "/lua/gcra.lua" : "/lua/token_bucket.lua").source();
        assertTrue(source.contains("local t = redis.call('TIME')"));
        // Only the time provider changes: every arithmetic/codec branch is production code.
        return "local testMicros = table.remove(ARGV); local testSeconds = table.remove(ARGV);\n"
                + source.replace("local t = redis.call('TIME')", "local t = {testSeconds, testMicros}");
    }
    private static List<Object> evaluate(StatefulRedisConnection<String,String> connection, String source,
                                         String key, String[] params, long sec, long micros) {
        List<String> args = new ArrayList<>(List.of(params));
        args.add(Long.toString(sec)); args.add(Long.toString(micros));
        return connection.sync().eval(source, ScriptOutputType.MULTI, new String[]{key}, args.toArray(String[]::new));
    }
    @Test void generatedFractionalHistoriesMatchBigIntegerDebtAtLargeOrigins() {
        try (var connection = client().connect()) {
            for (Algorithm algorithm : Algorithm.values()) {
                String source = script(algorithm);
                for (int seed = 0; seed < 12; seed++) {
                    Random random = new Random(seed);
                    long capacity = seed == 0 ? Limit.MAX_TOKENS : 1 + random.nextInt(25);
                    Limit limit = seed == 0 ? new Limit(capacity, 1_000_000, Duration.ofSeconds(1))
                            : new Limit(capacity, 7 + random.nextInt(77), Duration.ofSeconds(1));
                    BigInteger period = BigInteger.valueOf(limit.refillPeriod().toNanos());
                    BigInteger rate = BigInteger.valueOf(limit.refillAmount());
                    BigInteger interval = period.add(rate).subtract(BigInteger.ONE).divide(rate);
                    BigInteger maximum = interval.multiply(BigInteger.valueOf(capacity));
                    BigInteger debt = BigInteger.ZERO;
                    long elapsedMicros = 0;
                    long origin = seed % 2 == 0 ? 4_000_000_000_000L : 10;
                    String bucket = "numeric:" + algorithm + ":" + seed;
                    for (int step = 0; step < 100; step++) {
                        long gap = random.nextInt(50_000);
                        elapsedMicros += gap;
                        debt = debt.subtract(BigInteger.valueOf(gap * 1000)).max(BigInteger.ZERO);
                        long weight = seed == 0 ? 1 : 1 + random.nextInt((int) capacity + 1);
                        BigInteger candidate = debt.add(interval.multiply(BigInteger.valueOf(weight)));
                        boolean allowed = candidate.compareTo(maximum) <= 0;
                        if (allowed) debt = candidate;
                        var result = evaluate(connection, source, bucket, args(algorithm, limit, weight),
                                origin + elapsedMicros / 1_000_000, elapsedMicros % 1_000_000);
                        String trace = "seed=" + seed + ", first failing prefix=" + (step + 1) + ", algorithm=" + algorithm;
                        assertEquals(allowed ? 1L : 0L, result.get(0), trace);
                        assertEquals(maximum.subtract(debt).divide(interval).longValueExact(), result.get(1), trace);
                        if (weight > capacity) assertEquals(0L, result.get(2));
                    }
                }
            }
        }
    }
    @Test void freshSevenPerSecondWorksThroughRealServerTimeAndBindingsSurviveRestart() {
        try (var connection = client().connect(); var store = newStore(connection)) {
            var key = key("fresh");
            var limit = new Limit(1, 7, Duration.ofSeconds(1));
            assertTrue(store.tryAcquire(key, limit, Algorithm.GCRA, 1).acquired());
            assertFalse(store.tryAcquire(key, limit, Algorithm.GCRA, 1).acquired());
            try (var restarted = newStore(connection)) {
                var error = assertThrows(CompletionException.class,
                        () -> restarted.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1));
                assertInstanceOf(PolicyConfigurationException.class, error.getCause());
                assertThrows(CompletionException.class, () -> restarted.registerPolicies(List.of(
                        PolicyBinding.of(key, Algorithm.TOKEN_BUCKET))).toCompletableFuture().join());
            }
        }
    }
    @Test void malformedFinalMemberLeavesEntireChainAndSeedBatchUnchanged() {
        try (var connection = client().connect(); var store = newStore(connection)) {
            var first = key("first"); var last = key("last");
            var limit = new Limit(10, 1, Duration.ofSeconds(100));
            store.tryAcquire(first, limit, Algorithm.GCRA, 2);
            String parent = RedisKeyScheme.defaults().singleKey(first);
            String child = RedisKeyScheme.defaults().singleKey(last);
            String before = connection.sync().get(parent);
            String digest = ParameterFingerprint.of(Algorithm.GCRA, limit);
            for (String corrupt : List.of("legacy:0", "4:gcra:" + digest + ":1:0:1:0",
                    "3:tb:" + digest + ":1:0:1:0", "3:gcra:" + digest + ":1000000001:0:1:0",
                    "3:gcra:" + digest + ":1:2764800000000000:1:0", "3:gcra:invalid:1:0:1:0",
                    "3:gcra:" + digest + ":1:0:1:1000000000", "3:gcra:" + digest + ":11:0:1:0")) {
                connection.sync().set(child, corrupt);
                var chain = List.of(new LevelRequest(first, limit, Algorithm.GCRA, 1), new LevelRequest(last, limit, Algorithm.GCRA, 1));
                assertInstanceOf(StateCompatibilityException.class, assertThrows(CompletionException.class,
                        () -> store.tryAcquireAll(chain).toCompletableFuture().join()).getCause());
                assertEquals(before, connection.sync().get(parent));
                assertEquals(corrupt, connection.sync().get(child));
                assertInstanceOf(StateCompatibilityException.class, assertThrows(CompletionException.class,
                        () -> store.seed(first.domain(), List.of(new BucketState(first, limit, Algorithm.GCRA, 0),
                                new BucketState(last, limit, Algorithm.GCRA, 0))).toCompletableFuture().join()).getCause());
                assertEquals(before, connection.sync().get(parent));
                assertEquals(corrupt, connection.sync().get(child));
            }
        }
    }
    @Test void transitionClampsOldBalanceAndRejectionPersistsNewSchedule() {
        try (var connection = client().connect()) {
            for (Algorithm algorithm : Algorithm.values()) {
                String source = script(algorithm); String key = "transition:" + algorithm;
                var old = new Limit(10, 1, Duration.ofSeconds(1));
                var smaller = new Limit(5, 7, Duration.ofSeconds(1));
                evaluate(connection, source, key, args(algorithm, old, 2), 10, 0);
                String tag = algorithm == Algorithm.GCRA ? "gcra" : "tb";
                connection.sync().set(key, "3:" + tag + ":" + ParameterFingerprint.of(algorithm, old) + ":8:500000000:10:0");
                assertEquals(1L, evaluate(connection, source, key, args(algorithm, smaller, 5), 10, 1).get(0));
                var changed = new Limit(20, 2, Duration.ofSeconds(1));
                assertEquals(0L, evaluate(connection, source, key, args(algorithm, changed, 1), 10, 2).get(0));
                assertEquals(0L, evaluate(connection, source, key, args(algorithm, changed, 1), 10, 400002).get(0));
                assertEquals(1L, evaluate(connection, source, key, args(algorithm, changed, 1), 10, 500002).get(0));
                String[] fields = connection.sync().get(key).split(":");
                assertEquals(ParameterFingerprint.of(algorithm, changed), fields[2]);
                assertEquals("0", fields[3]);
            }
        }
    }
    @Test void backwardTimeAndMaximumIntermediateStayExact() {
        try (var connection = client().connect()) {
            for (Algorithm algorithm : Algorithm.values()) {
                String source = script(algorithm); String key = "backward:" + algorithm;
                var limit = new Limit(1, 1, Duration.ofSeconds(1));
                evaluate(connection, source, key, args(algorithm, limit, 1), 100, 0);
                assertEquals(0L, evaluate(connection, source, key, args(algorithm, limit, 1), 99, 0).get(0));
                assertEquals(0L, evaluate(connection, source, key, args(algorithm, limit, 1), 100, 0).get(0));
                assertEquals(1L, evaluate(connection, source, key, args(algorithm, limit, 1), 101, 0).get(0));
                var maximum = new Limit(1, 1, Duration.ofDays(32));
                String digest = ParameterFingerprint.of(algorithm, maximum);
                String tag = algorithm == Algorithm.GCRA ? "gcra" : "tb";
                connection.sync().set(key, "3:" + tag + ":" + digest + ":0:2764799999999999:1:0");
                assertEquals(1L, evaluate(connection, source, key, args(algorithm, maximum, 1), 2764801, 0).get(0));
                assertEquals("0", connection.sync().get(key).split(":")[4]);
            }
        }
    }
    @Test void seedUsesTargetFingerprintAndMixedConfigurationsCannotRestoreSpentCredit() {
        try (var connection = client().connect(); var store = newStore(connection)) {
            var first = key("mixed-parent"); var child = key("mixed-child");
            var old = new Limit(10, 1, Duration.ofHours(1));
            var changed = new Limit(20, 1, Duration.ofHours(1));
            store.tryAcquire(first, old, Algorithm.GCRA, 8);
            store.tryAcquire(child, old, Algorithm.GCRA, 10);
            var rejected = store.tryAcquireAll(List.of(new LevelRequest(first, changed, Algorithm.GCRA, 1),
                    new LevelRequest(child, old, Algorithm.GCRA, 1))).toCompletableFuture().join();
            assertFalse(rejected.acquired());
            String redisKey = RedisKeyScheme.defaults().singleKey(first);
            assertEquals("2", connection.sync().get(redisKey).split(":")[3]);
            assertEquals(ParameterFingerprint.of(Algorithm.GCRA, changed), connection.sync().get(redisKey).split(":")[2]);
            store.seed(first.domain(), List.of(new BucketState(first, changed, Algorithm.GCRA, 5))).toCompletableFuture().join();
            assertTrue(store.tryAcquire(first, changed, Algorithm.GCRA, 2).acquired());
            for (int i = 0; i < 10; i++) {
                var target = i % 2 == 0 ? old : changed;
                store.seed(first.domain(), List.of(new BucketState(first, target, Algorithm.GCRA, 5))).toCompletableFuture().join();
                assertFalse(store.tryAcquire(first, target, Algorithm.GCRA, 1).acquired());
                assertEquals(ParameterFingerprint.of(Algorithm.GCRA, target), connection.sync().get(redisKey).split(":")[2]);
            }
            assertThrows(IllegalArgumentException.class, () -> store.seed(first.domain(), List.of(
                    new BucketState(first, changed, Algorithm.GCRA, 0), new BucketState(first, changed, Algorithm.GCRA, 5))));
        }
    }

}
