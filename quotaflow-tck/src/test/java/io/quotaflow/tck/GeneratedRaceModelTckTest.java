package io.quotaflow.tck;

import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.store.redis.RedisStoreConfig;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Searches legal linearizations instead of comparing concurrent results with one arbitrary order. */
class GeneratedRaceModelTckTest extends TckContainers {
    record Operation(int child, long weight) { }
    record Observed(Operation operation, long started, long finished, ChainResult result) { }

    @Test void generatedLocalHistoriesAreLinearizable() throws Exception { verify(new LocalRateLimitStore(), "local"); }
    @Test void generatedRedisHistoriesAreLinearizable() throws Exception { remote(redisUri(), "redis"); }
    @Test void generatedValkeyHistoriesAreLinearizable() throws Exception { remote(valkeyUri(), "valkey"); }

    private void remote(String uri, String backend) throws Exception {
        try (var connection = newClient(uri).connect();
             var store = new io.quotaflow.testing.RecoveryStoreFixture(connection,
                     new RedisStoreConfig(Duration.ofSeconds(5), Duration.ofSeconds(30)))) {
            verify(store, backend);
        }
    }

    private static void verify(BatchRateLimitStore store, String backend) throws Exception {
        var executor = Executors.newFixedThreadPool(6);
        try {
            for (var algorithm : Algorithm.values()) {
                for (long seed = 0; seed < 16; seed++) {
                    var random = new Random(seed);
                    long parentCapacity = 1 + random.nextInt(24), childCapacity = 1 + random.nextInt(24);
                    var operations = new ArrayList<Operation>();
                    for (int i = 0; i < 6; i++) operations.add(new Operation(random.nextInt(4) - 1, 1 + random.nextInt(26)));
                    var observed = execute(store, executor, algorithm, parentCapacity, childCapacity, operations);
                    if (!linearizable(observed, parentCapacity, childCapacity)) {
                        var original = observed;
                        // Re-execute every proposed reduction against fresh buckets; dropping recorded
                        // outcomes alone would manufacture a failure by deleting their preceding debits.
                        for (int i = 0; i < operations.size() && operations.size() > 1;) {
                            var candidate = new ArrayList<>(operations); candidate.remove(i);
                            var replay = execute(store, executor, algorithm, parentCapacity, childCapacity, candidate);
                            if (!linearizable(replay, parentCapacity, childCapacity)) { operations = candidate; observed = replay; }
                            else i++;
                        }
                        Path report = Path.of("build/reports/generated", "race-" + backend + "-" + algorithm + "-" + seed + ".txt");
                        Files.createDirectories(report.getParent());
                        Files.writeString(report, "seed=" + seed + " parentCapacity=" + parentCapacity + " childCapacity=" + childCapacity
                                + "\noriginal=" + original + "\nminimizedReplay=" + observed);
                        fail("No legal quota ordering: " + report.toAbsolutePath());
                    }
                }
            }
        } finally { executor.shutdownNow(); assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS)); }
    }

    private static List<Observed> execute(BatchRateLimitStore store, ExecutorService executor, Algorithm algorithm,
            long parentCapacity, long childCapacity, List<Operation> operations) throws Exception {
        String name = "race-" + UUID.randomUUID(); var domain = new QuotaDomain("default", name);
        var parent = new BucketIdentity(domain, name, Scope.GLOBAL, "shared");
        var rootLimit = new Limit(parentCapacity, parentCapacity, Duration.ofDays(16));
        var leafLimit = new Limit(childCapacity, childCapacity, Duration.ofDays(16));
        java.util.function.Function<Operation, List<LevelRequest>> chain = operation -> {
            var root = new LevelRequest(parent, rootLimit, algorithm, operation.weight());
            if (operation.child() < 0) return List.of(root);
            return List.of(root, new LevelRequest(new BucketIdentity(domain, name + "-leaf", Scope.TENANT,
                    "child-" + operation.child()), leafLimit, algorithm, operation.weight()));
        };
        // Register/recover without spending quota before the concurrent trace.
        assertFalse(store.tryAcquireAll(chain.apply(new Operation(0, Math.max(parentCapacity, childCapacity) + 1)))
                .toCompletableFuture().get(10, TimeUnit.SECONDS).acquired());
        var start = new CyclicBarrier(operations.size()); var clock = new AtomicLong();
        var pending = new ArrayList<Future<Observed>>();
        for (var operation : operations) pending.add(executor.submit(() -> {
            start.await(10, TimeUnit.SECONDS);
            long entered = clock.incrementAndGet();
            var result = store.tryAcquireAll(chain.apply(operation)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            return new Observed(operation, entered, clock.incrementAndGet(), result);
        }));
        var observed = new ArrayList<Observed>();
        for (var result : pending) observed.add(result.get(15, TimeUnit.SECONDS));
        return observed;
    }

    private static boolean linearizable(List<Observed> history, long parent, long child) {
        return search(history, new boolean[history.size()], new long[]{parent, child, child, child},
                new long[]{parent, child, child, child}, 0);
    }

    private static boolean search(List<Observed> history, boolean[] used, long[] balances, long[] capacities, int count) {
        if (count == history.size()) return true;
        for (int index = 0; index < history.size(); index++) {
            if (used[index]) continue;
            var event = history.get(index); boolean eligible = true;
            for (int other = 0; other < history.size(); other++) {
                if (!used[other] && history.get(other).finished() < event.started()) { eligible = false; break; }
            }
            if (!eligible) continue;
            var op = event.operation(); long weight = op.weight(); int child = op.child() + 1;
            // The public weighted-acquisition contract reports an impossible weight at the
            // capacity that can never admit it, rather than promising a refill at another level.
            int impossible = weight > capacities[0] ? 0 : child > 0 && weight > capacities[child] ? 1 : -1;
            int fired = impossible >= 0 ? impossible : balances[0] < weight ? 0 : child > 0 && balances[child] < weight ? 1 : -1;
            var after = balances.clone();
            if (fired < 0) { after[0] -= weight; if (child > 0) after[child] -= weight; }
            long remaining = fired == 0 ? after[0] : fired == 1 ? after[child]
                    : child > 0 ? Math.min(after[0], after[child]) : after[0];
            int expectedLevel = fired < 0 ? (child > 0 ? 1 : 0) : fired;
            var actual = event.result();
            if (actual.recoveryPending() != null || actual.acquired() != (fired < 0)
                    || actual.remaining() != remaining || actual.firedLevelIndex() != expectedLevel
                    || (impossible >= 0 && actual.retryAfterMillis() != 0)) continue;
            used[index] = true;
            if (search(history, used, after, capacities, count + 1)) return true;
            used[index] = false;
        }
        return false;
    }

    @Test void impossibleConcurrentGrantIsRejectedByTheOrderingOracle() {
        var bothGranted = List.of(new Observed(new Operation(-1, 1), 1, 4, ChainResult.acquired(0, 0)),
                new Observed(new Operation(-1, 1), 2, 3, ChainResult.acquired(0, 0)));
        assertFalse(linearizable(bothGranted, 1, 1));
        assertTrue(linearizable(List.of(bothGranted.get(0),
                new Observed(new Operation(-1, 1), 2, 3, ChainResult.rejected(0, 0, 1))), 1, 1));
    }
}
