package io.quotaflow.store.redis;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Distributed allow path: whole-chain evaluation against a real Redis in one
 * round-trip per decision. Two benchmark shapes feed the {@code benchmarkGate}
 * task: batched asynchronous single-level chain evaluation for the throughput
 * gate (&ge; 50k decisions/s, plus regression versus the versioned baseline —
 * single-level because the measurement is server-CPU-bound and isolates the
 * per-decision cost of the chain script) and a synchronous two-level chain —
 * the realistic policy shape — for the p99 latency baseline. Limits are sized
 * so every call takes the allow path.
 */
@State(Scope.Benchmark)
@Threads(8)
public class RedisChainBenchmark {

    /** Decisions issued per throughput invocation; joined as one batch. */
    static final int BATCH = 256;

    private static final int CHAIN_POOL = 256;

    private GenericContainer<?> redis;
    private RedisRateLimitStore store;
    private List<List<LevelRequest>> singleLevelChains;
    private List<List<LevelRequest>> twoLevelChains;

    @Setup(Level.Trial)
    public void setup() {
        redis = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);
        redis.start();
        store = RedisRateLimitStore.connect(
                "redis://" + redis.getHost() + ':' + redis.getMappedPort(6379),
                // generous command timeout: the throughput benchmark deliberately queues
                // thousands of in-flight evaluations, which the production-style 100 ms
                // bound would surface as timeouts instead of measuring the allow path
                new RedisStoreConfig(Duration.ofSeconds(5), Duration.ofSeconds(30)),
                Duration.ofSeconds(5));
        Limit limit = new Limit(1_000_000_000, 1_000_000_000, Duration.ofSeconds(1));
        singleLevelChains = new ArrayList<>(CHAIN_POOL);
        twoLevelChains = new ArrayList<>(CHAIN_POOL);
        for (int i = 0; i < CHAIN_POOL; i++) {
            singleLevelChains.add(List.of(
                    new LevelRequest("bench:global:" + i, limit, Algorithm.TOKEN_BUCKET, 1)));
            twoLevelChains.add(List.of(
                    new LevelRequest("bench:global:" + i, limit, Algorithm.TOKEN_BUCKET, 1),
                    new LevelRequest("bench:tenant:" + i, limit, Algorithm.TOKEN_BUCKET, 1)));
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        store.close();
        redis.stop();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @OperationsPerInvocation(BATCH)
    public void chainAllowThroughput(Blackhole blackhole) {
        CompletableFuture<ChainResult>[] batch = new CompletableFuture[BATCH];
        for (int i = 0; i < BATCH; i++) {
            batch[i] = store.tryAcquireAll(randomChain(singleLevelChains)).toCompletableFuture();
        }
        blackhole.consume(CompletableFuture.allOf(batch).join());
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public ChainResult chainAllowLatency() {
        return store.tryAcquireAll(randomChain(twoLevelChains)).toCompletableFuture().join();
    }

    private List<LevelRequest> randomChain(List<List<LevelRequest>> pool) {
        return pool.get(ThreadLocalRandom.current().nextInt(CHAIN_POOL));
    }
}
