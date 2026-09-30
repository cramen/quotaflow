package io.quotaflow.store.redis;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.BucketIdentity;
import io.quotaflow.core.store.QuotaDomain;
import io.quotaflow.core.store.PolicyBinding;
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
 * Distributed allow path with one genuinely shared provider and many tenants.
 * Both throughput and latency evaluate the same two-level atomic hierarchy;
 * distinct tenants never duplicate the parent's budget or distribute its slot.
 */
@State(Scope.Benchmark)
@Threads(8)
public class RedisChainBenchmark {
    @org.openjdk.jmh.annotations.Param({"TOKEN_BUCKET"})
    public Algorithm algorithm;

    /** Decisions issued per throughput invocation; joined as one batch. */
    static final int BATCH = 256;

    private static final int CHAIN_POOL = 256;

    private GenericContainer<?> redis;
    private RedisRateLimitStore store;
    private List<List<LevelRequest>> twoLevelChains;

    @Setup(Level.Trial)
    public void setup() {
        String url = System.getenv("QUOTAFLOW_BENCHMARK_REDIS_URL");
        if (url == null || url.isBlank()) {
            redis = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);
            redis.start();
            url = "redis://" + redis.getHost() + ':' + redis.getMappedPort(6379);
        }
        String namespace = "benchmark-" + java.util.UUID.randomUUID();
        var provisioningClient = RedisClientFactory.createClient(url, Duration.ofSeconds(5));
        try (var connection = provisioningClient.connect()) {
            new RedisNamespaceAdmin(connection).provisionFresh(namespace, true);
        } finally { provisioningClient.shutdown(); }
        store = RedisRateLimitStore.connect(
                url,
                // generous command timeout: the throughput benchmark deliberately queues
                // thousands of in-flight evaluations, which the production-style 100 ms
                // bound would surface as timeouts instead of measuring the allow path
                new RedisStoreConfig(Duration.ofSeconds(5), Duration.ofSeconds(30)),
                Duration.ofSeconds(5));
        Limit limit = new Limit(1_000_000_000, 1_000_000, Duration.ofSeconds(1));
        twoLevelChains = new ArrayList<>(CHAIN_POOL);
        QuotaDomain domain = new QuotaDomain(namespace, "provider");
        BucketIdentity parent = new BucketIdentity(domain, "provider", io.quotaflow.core.Scope.GLOBAL, "shared");
        store.registerPolicies(List.of(PolicyBinding.of(parent, algorithm),
                new PolicyBinding(domain, "tenant", io.quotaflow.core.Scope.TENANT, algorithm))).toCompletableFuture().join();
        var cohort = io.quotaflow.core.store.RecoveryCohort.single();
        var adminClient = RedisClientFactory.createClient(url, Duration.ofSeconds(5));
        try (var connection = adminClient.connect()) {
            var controller = new RedisRecoveryController(connection, Duration.ofSeconds(5));
            controller.provisionCohort(namespace, cohort, "initial", true, true).toCompletableFuture().join();
            controller.provisionDomain(domain, cohort, "initial", "a".repeat(64), true, true).toCompletableFuture().join();
            var session = controller.enroll(namespace, cohort, "single", java.util.UUID.randomUUID().toString()).toCompletableFuture().join();
            var gather = controller.attach(domain, session).toCompletableFuture().join().context();
            var drain = controller.join(gather).toCompletableFuture().join().context();
            store.bindRecoveryContext(controller.ready(drain).toCompletableFuture().join().context());
        } finally { adminClient.shutdown(); }
        for (int i = 0; i < CHAIN_POOL; i++) {
            twoLevelChains.add(List.of(
                    new LevelRequest(parent, limit, algorithm, 1),
                    new LevelRequest(new BucketIdentity(domain, "tenant", io.quotaflow.core.Scope.TENANT,
                            Integer.toString(i)), limit, algorithm, 1)));
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        store.close();
        if (redis != null) redis.stop();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @OperationsPerInvocation(BATCH)
    public void chainAllowThroughput(Blackhole blackhole) {
        CompletableFuture<ChainResult>[] batch = new CompletableFuture[BATCH];
        for (int i = 0; i < BATCH; i++) {
            batch[i] = store.tryAcquireAll(randomChain(twoLevelChains)).toCompletableFuture();
        }
        CompletableFuture.allOf(batch).join();
        for (CompletableFuture<ChainResult> result : batch) {
            ChainResult decision = result.join();
            if (!decision.acquired()) throw new IllegalStateException("allow-path benchmark exhausted its quota");
            blackhole.consume(decision);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public ChainResult chainAllowLatency() {
        ChainResult decision = store.tryAcquireAll(randomChain(twoLevelChains)).toCompletableFuture().join();
        if (!decision.acquired()) throw new IllegalStateException("allow-path benchmark exhausted its quota");
        return decision;
    }

    private List<LevelRequest> randomChain(List<List<LevelRequest>> pool) {
        return pool.get(ThreadLocalRandom.current().nextInt(CHAIN_POOL));
    }
}
