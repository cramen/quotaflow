package io.quotaflow.verification;

import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.fallback.*;
import io.quotaflow.store.redis.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

/** The same externally compiled workload measures both compared library versions. */
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
public class CertificationBenchmark {
    @Param({"TOKEN_BUCKET", "GCRA"}) public Algorithm algorithm;
    @Param({"local", "distributed-hierarchy", "fallback-facade"}) public String path;
    private DefaultQuotaFlow flow;
    private final List<AutoCloseable> resources = new ArrayList<>();
    private final RateLimitContext context = RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "shared-child").build();

    @Setup public void setup() throws Exception {
        var limit = new Limit(1_000_000_000, 1_000_000, Duration.ofSeconds(1));
        var policies = PolicySet.compile(List.of(
                RateLimitPolicy.builder("provider").scope(io.quotaflow.core.Scope.GLOBAL).algorithm(algorithm).limit(limit).build(),
                RateLimitPolicy.builder("child").scope(io.quotaflow.core.Scope.USER).parentId("provider").algorithm(algorithm).limit(limit).build()));
        String namespace = path.equals("distributed-hierarchy") ? "benchmark-" + UUID.randomUUID() : "default";
        RateLimitStore store;
        FallbackRateLimitStore owner = null;
        io.quotaflow.testing.RecoveryPrimaryFixture fixture = null;
        if (path.equals("local")) store = new LocalRateLimitStore();
        else {
            RecoveryPrimary primary;
            if (path.equals("distributed-hierarchy")) {
                String url = Objects.requireNonNull(System.getenv("QUOTAFLOW_BENCHMARK_REDIS_URL"), "controlled Redis URL");
                var client = RedisClientFactory.createClient(url, Duration.ofSeconds(5)); resources.add(client);
                var connection = client.connect(); resources.add(connection);
                var redis = new RedisRateLimitStore(connection, new RedisStoreConfig(Duration.ofSeconds(5), Duration.ofSeconds(10)));
                resources.add(redis);
                new RedisNamespaceAdmin(connection).provisionFresh(namespace, true);
                var domain = new QuotaDomain(namespace, "provider");
                redis.registerPolicies(policies.policies().stream().map(p -> new PolicyBinding(domain, p.id(), p.scope(), p.algorithm())).toList())
                        .toCompletableFuture().join();
                var admin = new RedisRecoveryController(connection, Duration.ofSeconds(5));
                admin.provisionCohort(namespace, RecoveryCohort.single(), "benchmark", true, true).toCompletableFuture().join();
                admin.provisionDomain(domain, RecoveryCohort.single(), "benchmark", policies.recoveryFingerprint("provider"), true, true)
                        .toCompletableFuture().join();
                primary = redis.recoveryPrimary(namespace, Duration.ofSeconds(5), true);
            } else {
                fixture = new io.quotaflow.testing.RecoveryPrimaryFixture(policies);
                primary = fixture;
            }
            owner = new FallbackRateLimitStore(primary, new RecoverySettings(namespace, "benchmark", "single",
                    RecoveryCohort.single(), 16, 32, Duration.ofMillis(50), Duration.ofSeconds(5)), List.of());
            resources.add(owner); store = owner;
        }
        flow = DefaultQuotaFlow.builder(policies, store).namespace(namespace).build(); resources.add(flow);
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        if (owner != null) {
            while (owner.state() != DegradationState.CLOSED && System.nanoTime() < deadline) Thread.sleep(10);
            if (owner.state() != DegradationState.CLOSED) throw new IllegalStateException("Benchmark enrollment failed");
        }
        if (fixture != null) {
            fixture.available = false;
            flow.tryAcquire("child", context); flow.tryAcquire("child", context);
            Thread.sleep(10);
        }
        acquire();
    }
    private Decision acquire() {
        Decision decision = flow.tryAcquire("child", context);
        if (!decision.isAllowed()) throw new IllegalStateException("Allow-path benchmark did not acquire quota");
        return decision;
    }
    @Benchmark @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.SECONDS)
    public Decision throughput() { return acquire(); }
    @Benchmark @BenchmarkMode(Mode.SampleTime) @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public Decision latency() { return acquire(); }
    @TearDown public void close() throws Exception {
        Collections.reverse(resources);
        for (var resource : resources) resource.close();
        resources.clear();
    }
}
