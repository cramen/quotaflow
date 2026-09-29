package io.quotaflow.core;

import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.BucketIdentity;
import io.quotaflow.core.store.QuotaDomain;
import io.quotaflow.core.store.StoreResult;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Degradation-mode hot path: a single local-store decision. Gated absolutely
 * at p99 &le; 0.01 ms by the {@code benchmarkGate} task — the local path is
 * hardware-cheap enough that the absolute bound holds on any CI runner.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class LocalFallbackBenchmark {

    private LocalRateLimitStore store;
    private Limit limit;
    private final BucketIdentity bucket = new BucketIdentity(
            new QuotaDomain("benchmark", "fallback"), "fallback", io.quotaflow.core.Scope.KEY, "shared");

    @Setup
    public void setup() {
        store = new LocalRateLimitStore();
        // capacity far above the iteration count: always the allow path
        limit = new Limit(1_000_000_000, 1_000_000, Duration.ofSeconds(1));
    }

    @Benchmark
    public StoreResult tryAcquire() {
        return store.tryAcquire(bucket, limit, Algorithm.TOKEN_BUCKET, 1);
    }
}
