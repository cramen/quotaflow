package io.quotaflow.fallback;

import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import org.openjdk.jmh.annotations.*;

/** Actual degraded wrapper/facade acquisition with one stable bucket and no recovery probes. */
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class FallbackDecisionBenchmark {
    @Param({"TOKEN_BUCKET"}) public Algorithm algorithm;
    private DefaultQuotaFlow flow;
    private FallbackRateLimitStore store;
    @Setup public void setup() throws Exception {
        var limit = new Limit(1_000_000_000, 1_000_000, Duration.ofSeconds(1));
        var policy = RateLimitPolicy.builder("fallback").scope(io.quotaflow.core.Scope.GLOBAL)
                .algorithm(algorithm).limit(limit).build();
        var policies = PolicySet.compile(List.of(policy));
        var primary = new io.quotaflow.testing.RecoveryPrimaryFixture(policies);
        var offset = new java.util.concurrent.atomic.AtomicLong();
        store = new FallbackRateLimitStore(primary, new RecoverySettings("default", "benchmark", "single",
                RecoveryCohort.single(), 100, 100, Duration.ofMillis(10), Duration.ofSeconds(1)), List.of(),
                () -> System.nanoTime() + offset.get());
        flow = DefaultQuotaFlow.builder(policies, store).build();
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (store.state() != DegradationState.CLOSED && System.nanoTime() < deadline) Thread.sleep(5);
        if (store.state() != DegradationState.CLOSED) throw new IllegalStateException("benchmark enrollment failed");
        primary.available = false;
        flow.tryAcquire("fallback", RateLimitContext.empty());
        flow.tryAcquire("fallback", RateLimitContext.empty());
        // Earn credit through the real cold guard before measuring its allow path.
        offset.set(Duration.ofSeconds(500).toNanos());
        flow.tryAcquire("fallback", RateLimitContext.empty(), limit.capacity());
        offset.set(Duration.ofSeconds(1000).toNanos());
        acquire();
    }
    @TearDown public void close() { store.close(); }
    @Benchmark public Decision acquire() {
        Decision result = flow.tryAcquire("fallback", RateLimitContext.empty());
        if (!result.isAllowed()) throw new IllegalStateException("allow-path benchmark exhausted its quota");
        return result;
    }
}
