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
    private static final class UnavailableStore implements BatchRateLimitStore {
        @Override public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
            return CompletableFuture.failedFuture(new IllegalStateException("benchmark outage"));
        }
        @Override public CompletionStage<Void> registerPolicies(List<PolicyBinding> bindings) {
            return CompletableFuture.completedFuture(null);
        }
        @Override public StoreResult tryAcquire(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) {
            throw new IllegalStateException("benchmark outage");
        }
        @Override public CompletionStage<StoreResult> tryAcquireAsync(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) {
            return CompletableFuture.failedFuture(new IllegalStateException("benchmark outage"));
        }
    }
    @Setup public void setup() {
        var limit = new Limit(1_000_000_000, 1_000_000, Duration.ofSeconds(1));
        var policy = RateLimitPolicy.builder("fallback").scope(io.quotaflow.core.Scope.GLOBAL)
                .algorithm(algorithm).limit(limit).build();
        var store = new FallbackRateLimitStore(new UnavailableStore(), StateSeeder.noOp(),
                new FallbackConfig(1, Duration.ofHours(1), Duration.ofHours(1), 1, 100), List.of());
        flow = DefaultQuotaFlow.builder(PolicySet.compile(List.of(policy)), store).build();
        acquire();
    }
    @Benchmark public Decision acquire() {
        Decision result = flow.tryAcquire("fallback", RateLimitContext.empty());
        if (!result.isAllowed()) throw new IllegalStateException("allow-path benchmark exhausted its quota");
        return result;
    }
}
