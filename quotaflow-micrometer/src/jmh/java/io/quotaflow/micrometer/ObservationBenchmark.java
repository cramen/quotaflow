package io.quotaflow.micrometer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quotaflow.core.*;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

/** End-to-end acquisition and delivery, so dropping observations cannot improve results. */
@State(org.openjdk.jmh.annotations.Scope.Thread)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ObservationBenchmark {
    @Param({"TOKEN_BUCKET", "GCRA"}) public String algorithm;
    private DefaultQuotaFlow flow;
    private SimpleMeterRegistry registry;

    @Setup public void setup() {
        registry = new SimpleMeterRegistry();
        var policy = RateLimitPolicy.builder("sample").scope(io.quotaflow.core.Scope.GLOBAL)
                .algorithm(Algorithm.valueOf(algorithm)).limit(new Limit(100_000, 100_000, Duration.ofSeconds(1))).build();
        flow = DefaultQuotaFlow.builder(PolicySet.compile(List.of(policy)), new LocalRateLimitStore())
                .addListener(new MicrometerDecisionListener(registry)).build();
    }

    @Benchmark public Decision acquireAndDeliver() {
        var decision = flow.tryAcquire("sample", RateLimitContext.empty());
        flow.flushObservations().toCompletableFuture().join();
        return decision;
    }

    @TearDown public void close() {
        flow.close();
        flow.flushObservations().toCompletableFuture().join();
        if (flow.observationFailures() != 0) throw new IllegalStateException("Observation evidence is incomplete");
        registry.close();
    }
}
