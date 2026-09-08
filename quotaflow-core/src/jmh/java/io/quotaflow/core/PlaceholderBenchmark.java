package io.quotaflow.core;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

/** Placeholder benchmark keeping the JMH harness green until a real hot path exists. */
public class PlaceholderBenchmark {

    @Benchmark
    public void describe(Blackhole blackhole) {
        blackhole.consume(ModulePlaceholder.describe(true));
    }
}
