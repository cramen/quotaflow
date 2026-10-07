package io.quotaflow.verification.published;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quotaflow.core.*;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.micrometer.MicrometerDecisionListener;
import java.time.Duration;
import java.util.List;

public final class MetricsConsumer {
    public static void main(String[] args) {
        if (Runtime.version().feature()!=Integer.getInteger("verification.jdk")) throw new AssertionError("Unexpected JDK");
        var policies=PolicySet.compile(List.of(RateLimitPolicy.builder("metrics").scope(Scope.GLOBAL)
                .limit(new Limit(1,1,Duration.ofHours(1))).build()));
        var registry=new SimpleMeterRegistry();
        try (var listener=new MicrometerDecisionListener(registry);
             var flow=DefaultQuotaFlow.builder(policies,new LocalRateLimitStore()).addListener(listener).build()) {
            flow.tryAcquire("metrics",RateLimitContext.empty()); flow.tryAcquire("metrics",RateLimitContext.empty());
            flow.flushObservations().toCompletableFuture().orTimeout(5,java.util.concurrent.TimeUnit.SECONDS).join();
            if (registry.get("quotaflow.decisions").tag("result","allow").counter().count()!=1
                    || registry.get("quotaflow.decisions").tag("result","reject").counter().count()!=1
                    || registry.get("quotaflow.utilization").gauge().value()!=1
                    || flow.observationFailures()!=0) throw new AssertionError("Published metrics are incomplete");
        } finally { registry.close(); }
        System.out.println("PUBLISHED CONSUMERS VERIFIED case="+System.getProperty("verification.case"));
    }
}
