package io.quotaflow.verification.published;

import io.quotaflow.core.*;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.List;

public final class CoreConsumer {
    public static void main(String[] args) {
        if (Runtime.version().feature()!=Integer.getInteger("verification.jdk")) throw new AssertionError("Unexpected JDK");
        var policies=PolicySet.compile(List.of(RateLimitPolicy.builder("core").scope(Scope.GLOBAL)
                .limit(new Limit(2,1,Duration.ofHours(1))).build()));
        try (var flow=DefaultQuotaFlow.builder(policies,new LocalRateLimitStore()).build()) {
            if (!flow.tryAcquire("core",RateLimitContext.empty()).isAllowed()
                    || !flow.tryAcquireAsync("core",RateLimitContext.empty(),1).toCompletableFuture().join().isAllowed()
                    || flow.tryAcquire("core",RateLimitContext.empty()).isAllowed()) throw new AssertionError("Published core quota failed");
        }
        System.out.println("PUBLISHED CONSUMERS VERIFIED case="+System.getProperty("verification.case"));
    }
}
