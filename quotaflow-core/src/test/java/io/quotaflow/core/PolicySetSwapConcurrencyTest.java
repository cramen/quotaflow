package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * While the compiled set is being swapped, every individual decision must be
 * consistent with exactly one set (old or new), never a mix. The two sets
 * differ in scope and capacity, so a mixed decision would be detectable:
 * a USER decision can never report remaining above 2, a TENANT decision never
 * above 5.
 */
class PolicySetSwapConcurrencyTest {

    private static PolicySet userSet() {
        return PolicySet.compile(List.of(RateLimitPolicy.builder("p")
                .limit(new Limit(3, 1, Duration.ofHours(1)))
                .scope(Scope.USER)
                .build()));
    }

    private static PolicySet tenantSet() {
        return PolicySet.compile(List.of(RateLimitPolicy.builder("p")
                .limit(new Limit(6, 1, Duration.ofHours(1)))
                .scope(Scope.TENANT)
                .build()));
    }

    @Test
    void decisionsAlwaysUseOneConsistentSetDuringReplacement() throws Exception {
        AtomicLong nanos = new AtomicLong();
        DefaultQuotaFlow quotaFlow = DefaultQuotaFlow.builder(
                        userSet(), new LocalRateLimitStore(nanos::get))
                .build();
        RateLimitContext context = RateLimitContext.builder()
                .put(RateLimitContext.PRINCIPAL, "alice")
                .put(RateLimitContext.TENANT_ID, "acme")
                .build();

        int threads = 8;
        int decisionsPerThread = 5_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger violations = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < decisionsPerThread; i++) {
                    Decision decision = quotaFlow.tryAcquire("p", context);
                    boolean consistent = switch (decision.scope()) {
                        case USER -> decision.remaining() <= 2 && decision.policyId().equals("p");
                        case TENANT -> decision.remaining() <= 5 && decision.policyId().equals("p");
                        default -> false;
                    };
                    if (!consistent) {
                        violations.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        start.countDown();
        int swaps = 0;
        while (swaps < 2_000 && futures.stream().anyMatch(f -> !f.isDone())) {
            quotaFlow.replacePolicySet(swaps % 2 == 0 ? tenantSet() : userSet());
            swaps++;
        }
        for (Future<?> future : futures) {
            future.get();
        }
        pool.shutdown();
        assertTrue(swaps > 0, "swap loop must have run");
        assertEquals(0, violations.get(), "every decision must be consistent with one policy set");
    }
}
