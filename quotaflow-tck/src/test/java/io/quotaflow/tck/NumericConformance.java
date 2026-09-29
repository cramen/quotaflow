package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.*;

/** Independent whole-token oracle with no refill during the bounded trace. */
final class NumericConformance {
    private NumericConformance() { }
    static void verify(BatchRateLimitStore store) {
        for (Algorithm algorithm : Algorithm.values()) {
            for (int seed = 0; seed < 8; seed++) {
                String policy = "numeric-" + UUID.randomUUID();
                QuotaDomain domain = new QuotaDomain("default", policy);
                BucketIdentity parent = new BucketIdentity(domain, policy, Scope.GLOBAL, "shared");
                BucketIdentity child = new BucketIdentity(domain, policy + "-child", Scope.USER, "u");
                Limit limit = new Limit(53, 1, Duration.ofHours(1));
                long remaining = 53;
                Random random = new Random(seed);
                for (int step = 0; step < 40; step++) {
                    int weight = 1 + random.nextInt(55);
                    boolean expected = weight <= remaining;
                    ChainResult result = store.tryAcquireAll(List.of(new LevelRequest(parent, limit, algorithm, weight),
                            new LevelRequest(child, limit, algorithm, weight))).toCompletableFuture().join();
                    assertEquals(expected, result.acquired(), "seed=" + seed + ", prefix=" + (step + 1));
                    if (expected) remaining -= weight;
                    assertEquals(remaining, result.remaining());
                    if (weight > 53) assertEquals(0, result.retryAfterMillis());
                }
                // Transition preserves only known credit; an increase is not a new burst.
                Limit raised = new Limit(106, 1, Duration.ofHours(1));
                var denied = store.tryAcquire(parent, raised, algorithm, 54);
                assertFalse(denied.acquired());
                assertEquals(remaining, denied.remaining());
                RuntimeException failure = assertThrows(RuntimeException.class, () -> store.tryAcquire(parent, raised,
                        algorithm == Algorithm.GCRA ? Algorithm.TOKEN_BUCKET : Algorithm.GCRA, 1));
                Throwable cause = failure instanceof java.util.concurrent.CompletionException ? failure.getCause() : failure;
                assertInstanceOf(PolicyConfigurationException.class, cause);
            }
        }
    }
}
