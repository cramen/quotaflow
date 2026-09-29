package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.*;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.Scope;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/** Independent weighted accounting oracle shared by all backends. */
final class HierarchyConformance {
    private HierarchyConformance() { }

    static void verify(BatchRateLimitStore store, Algorithm algorithm) throws Exception {
        for (int seed = 0; seed < 8; seed++) {
            String root = "generated-" + UUID.randomUUID();
            QuotaDomain domain = new QuotaDomain("default", root);
            BucketIdentity parent = new BucketIdentity(domain, root, Scope.GLOBAL, "shared");
            Limit parentLimit = new Limit(53 + seed, 1, Duration.ofHours(1));
            Limit tenantLimit = new Limit(29, 1, Duration.ofHours(1));
            Limit userLimit = new Limit(7, 1, Duration.ofHours(1));
            String tenantPolicy = root + "-tenant";
            String userPolicy = root + "-user";
            store.registerPolicies(List.of(PolicyBinding.of(parent),
                    new PolicyBinding(domain, tenantPolicy, Scope.TENANT),
                    new PolicyBinding(domain, userPolicy, Scope.USER))).toCompletableFuture().join();
            AtomicLong total = new AtomicLong();
            AtomicLongArray tenants = new AtomicLongArray(4);
            AtomicLongArray users = new AtomicLongArray(32);
            var pool = Executors.newFixedThreadPool(4);
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            try {
                for (int thread = 0; thread < 4; thread++) {
                    final long replaySeed = seed * 101L + thread;
                    futures.add(pool.submit(() -> {
                        Random random = new Random(replaySeed);
                        for (int request = 0; request < 40; request++) {
                            int tenant = random.nextInt(4);
                            int user = tenant * 8 + random.nextInt(8);
                            long weight = 1 + random.nextInt(5);
                            var result = store.tryAcquireAll(List.of(
                                    new LevelRequest(parent, parentLimit, algorithm, weight),
                                    new LevelRequest(new BucketIdentity(domain, tenantPolicy, Scope.TENANT, "t" + tenant), tenantLimit, algorithm, weight),
                                    new LevelRequest(new BucketIdentity(domain, userPolicy, Scope.USER, "u" + user), userLimit, algorithm, weight)))
                                    .toCompletableFuture().join();
                            if (result.acquired()) {
                                total.addAndGet(weight);
                                tenants.addAndGet(tenant, weight);
                                users.addAndGet(user, weight);
                            }
                        }
                    }));
                }
                for (var future : futures) future.get(20, java.util.concurrent.TimeUnit.SECONDS);
            } finally { pool.shutdownNow(); }
            assertTrue(total.get() <= parentLimit.capacity(), "shared parent exceeded, seed=" + seed);
            for (int i = 0; i < 4; i++) assertTrue(tenants.get(i) <= 29, "tenant exceeded, seed=" + seed);
            for (int i = 0; i < 32; i++) assertTrue(users.get(i) <= 7, "user exceeded, seed=" + seed);
            long remaining = parentLimit.capacity() - total.get();
            if (remaining > 0) assertTrue(store.tryAcquire(parent, parentLimit, algorithm, remaining).acquired(),
                    "rejected children must not consume shared parent credit, seed=" + seed);
            assertFalse(store.tryAcquire(parent, parentLimit, algorithm, 1).acquired());
        }
        siblingPoliciesAndRejectedChild(store, algorithm);
    }

    private static void siblingPoliciesAndRejectedChild(BatchRateLimitStore store, Algorithm algorithm) {
        String root = "siblings-" + UUID.randomUUID();
        var domain = new QuotaDomain("default", root);
        var parent = new BucketIdentity(domain, root, Scope.GLOBAL, "shared");
        var two = new Limit(2, 1, Duration.ofHours(1));
        var one = new Limit(1, 1, Duration.ofHours(1));
        List<LevelRequest> first = List.of(new LevelRequest(parent, two, algorithm, 1),
                new LevelRequest(new BucketIdentity(domain, root + "-a", Scope.TENANT, "a"), one, algorithm, 1));
        assertTrue(store.tryAcquireAll(first).toCompletableFuture().join().acquired());
        assertFalse(store.tryAcquireAll(first).toCompletableFuture().join().acquired());
        List<LevelRequest> second = List.of(first.get(0),
                new LevelRequest(new BucketIdentity(domain, root + "-b", Scope.TENANT, "b"), one, algorithm, 1));
        assertTrue(store.tryAcquireAll(second).toCompletableFuture().join().acquired());
        assertFalse(store.tryAcquire(parent, two, algorithm, 1).acquired());
    }
}
