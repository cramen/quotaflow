package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.RedisClient;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Cross-instance race: many threads spread over several independent store
 * instances hammer one shared limit on one server. Atomic script execution
 * must cap the total number of allowed requests at the limit exactly (no
 * refill happens during the test, so algorithm precision is exact).
 */
class CrossInstanceRaceTckTest extends TckContainers {

    private static final int LIMIT = 200;
    private static final int INSTANCES = 4;
    private static final int THREADS_PER_INSTANCE = 4;
    // total attempts = 10x the limit: enough to drain the bucket completely
    private static final int ATTEMPTS_PER_THREAD = 125;

    @Test
    void raceOnRedis() throws Exception {
        race(redisUri());
    }

    @Test
    void raceOnValkey() throws Exception {
        race(valkeyUri());
    }

    private void race(String uri) throws Exception {
        // one token per hour: no refill interferes with the race window
        Limit limit = new Limit(LIMIT, 1, Duration.ofHours(1));
        String key = uniqueKey("race:global:g");

        List<RedisClient> clients = new ArrayList<>();
        List<RedisRateLimitStore> stores = new ArrayList<>();
        for (int i = 0; i < INSTANCES; i++) {
            RedisClient client = newClient(uri);
            clients.add(client);
            stores.add(RedisRateLimitStore.create(client, RedisStoreConfig.defaults()));
        }

        AtomicLong allowed = new AtomicLong();
        CountDownLatch ready = new CountDownLatch(INSTANCES * THREADS_PER_INSTANCE);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(INSTANCES * THREADS_PER_INSTANCE);
        try {
            for (int instance = 0; instance < INSTANCES; instance++) {
                RedisRateLimitStore store = stores.get(instance);
                for (int thread = 0; thread < THREADS_PER_INSTANCE; thread++) {
                    pool.submit(() -> {
                        ready.countDown();
                        try {
                            start.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        for (int attempt = 0; attempt < ATTEMPTS_PER_THREAD; attempt++) {
                            if (store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1).acquired()) {
                                allowed.incrementAndGet();
                            }
                        }
                    });
                }
            }
            assertTrue(ready.await(30, TimeUnit.SECONDS), "workers failed to start");
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS), "race did not finish in time");
        } finally {
            pool.shutdownNow();
            stores.forEach(RedisRateLimitStore::close);
        }

        assertTrue(allowed.get() <= LIMIT,
                "allowed " + allowed.get() + " exceeds the shared limit " + LIMIT);
        assertEquals(LIMIT, allowed.get(),
                "with ample attempts the bucket must be drained exactly, not over or under");
    }
}
