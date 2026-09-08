package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class LocalRateLimitStoreRaceTest {

    private static final int THREADS = 16;
    private static final int ATTEMPTS_PER_THREAD = 2_000;
    private static final long CAPACITY = 10_000;

    /** One token per hour: no refill during the test, so exactly CAPACITY acquisitions may succeed. */
    private static final Limit LIMIT = new Limit(CAPACITY, 1, Duration.ofHours(1));

    @Test
    void tokenBucketNeverOverAdmitsUnderContention() throws Exception {
        race(Algorithm.TOKEN_BUCKET);
    }

    @Test
    void gcraNeverOverAdmitsUnderContention() throws Exception {
        race(Algorithm.GCRA);
    }

    private void race(Algorithm algorithm) throws Exception {
        AtomicLong clock = new AtomicLong();
        LocalRateLimitStore store = new LocalRateLimitStore(clock::get);
        AtomicLong acquired = new AtomicLong();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                long mine = 0;
                for (int i = 0; i < ATTEMPTS_PER_THREAD; i++) {
                    if (store.tryAcquire("hot-key", LIMIT, algorithm, 1).acquired()) {
                        mine++;
                    }
                }
                acquired.addAndGet(mine);
                return null;
            }));
        }
        start.countDown();
        for (Future<?> future : futures) {
            future.get();
        }
        pool.shutdown();
        assertEquals(CAPACITY, acquired.get(),
                "total acquisitions must equal capacity exactly: no token lost, none invented");
    }
}
