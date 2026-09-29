package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import java.math.BigInteger;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class NumericHistoryPropertyTest {
    private record Step(long elapsed, long weight) { }
    private static final BucketIdentity KEY = new BucketIdentity(new QuotaDomain("property", "p"), "p", Scope.USER, "key");

    @Test void generatedHistoriesRetainSeedsAndShrinkFailures() {
        for (Algorithm algorithm : Algorithm.values()) for (int seed = 0; seed < 8; seed++) {
            Random random = new Random(seed);
            List<Step> history = new ArrayList<>();
            for (int i = 0; i < 50; i++) history.add(new Step(random.nextInt(150_000_000), random.nextInt(8) + 1));
            try { replay(history, algorithm); }
            catch (AssertionError failure) {
                // Deletion shrink, then scalar shrink, retaining only candidates that still fail.
                for (int i = 0; i < history.size();) {
                    List<Step> candidate = new ArrayList<>(history); candidate.remove(i);
                    if (fails(candidate, algorithm)) history = candidate; else i++;
                }
                for (int i = 0; i < history.size(); i++) {
                    Step step = history.get(i);
                    for (Step small : List.of(new Step(0, step.weight), new Step(step.elapsed, 1))) {
                        List<Step> candidate = new ArrayList<>(history); candidate.set(i, small);
                        if (fails(candidate, algorithm)) history = candidate;
                    }
                }
                throw new AssertionError("seed=" + seed + ", algorithm=" + algorithm + ", minimized=" + history, failure);
            }
        }
    }
    private static boolean fails(List<Step> history, Algorithm algorithm) {
        try { replay(history, algorithm); return false; } catch (AssertionError failure) { return true; }
    }
    private static void replay(List<Step> history, Algorithm algorithm) {
        AtomicLong clock = new AtomicLong(8_000_000_000_000_000L);
        LocalRateLimitStore store = new LocalRateLimitStore(clock::get);
        Limit limit = new Limit(7, 7, Duration.ofSeconds(1));
        BigInteger interval = BigInteger.valueOf(1_000_000_000L).add(BigInteger.valueOf(6)).divide(BigInteger.valueOf(7));
        BigInteger maximum = interval.multiply(BigInteger.valueOf(7));
        BigInteger debt = BigInteger.ZERO;
        for (Step step : history) {
            clock.addAndGet(step.elapsed);
            debt = debt.subtract(BigInteger.valueOf(step.elapsed)).max(BigInteger.ZERO);
            BigInteger candidate = debt.add(interval.multiply(BigInteger.valueOf(step.weight)));
            boolean allowed = candidate.compareTo(maximum) <= 0;
            if (allowed) debt = candidate;
            StoreResult result = store.tryAcquire(KEY, limit, algorithm, step.weight);
            assertEquals(allowed, result.acquired());
            assertEquals(maximum.subtract(debt).divide(interval).longValueExact(), result.remaining());
        }
    }

    @Test void generatedWeightedContentionPreservesMaximumWholeBalance() throws Exception {
        for (Algorithm algorithm : Algorithm.values()) for (int seed = 0; seed < 4; seed++) {
            LocalRateLimitStore store = new LocalRateLimitStore(() -> 0);
            Limit limit = new Limit(1_000_000_000L, 1_000_000, Duration.ofSeconds(1));
            AtomicLong spent = new AtomicLong();
            ExecutorService pool = Executors.newFixedThreadPool(4);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int worker = 0; worker < 4; worker++) {
                    Random random = new Random(seed * 31L + worker);
                    futures.add(pool.submit(() -> {
                        for (int i = 0; i < 100; i++) {
                            long weight = random.nextInt(20_000_000) + 1;
                            if (store.tryAcquire(KEY, limit, algorithm, weight).acquired()) spent.addAndGet(weight);
                        }
                    }));
                }
                for (Future<?> future : futures) future.get(10, TimeUnit.SECONDS);
                assertTrue(spent.get() <= limit.capacity(), "seed=" + seed);
                assertEquals(limit.capacity() - spent.get(), store.snapshot().get(0).remaining());
            } finally { pool.shutdownNow(); }
        }
    }
}
