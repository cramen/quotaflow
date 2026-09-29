package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.Scope;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** A rejected speculative debit must not turn discarded refill into extra burst credit. */
class LocalChainRefillRollbackTest {
    @Test void tokenBucketDoesNotCreateRefundCredit() throws Exception { verify(Algorithm.TOKEN_BUCKET); }
    @Test void gcraDoesNotCreateRefundCredit() throws Exception { verify(Algorithm.GCRA); }

    private void verify(Algorithm algorithm) throws Exception {
        AtomicLong now = new AtomicLong();
        AtomicInteger preparations = new AtomicInteger();
        CountDownLatch chainPrepared = new CountDownLatch(1);
        CountDownLatch continueChild = new CountDownLatch(1);
        LocalRateLimitStore store = new LocalRateLimitStore(now::get, 4096, () -> {
            if (Thread.currentThread().getName().equals("paused-chain") && preparations.incrementAndGet() == 1) {
                chainPrepared.countDown();
                try {
                    if (!continueChild.await(5, TimeUnit.SECONDS)) throw new AssertionError("child release timed out");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
        });
        QuotaDomain domain = new QuotaDomain("rollback-race", "parent");
        BucketIdentity parent = new BucketIdentity(domain, "parent", Scope.GLOBAL, "shared");
        BucketIdentity child = new BucketIdentity(domain, "child", Scope.USER, "u");
        Limit parentLimit = new Limit(1, 1, Duration.ofSeconds(1));
        Limit childLimit = new Limit(1, 1, Duration.ofSeconds(100));
        assertTrue(store.tryAcquire(child, childLimit, algorithm, 1).acquired());
        ExecutorService worker = Executors.newSingleThreadExecutor(r -> new Thread(r, "paused-chain"));
        try {
            Future<ChainResult> rejected = worker.submit(() -> store.tryAcquireAll(List.of(
                    new LevelRequest(parent, parentLimit, algorithm, 1),
                    new LevelRequest(child, childLimit, algorithm, 1))).toCompletableFuture().join());
            assertTrue(chainPrepared.await(5, TimeUnit.SECONDS));
            now.set(1_000_000_000L);
            assertTrue(store.tryAcquire(parent, parentLimit, algorithm, 1).acquired());
            continueChild.countDown();
            assertFalse(rejected.get(5, TimeUnit.SECONDS).acquired());
            assertFalse(store.tryAcquire(parent, parentLimit, algorithm, 1).acquired(),
                    algorithm + ": two successful parent admissions at the same instant exceed burst capacity one");
        } finally {
            continueChild.countDown();
            worker.shutdownNow();
        }
    }
}
