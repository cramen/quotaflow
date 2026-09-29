package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ExactNumericTest {
    private static BucketIdentity key(String policy) {
        return new BucketIdentity(new QuotaDomain("numeric", "root"), policy, Scope.USER, "key");
    }
    @Test void generatedSequencesMatchIndependentIntegerDebtOracle() {
        for (Algorithm algorithm : Algorithm.values()) {
            for (int seed = 0; seed < 40; seed++) {
                Random random = new Random(seed);
                long capacity = seed == 0 ? 1_000_000_000L : random.nextInt(100) + 1;
                Limit limit = seed == 0 ? new Limit(capacity, 1_000_000, Duration.ofSeconds(1))
                        : new Limit(capacity, random.nextInt(100) + 1, Duration.ofSeconds(1));
                BigInteger[] division = BigInteger.valueOf(limit.refillPeriod().toNanos())
                        .divideAndRemainder(BigInteger.valueOf(limit.refillAmount()));
                BigInteger interval = division[0].add(division[1].signum() == 0 ? BigInteger.ZERO : BigInteger.ONE);
                BigInteger maximum = interval.multiply(BigInteger.valueOf(capacity));
                BigInteger debt = BigInteger.ZERO;
                AtomicLong now = new AtomicLong(Long.MAX_VALUE - 100_000_000L);
                LocalRateLimitStore store = new LocalRateLimitStore(now::get);
                for (int step = 0; step < 300; step++) {
                    long gap = random.nextInt(10_000_000);
                    now.addAndGet(gap);
                    debt = debt.subtract(BigInteger.valueOf(gap)).max(BigInteger.ZERO);
                    long weight = seed == 0 ? 1 : random.nextInt((int) capacity + 2) + 1;
                    BigInteger candidate = debt.add(interval.multiply(BigInteger.valueOf(weight)));
                    boolean allow = candidate.compareTo(maximum) <= 0;
                    if (allow) debt = candidate;
                    StoreResult actual = store.tryAcquire(key("generated"), limit, algorithm, weight);
                    String context = "seed=" + seed + ", first failing prefix=" + (step + 1) + ", algorithm=" + algorithm;
                    assertEquals(allow, actual.acquired(), context);
                    if (weight <= capacity) {
                        assertEquals(maximum.subtract(debt).divide(interval).longValueExact(), actual.remaining(), context);
                    } else assertEquals(0, actual.retryAfterMillis(), context);
                }
            }
        }
    }
    @Test void fractionalCreditSurvivesFrequentRejectionsAndBackwardClock() {
        for (Algorithm algorithm : Algorithm.values()) {
            AtomicLong now = new AtomicLong(4_000_000_000_000_000L);
            LocalRateLimitStore store = new LocalRateLimitStore(now::get);
            Limit limit = new Limit(1, 7, Duration.ofSeconds(1));
            assertTrue(store.tryAcquire(key("p"), limit, algorithm, 1).acquired());
            for (int i = 0; i < 142; i++) {
                now.addAndGet(1_000_000);
                assertFalse(store.tryAcquire(key("p"), limit, algorithm, 1).acquired());
            }
            now.addAndGet(-100_000_000);
            assertFalse(store.tryAcquire(key("p"), limit, algorithm, 1).acquired());
            now.addAndGet(100_857_142);
            assertFalse(store.tryAcquire(key("p"), limit, algorithm, 1).acquired());
            now.incrementAndGet();
            assertTrue(store.tryAcquire(key("p"), limit, algorithm, 1).acquired());
        }
    }
    @Test void parameterTransitionRejectsOnceThenRefillsAndAlgorithmBindingSurvivesExpiry() {
        AtomicLong now = new AtomicLong();
        LocalRateLimitStore store = new LocalRateLimitStore(now::get);
        var key = key("p");
        assertTrue(store.tryAcquire(key, new Limit(10, 1, Duration.ofSeconds(1)), Algorithm.GCRA, 10).acquired());
        now.set(500_000_000);
        Limit changed = new Limit(20, 2, Duration.ofSeconds(1));
        assertFalse(store.tryAcquire(key, changed, Algorithm.GCRA, 1).acquired());
        now.addAndGet(500_000_000);
        assertTrue(store.tryAcquire(key, changed, Algorithm.GCRA, 1).acquired());
        now.set(Long.MAX_VALUE / 2);
        assertTrue(store.snapshot().isEmpty());
        assertThrows(PolicyConfigurationException.class, () -> store.tryAcquire(key, changed, Algorithm.TOKEN_BUCKET, 1));
        assertTrue(store.tryAcquire(key, changed, Algorithm.GCRA, 20).acquired());
    }
    @Test void impossibleChildDoesNotChargeParent() {
        LocalRateLimitStore store = new LocalRateLimitStore(() -> 0);
        Limit limit = new Limit(10, 1, Duration.ofSeconds(1));
        var result = store.tryAcquireAll(List.of(new LevelRequest(key("root"), limit, Algorithm.GCRA, 1),
                new LevelRequest(key("child"), limit, Algorithm.GCRA, 11))).toCompletableFuture().join();
        assertFalse(result.acquired());
        assertEquals(1, result.firedLevelIndex());
        assertEquals(0, result.retryAfterMillis());
        assertTrue(store.tryAcquire(key("root"), limit, Algorithm.GCRA, 10).acquired());
    }
    @Test void invalidInputsAndAlgorithmReloadLeaveExistingStateUntouched() {
        LocalRateLimitStore store = new LocalRateLimitStore(() -> 0);
        var limit = new Limit(10, 1, Duration.ofSeconds(1));
        assertThrows(IllegalArgumentException.class, () -> store.tryAcquire(key("p"), limit, Algorithm.GCRA, Limit.MAX_TOKENS + 1));
        assertEquals(0, store.cellCount());
        var policy = RateLimitPolicy.builder("p").scope(Scope.GLOBAL).algorithm(Algorithm.GCRA).limit(limit).build();
        DefaultQuotaFlow flow = DefaultQuotaFlow.builder(PolicySet.compile(List.of(policy)), store).build();
        assertTrue(flow.tryAcquire("p", RateLimitContext.empty(), 10).isAllowed());
        var incompatible = RateLimitPolicy.builder("p").scope(Scope.GLOBAL).algorithm(Algorithm.TOKEN_BUCKET).limit(limit).build();
        assertThrows(PolicyConfigurationException.class, () -> flow.replacePolicySet(PolicySet.compile(List.of(incompatible))));
        assertFalse(flow.tryAcquire("p", RateLimitContext.empty()).isAllowed());
        store.registerPolicies(List.of()).toCompletableFuture().join();
        assertThrows(PolicyConfigurationException.class, () -> store.registerPolicies(List.of(
                new PolicyBinding(new QuotaDomain("default", "p"), "p", Scope.GLOBAL, Algorithm.TOKEN_BUCKET))));
    }

}
