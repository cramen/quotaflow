package io.quotaflow.tck;

import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class GeneratedNumericModelTest {
    record Operation(long deltaNanos, long weight) { }

    @Test void seededFractionalRefillsWeightsAndClockReversalsMatchExactModel() throws Exception {
        for (Algorithm algorithm : Algorithm.values()) {
            for (long seed = 0; seed < 128; seed++) {
                var random = new Random(seed);
                long capacity = seed % 16 == 0 ? 1_000_000_000L : 1 + random.nextInt(64);
                long refill = 1 + random.nextInt(31);
                long period = refill * 1_000 + random.nextInt(997);
                var limit = new Limit(capacity, refill, Duration.ofNanos(period));
                var history = new ArrayList<Operation>();
                history.add(new Operation(0, capacity));
                for (int step = 0; step < 128; step++) {
                    history.add(new Operation(random.nextInt(20_000) - 1000, 1 + random.nextInt(70)));
                }
                if (!matches(algorithm, limit, history)) {
                    var minimized = minimize(history, candidate -> !matches(algorithm, limit, candidate));
                    Path report = Path.of("build/reports/generated", "numeric-" + algorithm + "-" + seed + ".txt");
                    Files.createDirectories(report.getParent());
                    Files.writeString(report, "seed=" + seed + " algorithm=" + algorithm + " limit=" + limit + "\noriginal=" + history + "\nminimized=" + minimized);
                    fail("Exact model disagreement: seed=" + seed + " report=" + report.toAbsolutePath());
                }
            }
        }
    }

    @Test void maximumPeriodAndFullRefillHorizonMatchTheReference() {
        for (var algorithm : Algorithm.values()) {
            for (var limit : List.of(new Limit(1, 1, Duration.ofDays(32)),
                    new Limit(1_000_000_000L, 1_000_000_000L, Duration.ofDays(32)))) {
                long horizon = Duration.ofDays(32).toNanos();
                assertTrue(matches(algorithm, limit, List.of(
                        new Operation(0, limit.capacity()), new Operation(horizon - 1, 1),
                        new Operation(1, 1), new Operation(horizon, limit.capacity()),
                        new Operation(-horizon, 1), new Operation(horizon, 1))));
            }
        }
    }

    @Test void referenceRetainsFractionalCreditAndNeverRoundsAnAdmissionUp() {
        var model = new ExactBudgetModel(2, 3, 3001, 1, 0);
        assertTrue(model.acquire(0, 2).allowed());
        assertFalse(model.acquire(1000, 1).allowed());
        assertTrue(model.acquire(1001, 1).allowed());
        assertFalse(model.acquire(2001, 1).allowed());
        assertTrue(model.acquire(2002, 1).allowed());
        assertEquals(0, model.acquire(2002, 3).retryMillis());
        var redisPrecision = new ExactBudgetModel(1, 3, 3001, 1000, 0);
        assertTrue(redisPrecision.acquire(0, 1).allowed());
        assertFalse(redisPrecision.acquire(1999, 1).allowed());
        assertTrue(redisPrecision.acquire(2000, 1).allowed());
        var sampledClock = new ExactBudgetModel(2, 3, 3001, 1000, 0);
        assertTrue(sampledClock.acquire(0, 2).allowed());
        assertTrue(sampledClock.acquire(2000, 1).allowed());
        assertTrue(sampledClock.acquire(4000, 2).allowed(), "clock precision must not round the emission interval to whole microseconds");
    }

    @Test void knownFractionalAndMaximumCapacityRegressionsShrinkToReplayableHistories() {
        var fractional = new Limit(2, 3, Duration.ofNanos(3001));
        var roundedDown = new Limit(2, 3, Duration.ofNanos(3000));
        var maximum = new Limit(1_000_000_000L, 1, Duration.ofNanos(1001));
        var wrongCapacity = new Limit(999_999_999L, 1, Duration.ofNanos(1001));
        for (var algorithm : Algorithm.values()) {
            var fractionalHistory = List.of(new Operation(0, 1), new Operation(0, 1),
                    new Operation(1000, 1), new Operation(1001, 1));
            var fractionalFailure = (java.util.function.Predicate<List<Operation>>) trace ->
                    !matches(algorithm, fractional, roundedDown, trace);
            assertTrue(fractionalFailure.test(fractionalHistory));
            var minimalFractional = minimize(fractionalHistory, fractionalFailure);
            assertTrue(fractionalFailure.test(minimalFractional));
            assertTrue(minimalFractional.size() < fractionalHistory.size());
            var maximumHistory = List.of(new Operation(0, 1_000_000_000L), new Operation(1001, 1), new Operation(0, 1));
            var capacityFailure = (java.util.function.Predicate<List<Operation>>) trace ->
                    !matches(algorithm, maximum, wrongCapacity, trace);
            var minimalMaximum = minimize(maximumHistory, capacityFailure);
            assertEquals(1, minimalMaximum.size());
            assertTrue(capacityFailure.test(minimalMaximum));
        }
    }

    private static boolean matches(Algorithm algorithm, Limit limit, List<Operation> history) {
        return matches(algorithm, limit, limit, history);
    }

    private static boolean matches(Algorithm algorithm, Limit limit, Limit actualLimit, List<Operation> history) {
        var clock = new AtomicLong();
        var store = new LocalRateLimitStore(clock::get);
        var key = new BucketIdentity(new QuotaDomain("model", "root"), "root", Scope.GLOBAL, "shared");
        ExactBudgetModel model = null;
        for (var operation : history) {
            clock.addAndGet(operation.deltaNanos());
            if (model == null) model = new ExactBudgetModel(limit.capacity(), limit.refillAmount(), limit.refillPeriod().toNanos(), 1, clock.get());
            var expected = model.acquire(clock.get(), operation.weight());
            var actual = store.tryAcquire(key, actualLimit, algorithm, operation.weight());
            if (expected.allowed() != actual.acquired() || expected.remaining() != actual.remaining()
                    || expected.retryMillis() != actual.retryAfterMillis()) return false;
        }
        return true;
    }

    private static List<Operation> minimize(List<Operation> original, java.util.function.Predicate<List<Operation>> fails) {
        var history = new ArrayList<>(original);
        for (int chunk = Math.max(1, history.size() / 2); chunk > 0; chunk /= 2) {
            for (int start = 0; start + chunk <= history.size();) {
                var candidate = new ArrayList<>(history);
                candidate.subList(start, start + chunk).clear();
                if (!candidate.isEmpty() && fails.test(candidate)) history = candidate;
                else start++;
            }
        }
        return List.copyOf(history);
    }
}
