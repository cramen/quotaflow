package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.config.ConfigReloader;
import io.quotaflow.config.ConfigurationParser;
import io.quotaflow.config.PropertiesFileConfigSource;
import io.quotaflow.core.Decision;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.RateLimitContext;
import io.quotaflow.core.Scope;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Dynamic configuration conformance: an operator edits the watched properties
 * file while traffic flows; new decisions honor the changed configuration
 * within one second under the default polling watcher, and every in-flight
 * decision is consistent with exactly one policy set (old or new), never a
 * mix. The two sets use different scopes, so their buckets are disjoint and
 * every decision is attributable to exactly one set by its scope and
 * remaining shape.
 */
class DynamicConfigReloadTckTest {

    private static final RateLimitContext ALICE = RateLimitContext.builder()
            .put(RateLimitContext.PRINCIPAL, "alice")
            .put(RateLimitContext.TENANT_ID, "acme")
            .build();

    /** Old set: user scope, effectively unlimited (never rejects in-test). */
    private static Map<String, String> relaxedPayload() {
        return payload("user", 100_000_000, 100_000_000, "PT1S");
    }

    /** New set: tenant scope, capacity 1 with a frozen refill. */
    private static Map<String, String> tightenedPayload() {
        return payload("tenant", 1, 1, "PT1H");
    }

    private static Map<String, String> payload(
            String scope, long capacity, long refillAmount, String refillPeriod) {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("quotaflow.policies.u.scope", scope);
        map.put("quotaflow.policies.u.limit.capacity", Long.toString(capacity));
        map.put("quotaflow.policies.u.limit.refill-amount", Long.toString(refillAmount));
        map.put("quotaflow.policies.u.limit.refill-period", refillPeriod);
        return map;
    }

    /**
     * USER decisions come from the relaxed set (allowed, remaining close to
     * 100M); TENANT decisions come from the tightened set (capacity 1, so
     * remaining is always 0). Any other shape proves a mixed-set decision.
     */
    private static boolean consistentWithOneSet(Decision decision) {
        if (decision.scope() == Scope.USER) {
            return decision.isAllowed() && decision.remaining() > 1_000_000;
        }
        if (decision.scope() == Scope.TENANT) {
            return decision.remaining() == 0;
        }
        return false;
    }

    @TempDir
    Path dir;

    @Test
    void limitChangeAppliesWithinOneSecondWithoutMixedDecisions() throws Exception {
        Path file = dir.resolve("quotaflow.properties");
        write(file, relaxedPayload());
        DefaultQuotaFlow quotaFlow = DefaultQuotaFlow
                .builder(ConfigurationParser.parse(relaxedPayload()).policySet(), new LocalRateLimitStore())
                .build();
        ConfigReloader reloader = ConfigReloader
                .builder(new PropertiesFileConfigSource(file), quotaFlow)
                .build();
        reloader.start();
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger violations = new AtomicInteger();
        ExecutorService traffic = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> workers = startTraffic(traffic, running, violations, quotaFlow);

            long changedAt = System.nanoTime();
            write(file, tightenedPayload());
            long appliedAfterNanos = awaitNewSet(quotaFlow, Duration.ofSeconds(5));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(appliedAfterNanos - changedAt);
            assertTrue(elapsedMillis <= 1_000,
                    "new configuration must govern decisions within 1 s of the source change, took "
                            + elapsedMillis + " ms");

            running.set(false);
            for (Future<?> worker : workers) {
                worker.get();
            }
            assertEquals(0, violations.get(),
                    "every decision must be consistent with exactly one policy set");
        } finally {
            running.set(false);
            traffic.shutdownNow();
            reloader.close();
        }
    }

    private static List<Future<?>> startTraffic(
            ExecutorService traffic, AtomicBoolean running, AtomicInteger violations,
            DefaultQuotaFlow quotaFlow) {
        CountDownLatch started = new CountDownLatch(1);
        List<Future<?>> workers = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            workers.add(traffic.submit(() -> {
                started.await();
                while (running.get()) {
                    Decision decision = quotaFlow.tryAcquire("u", ALICE);
                    if (!consistentWithOneSet(decision)) {
                        violations.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        started.countDown();
        return workers;
    }

    /** Waits until decisions visibly run on the tightened set (TENANT scope rejecting). */
    private static long awaitNewSet(DefaultQuotaFlow quotaFlow, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Decision decision = quotaFlow.tryAcquire("u", ALICE);
            if (decision.scope() == Scope.TENANT && !decision.isAllowed()) {
                return System.nanoTime();
            }
            Thread.sleep(5);
        }
        throw new AssertionError("the tightened configuration was never applied within " + timeout);
    }

    private static void write(Path file, Map<String, String> payload) throws IOException {
        StringBuilder content = new StringBuilder();
        for (Map.Entry<String, String> entry : payload.entrySet()) {
            content.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
        }
        Files.writeString(file, content.toString());
    }
}
