package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.config.CachingLimitResolver;
import io.quotaflow.config.ConfigReloader;
import io.quotaflow.config.ConfigurationParser;
import io.quotaflow.config.MapConfigSource;
import io.quotaflow.core.Decision;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.Limit;
import io.quotaflow.core.RateLimitContext;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tariff resolution conformance: a policy declares a dynamic limit reference;
 * the resolver's tariff change is adopted after the resolution cache TTL
 * expires, without a restart, and within the TTL the hot path performs no
 * resolver calls.
 */
class TariffResolutionTckTest {

    private static final Duration TTL = Duration.ofMillis(300);

    private static final RateLimitContext ALICE =
            RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "alice").build();

    private static Map<String, String> payload() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("quotaflow.policies.u.scope", "user");
        map.put("quotaflow.policies.u.limit-ref", "tariff");
        return map;
    }

    private final AtomicInteger resolverCalls = new AtomicInteger();
    private final AtomicReference<Optional<Limit>> tariff = new AtomicReference<>();

    private CachingLimitResolver cachingResolver() {
        return CachingLimitResolver.wrap((limitRef, keyGroup) -> {
            resolverCalls.incrementAndGet();
            return tariff.get();
        }, TTL);
    }

    private DefaultQuotaFlow quotaFlow(CachingLimitResolver resolver) {
        return DefaultQuotaFlow
                .builder(ConfigurationParser.parse(payload()).policySet(), new LocalRateLimitStore())
                .limitResolver(resolver)
                .build();
    }

    @Test
    void tariffChangeAppliesAfterTtlWithoutRestart() throws Exception {
        // old tariff: capacity 2, refill frozen; new tariff: capacity 10, fast refill
        tariff.set(Optional.of(new Limit(2, 1, Duration.ofHours(1))));
        CachingLimitResolver resolver = cachingResolver();
        DefaultQuotaFlow quotaFlow = quotaFlow(resolver);

        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
        assertFalse(quotaFlow.tryAcquire("u", ALICE).isAllowed(), "old tariff capacity exhausted");
        assertEquals(1, resolverCalls.get());
        for (int i = 0; i < 20; i++) {
            quotaFlow.tryAcquire("u", ALICE);
        }
        assertEquals(1, resolverCalls.get(), "no resolver calls within the TTL");

        tariff.set(Optional.of(new Limit(10, 100, Duration.ofSeconds(1))));
        long changedAt = System.nanoTime();

        // before the TTL expires the cached (old) tariff keeps governing
        while (System.nanoTime() - changedAt < TTL.toNanos()) {
            assertFalse(quotaFlow.tryAcquire("u", ALICE).isAllowed(),
                    "tariff change must not apply before the TTL expires");
            Thread.sleep(5);
        }
        assertEquals(1, resolverCalls.get());

        // after the TTL the new tariff is adopted, without a restart
        awaitTrue(() -> quotaFlow.tryAcquire("u", ALICE).isAllowed(),
                Duration.ofSeconds(2), "new tariff adopted after TTL");
        assertEquals(2, resolverCalls.get());
    }

    @Test
    void reloadClearsTheResolutionCache() throws Exception {
        tariff.set(Optional.of(new Limit(1, 1, Duration.ofHours(1))));
        CachingLimitResolver resolver = cachingResolver();
        DefaultQuotaFlow quotaFlow = quotaFlow(resolver);
        ConfigReloader reloader = ConfigReloader
                .builder(new MapConfigSource(payload()), quotaFlow)
                .pollInterval(Duration.ZERO)
                .onApplied(resolver::clear)
                .build();

        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
        assertFalse(quotaFlow.tryAcquire("u", ALICE).isAllowed());
        assertEquals(1, resolverCalls.get());

        tariff.set(Optional.of(new Limit(5, 100, Duration.ofSeconds(1))));
        assertTrue(reloader.reload().applied());
        awaitTrue(() -> quotaFlow.tryAcquire("u", ALICE).isAllowed(),
                Duration.ofSeconds(2), "reload cleared the cache; the new tariff resolves");
        assertEquals(2, resolverCalls.get());
    }

    @Test
    void unresolvableTariffRejectsWithoutRetrySchedule() {
        tariff.set(Optional.empty());
        DefaultQuotaFlow quotaFlow = quotaFlow(cachingResolver());
        Decision decision = quotaFlow.tryAcquire("u", ALICE);
        assertFalse(decision.isAllowed());
        assertTrue(decision.retryAfter().isEmpty());
        assertEquals("u", decision.policyId());
    }

    private static void awaitTrue(Check condition, Duration timeout, String description)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.check()) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timed out waiting for: " + description);
    }

    @FunctionalInterface
    private interface Check {
        boolean check();
    }
}
