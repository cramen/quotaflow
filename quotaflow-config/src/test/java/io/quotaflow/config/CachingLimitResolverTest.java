package io.quotaflow.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.Limit;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CachingLimitResolverTest {

    private static final Limit LIMIT = new Limit(10, 10, Duration.ofSeconds(1));

    private final AtomicLong nanos = new AtomicLong();
    private final AtomicInteger delegateCalls = new AtomicInteger();
    private final AtomicReference<Optional<Limit>> delegateResult =
            new AtomicReference<>(Optional.of(LIMIT));

    private CachingLimitResolver resolver(Duration ttl) {
        return new CachingLimitResolver(
                (ref, group) -> {
                    delegateCalls.incrementAndGet();
                    return delegateResult.get();
                },
                ttl, nanos::get);
    }

    @Test
    void withinTtlTheDelegateIsCalledOncePerRefAndKeyGroup() {
        CachingLimitResolver caching = resolver(Duration.ofSeconds(60));
        for (int i = 0; i < 100; i++) {
            assertEquals(Optional.of(LIMIT), caching.resolve("tariff", "tenant"));
        }
        assertEquals(1, delegateCalls.get());
        caching.resolve("tariff", "user");
        caching.resolve("other", "tenant");
        assertEquals(3, delegateCalls.get(), "cache identity is (limitRef, keyGroup)");
    }

    @Test
    void afterTtlTheDelegateIsConsultedAgain() {
        CachingLimitResolver caching = resolver(Duration.ofSeconds(60));
        caching.resolve("tariff", "tenant");
        nanos.addAndGet(Duration.ofSeconds(59).toNanos());
        caching.resolve("tariff", "tenant");
        assertEquals(1, delegateCalls.get());
        nanos.addAndGet(Duration.ofSeconds(2).toNanos());
        Limit upgraded = new Limit(50, 50, Duration.ofSeconds(1));
        delegateResult.set(Optional.of(upgraded));
        assertEquals(Optional.of(upgraded), caching.resolve("tariff", "tenant"));
        assertEquals(2, delegateCalls.get());
    }

    @Test
    void unresolvableResultsAreCachedWithinTtl() {
        delegateResult.set(Optional.empty());
        CachingLimitResolver caching = resolver(Duration.ofSeconds(60));
        assertTrue(caching.resolve("tariff", "tenant").isEmpty());
        assertTrue(caching.resolve("tariff", "tenant").isEmpty());
        assertEquals(1, delegateCalls.get());
    }

    @Test
    void clearDropsAllCachedResolutions() {
        CachingLimitResolver caching = resolver(Duration.ofSeconds(60));
        caching.resolve("tariff", "tenant");
        assertEquals(1, caching.cacheSize());
        caching.clear();
        assertEquals(0, caching.cacheSize());
        caching.resolve("tariff", "tenant");
        assertEquals(2, delegateCalls.get());
    }

    @Test
    void rejectsInvalidConstruction() {
        assertThrows(NullPointerException.class, () -> CachingLimitResolver.wrap(null));
        assertThrows(IllegalArgumentException.class,
                () -> CachingLimitResolver.wrap((ref, group) -> Optional.empty(), Duration.ZERO));
        assertThrows(NullPointerException.class,
                () -> CachingLimitResolver.wrap((ref, group) -> Optional.empty(), null));
        CachingLimitResolver caching = resolver(Duration.ofSeconds(1));
        assertThrows(NullPointerException.class, () -> caching.resolve(null, "tenant"));
        assertThrows(NullPointerException.class, () -> caching.resolve("tariff", null));
    }
}
