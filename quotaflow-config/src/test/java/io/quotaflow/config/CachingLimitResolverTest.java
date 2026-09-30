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
    @org.junit.jupiter.api.Test void wrappingPreservesCompleteVersionedSnapshots() {
        var key = new io.quotaflow.core.LimitSnapshot.Key("plan", "global");
        var first = new io.quotaflow.core.LimitSnapshot(1, java.util.Map.of(key, new io.quotaflow.core.Limit(10, 1, java.time.Duration.ofSeconds(1))));
        var published = new java.util.concurrent.atomic.AtomicReference<java.util.Optional<io.quotaflow.core.LimitSnapshot>>(java.util.Optional.of(first));
        io.quotaflow.core.VersionedLimitResolver provider = published::get;
        var wrapped = CachingLimitResolver.wrap(provider);
        org.junit.jupiter.api.Assertions.assertInstanceOf(io.quotaflow.core.VersionedLimitResolver.class, wrapped);
        org.junit.jupiter.api.Assertions.assertSame(first, ((io.quotaflow.core.VersionedLimitResolver) wrapped).snapshot().orElseThrow());
        var second = new io.quotaflow.core.LimitSnapshot(2, java.util.Map.of(key, new io.quotaflow.core.Limit(1, 1, java.time.Duration.ofSeconds(1))));
        published.set(java.util.Optional.of(second));
        org.junit.jupiter.api.Assertions.assertEquals(java.util.Optional.of(second.limits().get(key)), wrapped.resolve("plan", "global"));
        wrapped.clear();
        org.junit.jupiter.api.Assertions.assertSame(second, ((io.quotaflow.core.VersionedLimitResolver) wrapped).snapshot().orElseThrow());
        published.set(java.util.Optional.empty()); org.junit.jupiter.api.Assertions.assertTrue(wrapped.resolve("plan", "global").isEmpty());
    }
}
