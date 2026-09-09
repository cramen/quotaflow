package io.quotaflow.config;

import io.quotaflow.core.Limit;
import io.quotaflow.core.LimitResolver;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * TTL-caching wrapper around a tariff {@link LimitResolver} delegate, keeping
 * resolver calls off the hot path: resolution results are cached per
 * {@code (limitRef, keyGroup)} for a configurable TTL (default 60 s), so
 * within the TTL decisions cost one {@link ConcurrentHashMap} read and the
 * delegate is never invoked. Empty (unresolvable) results are cached too, so
 * a failing tariff lookup cannot stampede the delegate.
 *
 * <p>The key group — never the raw key — is the cache and resolver identity,
 * keeping cardinality bounded by construction. {@link #clear()} invalidates
 * everything; wire it into {@link ConfigReloader.Builder#onApplied(Runnable)}
 * so configuration reloads drop cached resolutions along with the old policy
 * set.
 */
public final class CachingLimitResolver implements LimitResolver {

    /** Default resolution cache TTL. */
    public static final Duration DEFAULT_TTL = Duration.ofSeconds(60);

    private final LimitResolver delegate;
    private final long ttlNanos;
    private final LongSupplier nanoClock;
    private final ConcurrentHashMap<CacheKey, Entry> cache = new ConcurrentHashMap<>();

    private record CacheKey(String limitRef, String keyGroup) {
    }

    private record Entry(Optional<Limit> limit, long expiresAtNanos) {
    }

    public static CachingLimitResolver wrap(LimitResolver delegate) {
        return wrap(delegate, DEFAULT_TTL);
    }

    public static CachingLimitResolver wrap(LimitResolver delegate, Duration ttl) {
        return new CachingLimitResolver(delegate, ttl, System::nanoTime);
    }

    CachingLimitResolver(LimitResolver delegate, Duration ttl, LongSupplier nanoClock) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be positive, got " + ttl);
        }
        this.ttlNanos = ttl.toNanos();
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    @Override
    public Optional<Limit> resolve(String limitRef, String keyGroup) {
        CacheKey key = new CacheKey(
                Objects.requireNonNull(limitRef, "limitRef"),
                Objects.requireNonNull(keyGroup, "keyGroup"));
        long now = nanoClock.getAsLong();
        Entry cached = cache.get(key);
        if (cached != null && now < cached.expiresAtNanos()) {
            return cached.limit();
        }
        Optional<Limit> resolved = delegate.resolve(limitRef, keyGroup);
        cache.put(key, new Entry(resolved, now + ttlNanos));
        return resolved;
    }

    /** Drops all cached resolutions; the next decision per key re-resolves. */
    public void clear() {
        cache.clear();
    }

    /** Visible for tests and inspection: number of cached resolutions. */
    public int cacheSize() {
        return cache.size();
    }
}
