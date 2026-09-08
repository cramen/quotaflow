package io.quotaflow.core.store;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * In-memory {@link RateLimitStore} implementing both token bucket and GCRA
 * with identical semantics, so a degraded instance executes the same algorithm
 * as the distributed store, just with smaller limits.
 *
 * <p>State lives in immutable cells inside a {@link ConcurrentHashMap} updated
 * through lock-free compare-and-set loops: no locks, no {@code synchronized},
 * no blocking — safe for virtual threads.
 *
 * <p>The two algorithms are parameterized to be exactly equivalent: with
 * emission interval {@code T = refillPeriod / refillAmount} and GCRA tolerance
 * {@code tau = capacity * T}, both admit a burst of exactly {@code capacity}
 * from a full bucket and refill at the same rate.
 */
public final class LocalRateLimitStore implements RateLimitStore {

    // Identity equality is intentional: CAS loops compare the exact instance read.
    private static final class TokenBucketState {
        private final double tokens;
        private final long lastRefillNanos;

        TokenBucketState(double tokens, long lastRefillNanos) {
            this.tokens = tokens;
            this.lastRefillNanos = lastRefillNanos;
        }
    }

    private static final class GcraState {
        private final double tatNanos;

        GcraState(double tatNanos) {
            this.tatNanos = tatNanos;
        }
    }

    private final ConcurrentHashMap<String, Object> cells = new ConcurrentHashMap<>();
    private final LongSupplier nanoClock;

    public LocalRateLimitStore() {
        this(System::nanoTime);
    }

    /** @param nanoClock monotonic nanosecond clock; injectable for deterministic tests */
    public LocalRateLimitStore(LongSupplier nanoClock) {
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    @Override
    public StoreResult tryAcquire(String storageKey, Limit limit, Algorithm algorithm, long weight) {
        Objects.requireNonNull(storageKey, "storageKey");
        Objects.requireNonNull(limit, "limit");
        Objects.requireNonNull(algorithm, "algorithm");
        if (weight < 1) {
            throw new IllegalArgumentException("weight must be >= 1, got " + weight);
        }
        return switch (algorithm) {
            case TOKEN_BUCKET -> acquireTokenBucket(storageKey, limit, weight);
            case GCRA -> acquireGcra(storageKey, limit, weight);
        };
    }

    @Override
    public CompletionStage<StoreResult> tryAcquireAsync(
            String storageKey, Limit limit, Algorithm algorithm, long weight) {
        return CompletableFuture.completedFuture(tryAcquire(storageKey, limit, algorithm, weight));
    }

    /** Visible for tests: number of live state cells. */
    public int cellCount() {
        return cells.size();
    }

    private StoreResult acquireTokenBucket(String storageKey, Limit limit, long weight) {
        double interval = limit.emissionIntervalNanos();
        double capacity = limit.capacity();
        while (true) {
            long now = nanoClock.getAsLong();
            Object current = cells.get(storageKey);
            TokenBucketState state = current instanceof TokenBucketState s ? s : null;
            double tokens = capacity;
            if (state != null) {
                long elapsed = Math.max(0, now - state.lastRefillNanos);
                tokens = Math.min(capacity, state.tokens + elapsed / interval);
            }
            if (tokens >= weight) {
                TokenBucketState next = new TokenBucketState(tokens - weight, now);
                if (compareAndSet(storageKey, current, next)) {
                    return StoreResult.acquired((long) next.tokens);
                }
            } else {
                long retryAfterMillis = toMillisCeil((weight - tokens) * interval);
                return StoreResult.rejected((long) tokens, retryAfterMillis);
            }
        }
    }

    private StoreResult acquireGcra(String storageKey, Limit limit, long weight) {
        double interval = limit.emissionIntervalNanos();
        double tau = limit.capacity() * interval;
        while (true) {
            long now = nanoClock.getAsLong();
            Object current = cells.get(storageKey);
            GcraState state = current instanceof GcraState s ? s : null;
            double tat = state == null ? now : Math.max(state.tatNanos, now);
            double candidate = tat + weight * interval;
            double overdraft = candidate - now;
            if (overdraft <= tau) {
                GcraState next = new GcraState(candidate);
                if (compareAndSet(storageKey, current, next)) {
                    return StoreResult.acquired(remaining(tau - overdraft, interval, limit.capacity()));
                }
            } else {
                long remaining = remaining(tau - (tat - now), interval, limit.capacity());
                long retryAfterMillis = toMillisCeil(overdraft - tau);
                return StoreResult.rejected(remaining, retryAfterMillis);
            }
        }
    }

    private static long remaining(double headroomNanos, double interval, long capacity) {
        long remaining = (long) Math.floor(headroomNanos / interval);
        return Math.max(0, Math.min(capacity, remaining));
    }

    private static long toMillisCeil(double nanos) {
        return Math.max(1, (long) Math.ceil(nanos / 1_000_000.0));
    }

    private boolean compareAndSet(String storageKey, Object expected, Object next) {
        if (expected == null) {
            return cells.putIfAbsent(storageKey, next) == null;
        }
        return cells.replace(storageKey, expected, next);
    }
}
