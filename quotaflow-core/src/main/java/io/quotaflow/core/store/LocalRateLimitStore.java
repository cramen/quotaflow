package io.quotaflow.core.store;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * In-memory {@link BatchRateLimitStore} implementing both token bucket and
 * GCRA with identical semantics, so a degraded instance executes the same
 * algorithm as the distributed store, just with smaller limits.
 *
 * <p>State lives in immutable cells inside a {@link ConcurrentHashMap} updated
 * through lock-free compare-and-set loops: no locks, no {@code synchronized},
 * no blocking — safe for virtual threads. Each cell carries the limit it was
 * last evaluated with, so {@link #snapshot()} can report remaining capacity
 * without external bookkeeping.
 *
 * <p>The two algorithms are parameterized to be exactly equivalent: with
 * emission interval {@code T = refillPeriod / refillAmount} and GCRA tolerance
 * {@code tau = capacity * T}, both admit a burst of exactly {@code capacity}
 * from a full bucket and refill at the same rate.
 *
 * <p>Chain evaluation deducts level by level through the same CAS cells and
 * compensates already-deducted levels with an exact inverse operation when a
 * later level rejects, so once the call returns a rejection has consumed
 * nothing anywhere. The compensation is additive (it credits the deducted
 * weight back rather than restoring a snapshot), so it commutes with
 * concurrent acquisitions of the same cells.
 */
public final class LocalRateLimitStore implements BatchRateLimitStore {

    // Identity equality is intentional: CAS loops compare the exact instance read.
    private static final class TokenBucketState {
        private final double tokens;
        private final long lastRefillNanos;
        private final Limit limit;

        TokenBucketState(double tokens, long lastRefillNanos, Limit limit) {
            this.tokens = tokens;
            this.lastRefillNanos = lastRefillNanos;
            this.limit = limit;
        }
    }

    private static final class GcraState {
        private final double tatNanos;
        private final Limit limit;

        GcraState(double tatNanos, Limit limit) {
            this.tatNanos = tatNanos;
            this.limit = limit;
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

    /**
     * Evaluates the chain level by level against current state and deducts at
     * every level only if all levels admit the request; a rejection at any
     * level rolls the earlier levels' deductions back, so a rejected chain has
     * consumed nothing once this call returns.
     */
    @Override
    public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
        Objects.requireNonNull(chain, "chain");
        if (chain.isEmpty()) {
            throw new IllegalArgumentException("chain must not be empty");
        }
        List<StoreResult> admitted = new ArrayList<>(chain.size());
        for (int i = 0; i < chain.size(); i++) {
            LevelRequest level = chain.get(i);
            StoreResult result =
                    tryAcquire(level.storageKey(), level.limit(), level.algorithm(), level.weight());
            if (!result.acquired()) {
                for (int j = i - 1; j >= 0; j--) {
                    LevelRequest committed = chain.get(j);
                    rollback(
                            committed.storageKey(),
                            committed.limit(),
                            committed.algorithm(),
                            committed.weight());
                }
                return CompletableFuture.completedFuture(
                        ChainResult.rejected(i, result.remaining(), result.retryAfterMillis()));
            }
            admitted.add(result);
        }
        long minRemaining = Long.MAX_VALUE;
        for (StoreResult result : admitted) {
            minRemaining = Math.min(minRemaining, result.remaining());
        }
        return CompletableFuture.completedFuture(ChainResult.acquired(chain.size() - 1, minRemaining));
    }

    /**
     * Snapshot of the locally active buckets: every cell that still carries
     * information (a token bucket that has not refilled to full, a GCRA cell
     * whose TAT lies in the future) with its remaining whole tokens. Cells
     * that have refilled to full or drained carry no information and are
     * evicted instead, approximating the distributed store's TTL expiry and
     * bounding the snapshot to keys active in the recent window.
     *
     * <p>The snapshot is weakly consistent (taken from a live concurrent map)
     * and intended for best-effort state replay only.
     */
    public List<BucketState> snapshot() {
        long now = nanoClock.getAsLong();
        List<BucketState> active = new ArrayList<>();
        for (Map.Entry<String, Object> entry : cells.entrySet()) {
            Object cell = entry.getValue();
            if (cell instanceof TokenBucketState state) {
                Limit limit = state.limit;
                double interval = limit.emissionIntervalNanos();
                long elapsed = Math.max(0, now - state.lastRefillNanos);
                double tokens = Math.min(limit.capacity(), state.tokens + elapsed / interval);
                if (tokens >= limit.capacity()) {
                    cells.remove(entry.getKey(), cell);
                } else {
                    active.add(new BucketState(
                            entry.getKey(), limit, Algorithm.TOKEN_BUCKET, (long) tokens));
                }
            } else if (cell instanceof GcraState state) {
                Limit limit = state.limit;
                if (state.tatNanos <= now) {
                    cells.remove(entry.getKey(), cell);
                } else {
                    double interval = limit.emissionIntervalNanos();
                    double tau = limit.capacity() * interval;
                    long remaining = remaining(tau - (state.tatNanos - now), interval, limit.capacity());
                    active.add(new BucketState(entry.getKey(), limit, Algorithm.GCRA, remaining));
                }
            }
        }
        return active;
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
                TokenBucketState next = new TokenBucketState(tokens - weight, now, limit);
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
                GcraState next = new GcraState(candidate, limit);
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

    /**
     * Credits {@code weight} back to the bucket, undoing an earlier deduction.
     * The credit is additive (not a snapshot restore), so it commutes with
     * concurrent acquisitions; a cell that disappeared in between — swept
     * because it had refilled to full — needs no credit.
     */
    private void rollback(String storageKey, Limit limit, Algorithm algorithm, long weight) {
        switch (algorithm) {
            case TOKEN_BUCKET -> rollbackTokenBucket(storageKey, limit, weight);
            case GCRA -> rollbackGcra(storageKey, limit, weight);
        }
    }

    private void rollbackTokenBucket(String storageKey, Limit limit, long weight) {
        double interval = limit.emissionIntervalNanos();
        double capacity = limit.capacity();
        while (true) {
            long now = nanoClock.getAsLong();
            Object current = cells.get(storageKey);
            if (!(current instanceof TokenBucketState state)) {
                return;
            }
            long elapsed = Math.max(0, now - state.lastRefillNanos);
            double tokens = Math.min(capacity, state.tokens + elapsed / interval);
            TokenBucketState next = new TokenBucketState(Math.min(capacity, tokens + weight), now, limit);
            if (cells.replace(storageKey, current, next)) {
                return;
            }
        }
    }

    private void rollbackGcra(String storageKey, Limit limit, long weight) {
        double interval = limit.emissionIntervalNanos();
        while (true) {
            Object current = cells.get(storageKey);
            if (!(current instanceof GcraState state)) {
                return;
            }
            GcraState next = new GcraState(state.tatNanos - weight * interval, limit);
            if (cells.replace(storageKey, current, next)) {
                return;
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
