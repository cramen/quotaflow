package io.quotaflow.fallback;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.Verdict;
import io.quotaflow.core.store.BatchRateLimitStore;
import io.quotaflow.core.store.BucketState;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.StateSeeder;
import io.quotaflow.core.store.StoreResult;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchRateLimitStore} that delegates to a primary (distributed)
 * store while it is healthy and degrades to a conservative local limiter when
 * the primary fails or stalls. No store exception escapes to the caller: a
 * failed primary call trips an embedded circuit breaker and the request is
 * decided locally instead.
 *
 * <p>Degraded mode divides every level's limit by the configured
 * {@code expectedInstances} (token bucket: capacity and refill divided with a
 * floor of 1; GCRA: emission interval multiplied), so the summed degraded flow
 * across all instances stays within the global limit. Degraded chains are
 * evaluated through the local batch path, so fired level, remaining and
 * retry-after read exactly like healthy-mode decisions. The requested weight
 * is a property of the request and passes through unscaled.
 *
 * <p>Recovery rides a real request: after the open duration the first caller
 * probes the primary; on success the wrapper replays the local store's active
 * bucket state into the primary (best-effort, capped, conservative merge) and
 * only then closes the breaker, so the store does not serve full post-outage
 * buckets and provoke a pass-through spike. Seeding failure reopens the
 * breaker and recovery is retried after the next open duration.
 *
 * <p>Every transition and every fallback decision is reported to
 * {@link DegradationListener}s and transitions are logged, always without raw
 * limit keys.
 */
public final class FallbackRateLimitStore implements BatchRateLimitStore {

    private static final Logger log = LoggerFactory.getLogger(FallbackRateLimitStore.class);

    private static final String UNKNOWN_GROUP = "unknown";

    private final BatchRateLimitStore primary;
    private final LocalRateLimitStore local;
    private final StateSeeder seeder;
    private final FallbackConfig config;
    private final List<DegradationListener> listeners;
    private final Function<String, String> keyGroupExtractor;
    private final CircuitBreaker breaker;

    /**
     * Chains served locally during the current outage: leaf storage key to the
     * original (unscaled) level requests, so recovery seeding can address the
     * same distributed state and translate scaled remaining tokens back to
     * full-limit scale.
     */
    private final ConcurrentHashMap<String, Map<String, LevelRequest>> trackedChains =
            new ConcurrentHashMap<>();
    private final AtomicInteger trackedEntries = new AtomicInteger();
    private final AtomicBoolean trackingCapLogged = new AtomicBoolean();

    /**
     * Wrapper with a fresh local store, the system monotonic clock and the
     * default key-group extractor (the scope segment of the storage key).
     */
    public FallbackRateLimitStore(
            BatchRateLimitStore primary,
            StateSeeder seeder,
            FallbackConfig config,
            List<DegradationListener> listeners) {
        this(primary, new LocalRateLimitStore(), seeder, config, listeners,
                System::nanoTime, FallbackRateLimitStore::defaultKeyGroup);
    }

    /**
     * Full constructor for tests and custom wiring.
     *
     * @param nanoClock monotonic nanosecond clock driving breaker timing
     * @param keyGroupExtractor maps an engine storage key to the aggregated
     *                          key-group identity reported to listeners; must
     *                          never return the raw key
     */
    public FallbackRateLimitStore(
            BatchRateLimitStore primary,
            LocalRateLimitStore local,
            StateSeeder seeder,
            FallbackConfig config,
            List<DegradationListener> listeners,
            LongSupplier nanoClock,
            Function<String, String> keyGroupExtractor) {
        this.primary = Objects.requireNonNull(primary, "primary");
        this.local = Objects.requireNonNull(local, "local");
        this.seeder = Objects.requireNonNull(seeder, "seeder");
        this.config = Objects.requireNonNull(config, "config");
        this.listeners = List.copyOf(Objects.requireNonNull(listeners, "listeners"));
        this.keyGroupExtractor = Objects.requireNonNull(keyGroupExtractor, "keyGroupExtractor");
        this.breaker = new CircuitBreaker(config, nanoClock, this::onTransition);
        if (config.expectedInstances() == 1) {
            log.warn("expectedInstances is not configured (default 1): in degraded mode each"
                    + " instance enforces the full limit, so the summed degraded flow may exceed"
                    + " the global limit when more than one instance runs");
        }
    }

    /** Current degradation state; visible for tests and inspection. */
    public DegradationState state() {
        return breaker.state();
    }

    @Override
    public StoreResult tryAcquire(String storageKey, Limit limit, Algorithm algorithm, long weight) {
        Objects.requireNonNull(storageKey, "storageKey");
        Objects.requireNonNull(limit, "limit");
        Objects.requireNonNull(algorithm, "algorithm");
        if (weight < 1) {
            throw new IllegalArgumentException("weight must be >= 1, got " + weight);
        }
        CircuitBreaker.Call call = breaker.permitCall();
        if (call == CircuitBreaker.Call.LOCAL) {
            return serveLocally(storageKey, limit, algorithm, weight);
        }
        try {
            StoreResult result = primary.tryAcquire(storageKey, limit, algorithm, weight);
            onPrimarySuccess(call);
            return result;
        } catch (RuntimeException e) {
            if (isCallerError(e)) {
                throw e;
            }
            onPrimaryFailure(call, e);
            return serveLocally(storageKey, limit, algorithm, weight);
        }
    }

    @Override
    public CompletionStage<StoreResult> tryAcquireAsync(
            String storageKey, Limit limit, Algorithm algorithm, long weight) {
        Objects.requireNonNull(storageKey, "storageKey");
        Objects.requireNonNull(limit, "limit");
        Objects.requireNonNull(algorithm, "algorithm");
        if (weight < 1) {
            throw new IllegalArgumentException("weight must be >= 1, got " + weight);
        }
        CircuitBreaker.Call call = breaker.permitCall();
        if (call == CircuitBreaker.Call.LOCAL) {
            return CompletableFuture.completedFuture(serveLocally(storageKey, limit, algorithm, weight));
        }
        CompletionStage<StoreResult> primaryCall;
        try {
            primaryCall = primary.tryAcquireAsync(storageKey, limit, algorithm, weight);
        } catch (RuntimeException e) {
            if (isCallerError(e)) {
                throw e;
            }
            onPrimaryFailure(call, e);
            return CompletableFuture.completedFuture(serveLocally(storageKey, limit, algorithm, weight));
        }
        return primaryCall.handle((result, error) -> {
            if (error == null) {
                onPrimarySuccess(call);
                return result;
            }
            Throwable cause = unwrap(error);
            if (isCallerError(cause)) {
                throw new CompletionException(cause);
            }
            onPrimaryFailure(call, cause);
            return serveLocally(storageKey, limit, algorithm, weight);
        });
    }

    @Override
    public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
        Objects.requireNonNull(chain, "chain");
        if (chain.isEmpty()) {
            throw new IllegalArgumentException("chain must not be empty");
        }
        CircuitBreaker.Call call = breaker.permitCall();
        if (call == CircuitBreaker.Call.LOCAL) {
            return CompletableFuture.completedFuture(serveLocally(chain));
        }
        CompletionStage<ChainResult> primaryCall;
        try {
            primaryCall = primary.tryAcquireAll(chain);
        } catch (RuntimeException e) {
            if (isCallerError(e)) {
                throw e;
            }
            onPrimaryFailure(call, e);
            return CompletableFuture.completedFuture(serveLocally(chain));
        }
        return primaryCall.handle((result, error) -> {
            if (error == null) {
                onPrimarySuccess(call);
                return result;
            }
            Throwable cause = unwrap(error);
            if (isCallerError(cause)) {
                throw new CompletionException(cause);
            }
            onPrimaryFailure(call, cause);
            return serveLocally(chain);
        });
    }

    private void onPrimarySuccess(CircuitBreaker.Call call) {
        if (call == CircuitBreaker.Call.PROBE) {
            initiateRecovery();
        } else {
            breaker.onSuccess();
        }
    }

    private void onPrimaryFailure(CircuitBreaker.Call call, Throwable error) {
        if (call == CircuitBreaker.Call.PROBE) {
            breaker.onFailure("probe failed (" + error.getClass().getSimpleName() + ")");
        } else {
            breaker.onFailure(
                    "primary store call failed (" + error.getClass().getSimpleName() + ")");
        }
    }

    /**
     * Probe succeeded: replay the local store's active buckets into the
     * primary, then close the breaker. The breaker stays HALF_OPEN (all other
     * traffic local) until seeding completes; a seeding failure reopens it.
     */
    private void initiateRecovery() {
        List<BucketState> active = local.snapshot();
        Map<String, List<BucketState>> byChain = groupByChain(active);
        int total = byChain.values().stream().mapToInt(List::size).sum();
        int overflow = total - config.maxSeedEntries();
        if (overflow > 0) {
            log.warn(
                    "seeding is capped at {} entries; skipping {} of {} active buckets",
                    config.maxSeedEntries(), overflow, total);
        }
        List<CompletionStage<Void>> flushes = new ArrayList<>(byChain.size());
        int budget = config.maxSeedEntries();
        for (Map.Entry<String, List<BucketState>> chain : byChain.entrySet()) {
            List<BucketState> buckets = chain.getValue();
            if (buckets.size() > budget) {
                buckets = buckets.subList(0, budget);
            }
            budget -= buckets.size();
            if (!buckets.isEmpty()) {
                flushes.add(seeder.seed(chain.getKey(), buckets));
            }
        }
        CompletableFuture
                .allOf(flushes.stream().map(CompletionStage::toCompletableFuture)
                        .toArray(CompletableFuture[]::new))
                .whenComplete((ignored, error) -> {
                    if (error == null) {
                        trackedChains.clear();
                        trackedEntries.set(0);
                        trackingCapLogged.set(false);
                        breaker.onSeedingSuccess(
                                "probe succeeded and " + total + " local buckets replayed");
                    } else {
                        breaker.onSeedingFailure(
                                "seeding failed (" + unwrap(error).getClass().getSimpleName() + ")");
                    }
                });
    }

    /**
     * Groups active buckets by the chain they were served under and translates
     * them back to full-limit scale: the local cells hold limits divided by
     * {@code expectedInstances}, so the remaining measured locally is
     * multiplied back (one instance's remaining of r out of capacity/N means
     * N*r of the shared capacity is unused). Buckets without tracking
     * information (leftover cells) seed as their own chain with the limit the
     * cell carries, which under-estimates remaining — the conservative
     * direction.
     */
    private Map<String, List<BucketState>> groupByChain(List<BucketState> active) {
        Map<String, BucketState> byKey = new LinkedHashMap<>();
        for (BucketState bucket : active) {
            byKey.put(bucket.storageKey(), bucket);
        }
        Map<String, List<BucketState>> byChain = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, LevelRequest>> chain : trackedChains.entrySet()) {
            List<BucketState> buckets = new ArrayList<>(chain.getValue().size());
            for (Map.Entry<String, LevelRequest> level : chain.getValue().entrySet()) {
                BucketState bucket = byKey.remove(level.getKey());
                if (bucket != null) {
                    buckets.add(toFullScale(bucket, level.getValue()));
                }
            }
            if (!buckets.isEmpty()) {
                byChain.put(chain.getKey(), buckets);
            }
        }
        for (BucketState untracked : byKey.values()) {
            byChain.put(untracked.storageKey(), List.of(untracked));
        }
        return byChain;
    }

    private BucketState toFullScale(BucketState localBucket, LevelRequest original) {
        long capacity = original.limit().capacity();
        int instances = config.expectedInstances();
        long remaining = localBucket.remaining();
        if (instances > 1) {
            remaining = (long) Math.min((double) capacity, (double) remaining * instances);
        }
        return new BucketState(
                localBucket.storageKey(), original.limit(), original.algorithm(),
                Math.min(capacity, remaining));
    }

    private ChainResult serveLocally(List<LevelRequest> chain) {
        track(chain);
        List<LevelRequest> scaled = new ArrayList<>(chain.size());
        for (LevelRequest level : chain) {
            scaled.add(scale(level));
        }
        ChainResult result = local.tryAcquireAll(scaled).toCompletableFuture().join();
        LevelRequest fired = chain.get(result.firedLevelIndex());
        notifyFallbackDecision(fired.storageKey(), result.acquired());
        return result;
    }

    private StoreResult serveLocally(String storageKey, Limit limit, Algorithm algorithm, long weight) {
        if (trackedEntries.get() < config.maxSeedEntries()
                && trackedChains
                        .computeIfAbsent(storageKey, key -> new ConcurrentHashMap<>())
                        .putIfAbsent(storageKey, new LevelRequest(storageKey, limit, algorithm, weight))
                        == null) {
            trackedEntries.incrementAndGet();
        }
        StoreResult result = local.tryAcquire(storageKey, scale(limit, algorithm), algorithm, weight);
        notifyFallbackDecision(storageKey, result.acquired());
        return result;
    }

    /**
     * Remembers the chain layout of a locally served request so recovery
     * seeding can address the same state. Tracking is capped at
     * {@code maxSeedEntries} level entries; beyond the cap new chains are
     * still served but no longer tracked (logged once per outage).
     */
    private void track(List<LevelRequest> chain) {
        if (trackedEntries.get() >= config.maxSeedEntries()) {
            if (trackingCapLogged.compareAndSet(false, true)) {
                log.warn("seed tracking is capped at {} level entries; further chains served"
                        + " during this outage will not be seeded on recovery", config.maxSeedEntries());
            }
            return;
        }
        String leaf = chain.get(chain.size() - 1).storageKey();
        Map<String, LevelRequest> members =
                trackedChains.computeIfAbsent(leaf, key -> new ConcurrentHashMap<>());
        for (LevelRequest level : chain) {
            if (members.putIfAbsent(level.storageKey(), level) == null) {
                trackedEntries.incrementAndGet();
            }
        }
    }

    private LevelRequest scale(LevelRequest level) {
        return new LevelRequest(level.storageKey(),
                scale(level.limit(), level.algorithm()), level.algorithm(), level.weight());
    }

    private Limit scale(Limit limit, Algorithm algorithm) {
        int instances = config.expectedInstances();
        if (instances == 1) {
            return limit;
        }
        return switch (algorithm) {
            case TOKEN_BUCKET -> new Limit(
                    Math.max(1, limit.capacity() / instances),
                    Math.max(1, limit.refillAmount() / instances),
                    limit.refillPeriod());
            case GCRA -> new Limit(
                    limit.capacity(), limit.refillAmount(), limit.refillPeriod().multipliedBy(instances));
        };
    }

    private void onTransition(DegradationState from, DegradationState to, String reason) {
        log.info("Degradation state transition {} -> {}: {}", from, to, reason);
        for (DegradationListener listener : listeners) {
            try {
                listener.onTransition(from, to, reason);
            } catch (RuntimeException e) {
                log.warn("DegradationListener {} failed on transition",
                        listener.getClass().getSimpleName(), e);
            }
        }
    }

    private void notifyFallbackDecision(String storageKey, boolean acquired) {
        if (listeners.isEmpty()) {
            return;
        }
        String policyId = policyIdOf(storageKey);
        String keyGroup = keyGroupExtractor.apply(storageKey);
        Verdict verdict = acquired ? Verdict.ALLOWED : Verdict.REJECTED;
        for (DegradationListener listener : listeners) {
            try {
                listener.onFallbackDecision(policyId, keyGroup, verdict);
            } catch (RuntimeException e) {
                log.warn("DegradationListener {} failed on fallback decision",
                        listener.getClass().getSimpleName(), e);
            }
        }
    }

    private static String policyIdOf(String storageKey) {
        int first = storageKey.indexOf(':');
        return first < 0 ? UNKNOWN_GROUP : storageKey.substring(0, first);
    }

    /** Default key-group identity: the scope segment of {@code <policyId>:<scope>:<rawKey>}. */
    private static String defaultKeyGroup(String storageKey) {
        int first = storageKey.indexOf(':');
        int second = first < 0 ? -1 : storageKey.indexOf(':', first + 1);
        return second < 0 ? UNKNOWN_GROUP : storageKey.substring(first + 1, second);
    }

    /** Caller bugs (invalid input) propagate; only infrastructure failures degrade. */
    private static boolean isCallerError(Throwable error) {
        return error instanceof IllegalArgumentException || error instanceof NullPointerException;
    }

    private static Throwable unwrap(Throwable error) {
        if ((error instanceof CompletionException || error instanceof ExecutionException)
                && error.getCause() != null) {
            return error.getCause();
        }
        return error;
    }
}
