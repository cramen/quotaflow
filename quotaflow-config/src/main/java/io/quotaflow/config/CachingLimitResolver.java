package io.quotaflow.config;

import io.quotaflow.core.Limit;
import io.quotaflow.core.AsyncLimitResolver;
import io.quotaflow.core.execution.BoundedExecution;
import io.quotaflow.core.execution.DeadlineScheduler;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * <p>Versioned providers retain their complete immutable snapshot capability and
 * are not flattened into per-entry TTL caches. Their snapshot retrieval must be
 * a fast in-memory operation backed by externally refreshed immutable data.
 *
 * <p>The key group — never the raw key — is the cache and resolver identity,
 * keeping cardinality bounded by construction. {@link #clear()} invalidates
 * everything; wire it into {@link ConfigReloader.Builder#onApplied(Runnable)}
 * so configuration reloads drop cached resolutions along with the old policy
 * set.
 */
public class CachingLimitResolver implements AsyncLimitResolver {

    /** Default resolution cache TTL. */
    public static final Duration DEFAULT_TTL = Duration.ofSeconds(60);

    private final LimitResolver delegate;
    private final long ttlNanos;
    private final long lookupNanos;
    private final LongSupplier nanoClock;
    private final BoundedExecution execution;
    private final AtomicReference<ConcurrentHashMap<CacheKey, Entry>> generation =
            new AtomicReference<>(new ConcurrentHashMap<>());

    private record CacheKey(String limitRef, String keyGroup) { }
    private static final class Entry {
        final CompletableFuture<Optional<Limit>> result = new CompletableFuture<>();
        volatile boolean finished;
        volatile long completedAt;
        volatile boolean expired;
    }

    public static CachingLimitResolver wrap(LimitResolver delegate) { return wrap(delegate, DEFAULT_TTL); }
    public static CachingLimitResolver wrap(LimitResolver delegate, Duration ttl) {
        return wrap(delegate, ttl, Duration.ofSeconds(1), BoundedExecution.shared());
    }
    /** Lookup timeout includes bounded execution admission; TTL starts at successful completion. */
    public static CachingLimitResolver wrap(LimitResolver delegate, Duration ttl, Duration lookupTimeout,
            BoundedExecution execution) {
        if (delegate instanceof io.quotaflow.core.VersionedLimitResolver versioned)
            return new SnapshotPreservingResolver(versioned, ttl, lookupTimeout, execution);
        return new CachingLimitResolver(delegate, ttl, lookupTimeout, execution, System::nanoTime);
    }
    CachingLimitResolver(LimitResolver delegate, Duration ttl, LongSupplier nanoClock) {
        this(delegate, ttl, Duration.ofSeconds(1), BoundedExecution.shared(), nanoClock);
    }
    CachingLimitResolver(LimitResolver delegate, Duration ttl, Duration lookupTimeout,
            BoundedExecution execution, LongSupplier nanoClock) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.ttlNanos = positive(ttl, "ttl");
        this.lookupNanos = positive(lookupTimeout, "lookupTimeout");
        this.execution = Objects.requireNonNull(execution, "execution");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }
    private static long positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) throw new IllegalArgumentException(name + " must be positive");
        return value.toNanos();
    }

    @Override public Optional<Limit> resolve(String reference, String group) {
        try { return resolveAsync(reference, group).toCompletableFuture().get(lookupNanos, TimeUnit.NANOSECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); return Optional.empty(); }
        catch (TimeoutException e) { return Optional.empty(); }
        catch (ExecutionException e) { throw new CompletionException(e.getCause()); }
    }
    @Override public CompletionStage<Optional<Limit>> resolveAsync(String reference, String group) {
        var key = new CacheKey(Objects.requireNonNull(reference, "limitRef"), Objects.requireNonNull(group, "keyGroup"));
        var entries = generation.get();
        while (true) {
            var existing = entries.get(key);
            if (existing != null) {
                if (!existing.finished || nanoClock.getAsLong() - existing.completedAt < ttlNanos)
                    return existing.result.copy();
                entries.remove(key, existing);
                continue;
            }
            var entry = new Entry();
            if (entries.putIfAbsent(key, entry) != null) continue;
            long started = nanoClock.getAsLong();
            var timeout = DeadlineScheduler.schedule(() -> {
                synchronized (entry) {
                    if (entry.finished || entry.result.isDone()) return;
                    entry.expired = true;
                }
                ForkJoinPool.commonPool().execute(() -> entry.result.complete(Optional.empty()));
            }, lookupNanos);
            execution.submitStage(() -> delegate instanceof AsyncLimitResolver async
                    ? async.resolveAsync(reference, group)
                    : CompletableFuture.completedFuture(delegate.resolve(reference, group)),
                    () -> !entry.expired && !entry.result.isDone()).whenComplete((value, failure) -> {
                boolean expired;
                synchronized (entry) {
                    expired = entry.expired || nanoClock.getAsLong() - started >= lookupNanos;
                    entry.expired = expired;
                    if (!expired && failure == null && value != null) {
                        entry.completedAt = nanoClock.getAsLong(); entry.finished = true;
                    }
                }
                timeout.cancel(false);
                if (expired || failure != null || value == null) entries.remove(key, entry);
                if (expired || failure instanceof RejectedExecutionException || failure instanceof CancellationException)
                    entry.result.complete(Optional.empty());
                else if (failure != null) entry.result.completeExceptionally(failure);
                else if (value == null) entry.result.completeExceptionally(new NullPointerException("resolver result"));
                else entry.result.complete(value);
            });
            return entry.result.copy();
        }
    }

    /** Publishes a fresh generation; old physical work cannot populate it. */
    public void clear() { generation.set(new ConcurrentHashMap<>()); }
    public int cacheSize() { return generation.get().size(); }
    /** Versioned providers already publish immutable in-memory views; per-entry TTLs would mix revisions. */
    private static final class SnapshotPreservingResolver extends CachingLimitResolver implements io.quotaflow.core.VersionedLimitResolver {
        private final io.quotaflow.core.VersionedLimitResolver snapshots;
        SnapshotPreservingResolver(io.quotaflow.core.VersionedLimitResolver delegate, Duration ttl, Duration timeout, BoundedExecution execution) {
            super(delegate, ttl, timeout, execution, System::nanoTime); snapshots = delegate;
        }
        @Override public Optional<io.quotaflow.core.LimitSnapshot> snapshot() {
            return Objects.requireNonNull(snapshots.snapshot(), "snapshot result");
        }
        @Override public CompletionStage<Optional<Limit>> resolveAsync(String reference, String group) {
            Objects.requireNonNull(reference, "limitRef"); Objects.requireNonNull(group, "keyGroup");
            var owner = (CachingLimitResolver) this;
            var view = new CompletableFuture<Optional<Limit>>();
            var claimed = new AtomicBoolean();
            long started = owner.nanoClock.getAsLong();
            var timeout = DeadlineScheduler.schedule(() -> {
                if (claimed.compareAndSet(false, true)) ForkJoinPool.commonPool().execute(() -> view.complete(Optional.empty()));
            }, owner.lookupNanos);
            owner.execution.submit(() -> snapshot().flatMap(value -> value.resolve(reference, group)), () -> !claimed.get())
                    .whenComplete((value, failure) -> {
                        timeout.cancel(false);
                        if (!claimed.compareAndSet(false, true)) return;
                        if (owner.nanoClock.getAsLong() - started >= owner.lookupNanos) { view.complete(Optional.empty()); return; }
                        if (failure instanceof RejectedExecutionException || failure instanceof CancellationException) view.complete(Optional.empty());
                        else if (failure != null) view.completeExceptionally(failure);
                        else view.complete(value);
                    });
            return view.copy();
        }
        @Override public Optional<Limit> resolve(String reference, String group) {
            return super.resolve(reference, group);
        }
    }
}
