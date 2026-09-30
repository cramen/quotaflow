package io.quotaflow.fallback;

import io.quotaflow.core.store.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;

/** Bounded shared metadata with explicit attempt leases and epoch pins. No consumed entry is evicted under pressure. */
final class RecoveryTracking {
    static final class Entry {
        final BucketIdentity key;
        final AtomicInteger references = new AtomicInteger(1);
        record Metadata(LevelRequest request, long revision, long observedAt) { }
        final AtomicReference<Metadata> metadata;
        final AtomicLong lastUse;
        final AtomicLong horizon;
        Entry(LevelRequest request, long now, long revision) {
            key = request.storageKey(); metadata = new AtomicReference<>(new Metadata(request, revision, now));
            lastUse = new AtomicLong(now); horizon = new AtomicLong(horizon(request));
        }
        private static long horizon(LevelRequest request) {
            return request.limit().capacity() * request.limit().emissionIntervalNanos();
        }
        void observe(LevelRequest request, long now, long revision) {
            metadata.updateAndGet(previous -> revision > previous.revision()
                    || (revision == previous.revision() && (request.resolverRevision() > previous.request().resolverRevision()
                        || (request.resolverRevision() == previous.request().resolverRevision() && now - previous.observedAt() >= 0)))
                    ? new Metadata(request, revision, now) : previous);
            lastUse.accumulateAndGet(now, (old, value) -> value - old > 0 ? value : old);
            horizon.accumulateAndGet(horizon(request), Math::max);
        }
        boolean expired(long now) { return expired(now, 0); }
        boolean expired(long now, long grace) {
            return references.get() == 1 && now - lastUse.get() >= horizon.get() + grace;
        }
        boolean retain() {
            while (true) {
                int previous = references.get();
                if (previous < 0 || previous == Integer.MAX_VALUE) return false;
                if (references.compareAndSet(previous, previous + 1)) return true;
            }
        }
    }
    private final ConcurrentHashMap<BucketIdentity, Entry> entries = new ConcurrentHashMap<>();
    private final AtomicInteger occupied = new AtomicInteger();
    private final int maximum;
    RecoveryTracking(int maximum) {
        if (maximum < 1) throw new IllegalArgumentException("tracking bound must be positive");
        this.maximum = maximum;
    }
    List<Entry> retain(List<LevelRequest> requests, long now) { return retain(requests, now, 0); }
    List<Entry> retain(List<LevelRequest> requests, long now, long revision) {
        List<Entry> result = new ArrayList<>(requests.size());
        for (LevelRequest request : requests) {
            Entry entry = retain(request, now, revision);
            if (entry == null) { result.forEach(this::release); return null; }
            result.add(entry);
        }
        return result;
    }
    private Entry retain(LevelRequest request, long now, long revision) {
        while (true) {
            Entry old = entries.get(request.storageKey());
            if (old != null && old.retain()) {
                old.observe(request, now, revision);
                return old;
            }
            if (old != null) { entries.remove(old.key, old); continue; }
            int used = occupied.get();
            if (used >= maximum) return null;
            if (!occupied.compareAndSet(used, used + 1)) continue;
            Entry created = new Entry(request, now, revision);
            if (entries.putIfAbsent(created.key, created) == null) return created;
            occupied.decrementAndGet();
        }
    }
    void release(Entry entry) {
        int remaining = entry.references.decrementAndGet();
        if (remaining < 0) throw new IllegalStateException("tracking lease released twice");
        if (remaining == 0 && entry.references.compareAndSet(0, -1)) {
            entries.remove(entry.key, entry);
            occupied.decrementAndGet();
        }
    }
    int size() { return occupied.get(); }
}
