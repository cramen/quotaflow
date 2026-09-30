package io.quotaflow.core.store;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Exact local credit accounting with atomic publication of an entire quota domain.
 * Immutable balanced trees share unchanged branches; a request copies only paths
 * to its affected buckets. Single requests, chains and expiry use the same CAS
 * protocol. A stalled caller holds no admission lock and exposes no partial debit.
 */
public final class LocalRateLimitStore implements BatchRateLimitStore {
    private static final Runnable NO_OBSERVER = () -> { };

    private static final class Cell {
        final long tokens, remainder, timestamp;
        final Limit limit;
        final Algorithm algorithm;
        Cell(long tokens, long remainder, long timestamp, Limit limit, Algorithm algorithm) {
            this.tokens = tokens;
            this.remainder = remainder;
            this.timestamp = timestamp;
            this.limit = limit;
            this.algorithm = algorithm;
        }
    }

    /** A distinct wrapper prevents ABA even when a tree becomes empty again. */
    private record Version(Node root, long admissionGeneration, RecoveryPending pending, long fenceGeneration) {
        Version(Node root) { this(root, 0, null, -1); }
    }

    public enum InitialCredit { FULL, EMPTY }

    private static final class Node {
        final BucketIdentity key;
        final Cell cell;
        final Node left, right;
        final int height, size;
        Node(BucketIdentity key, Cell cell, Node left, Node right) {
            this.key = key;
            this.cell = cell;
            this.left = left;
            this.right = right;
            this.height = 1 + Math.max(height(left), height(right));
            this.size = 1 + size(left) + size(right);
        }
    }

    // Never detach these references: their count is bounded by binding history per namespace.
    private final ConcurrentHashMap<QuotaDomain, AtomicReference<Version>> domains = new ConcurrentHashMap<>();
    private final LongSupplier nanoClock;
    private final LocalPolicyBindings bindings;
    private final Runnable beforeCommit;
    private final InitialCredit initialCredit;

    public LocalRateLimitStore() { this(System::nanoTime); }
    public LocalRateLimitStore(LongSupplier nanoClock) {
        this(nanoClock, LocalPolicyBindings.DEFAULT_MAX_REGISTERED_POLICIES);
    }
    public LocalRateLimitStore(LongSupplier nanoClock, int maxRegisteredPolicies) {
        this(nanoClock, maxRegisteredPolicies, NO_OBSERVER);
    }

    /** Test seam for controlled preemption after preparation and before publication. */
    LocalRateLimitStore(LongSupplier nanoClock, int maxRegisteredPolicies, Runnable beforeCommit) {
        this(nanoClock, maxRegisteredPolicies, InitialCredit.FULL, beforeCommit);
    }

    public LocalRateLimitStore(LongSupplier nanoClock, int maxRegisteredPolicies, InitialCredit initialCredit) {
        this(nanoClock, maxRegisteredPolicies, initialCredit, NO_OBSERVER);
    }

    LocalRateLimitStore(LongSupplier nanoClock, int maxRegisteredPolicies, InitialCredit initialCredit, Runnable beforeCommit) {
        this.initialCredit = Objects.requireNonNull(initialCredit, "initialCredit");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.bindings = new LocalPolicyBindings(maxRegisteredPolicies);
        this.beforeCommit = Objects.requireNonNull(beforeCommit, "beforeCommit");
    }

    @Override
    public CompletionStage<Void> registerPolicies(List<PolicyBinding> candidate) {
        bindings.register(candidate);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public StoreResult tryAcquire(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) {
        LevelRequest request = new LevelRequest(key, limit, algorithm, weight);
        bindings.register(key, algorithm);
        ChainResult result = acquire(List.of(request));
        return new StoreResult(result.acquired(), result.remaining(), result.retryAfterMillis(), result.recoveryPending());
    }

    @Override
    public CompletionStage<StoreResult> tryAcquireAsync(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) {
        return CompletableFuture.completedFuture(tryAcquire(key, limit, algorithm, weight));
    }

    @Override
    public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
        List<LevelRequest> requests = List.copyOf(chain);
        LevelRequest.validateChain(requests);
        bindings.register(requests.stream().map(level -> PolicyBinding.of(level.storageKey(), level.algorithm())).toList());
        return CompletableFuture.completedFuture(acquire(requests));
    }

    private ChainResult acquire(List<LevelRequest> chain) {
        AtomicReference<Version> domain = domains.computeIfAbsent(chain.get(0).storageKey().domain(),
                ignored -> new AtomicReference<>(new Version(null)));
        long admittedGeneration = domain.get().admissionGeneration;
        while (true) {
            Version previous = domain.get();
            if (previous.pending != null) return ChainResult.pending(0, previous.pending);
            if (previous.admissionGeneration != admittedGeneration) {
                return ChainResult.pending(0, new RecoveryPending(chain.get(0).storageKey().domain(),
                        previous.admissionGeneration, CompletableFuture.completedFuture(null)));
            }
            long now = nanoClock.getAsLong();
            Cell[] states = new Cell[chain.size()];
            int rejected = -1;
            int impossible = -1;
            for (int i = 0; i < chain.size(); i++) {
                LevelRequest level = chain.get(i);
                Cell state = normalize(find(previous.root, level.storageKey()), level.limit(), level.algorithm(), now);
                states[i] = state;
                if (level.weight() > level.limit().capacity() && impossible < 0) impossible = i;
                if (state.tokens < level.weight() && rejected < 0) rejected = i;
            }
            int fired = impossible >= 0 ? impossible : rejected;
            Node next = previous.root;
            long remaining = Long.MAX_VALUE;
            for (int i = 0; i < chain.size(); i++) {
                LevelRequest level = chain.get(i);
                Cell state = states[i];
                if (fired < 0) state = new Cell(state.tokens - level.weight(), state.remainder,
                        state.timestamp, state.limit, state.algorithm);
                // A rejection must not allocate untouched full buckets for arbitrary new raw keys.
                if (fired < 0 || state.tokens < state.limit.capacity() || find(previous.root, level.storageKey()) != null) {
                    next = put(next, level.storageKey(), state);
                }
                remaining = Math.min(remaining, state.tokens);
            }
            beforeCommit.run();
            if (!domain.compareAndSet(previous, new Version(next, previous.admissionGeneration, previous.pending, previous.fenceGeneration))) continue;
            if (fired < 0) return ChainResult.acquired(chain.size() - 1, remaining);
            Cell state = states[fired];
            long retry = impossible >= 0 ? 0 : millisCeil(
                    (chain.get(fired).weight() - state.tokens) * state.limit.emissionIntervalNanos() - state.remainder);
            return ChainResult.rejected(fired, state.tokens, retry);
        }
    }

    /**
     * Weak snapshot of immutable domain versions. Cleanup is conditional on the
     * observed version, so a concurrent debit cannot be erased by stale expiry.
     * Authoritative recovery still requires an external admission fence.
     */
    public List<BucketState> snapshot() {
        long now = nanoClock.getAsLong();
        List<BucketState> result = new ArrayList<>();
        for (AtomicReference<Version> domain : domains.values()) {
            Version previous = domain.get();
            if (previous.pending != null) {
                collectAll(previous.root, now, result);
                continue;
            }
            List<BucketIdentity> expired = new ArrayList<>();
            collect(previous.root, now, result, expired);
            if (!expired.isEmpty()) {
                Node next = previous.root;
                for (BucketIdentity key : expired) next = remove(next, key);
                beforeCommit.run();
                domain.compareAndSet(previous, new Version(next, previous.admissionGeneration, previous.pending, previous.fenceGeneration));
            }
        }
        return result;
    }

    private void collect(Node node, long now, List<BucketState> active, List<BucketIdentity> expired) {
        if (node == null) return;
        collect(node.left, now, active, expired);
        Cell state = normalize(node.cell, node.cell.limit, node.cell.algorithm, now);
        if (state.tokens == state.limit.capacity()) expired.add(node.key);
        else active.add(new BucketState(node.key, state.limit, state.algorithm, state.tokens));
        collect(node.right, now, active, expired);
    }

    public InitialCredit initialCredit() { return initialCredit; }

    /** Stops future local commits. Older or already-resumed fence generations cannot be installed again. */
    public void fence(RecoveryPending pending) {
        Objects.requireNonNull(pending, "pending");
        if (!bindings.containsDomain(pending.domain())) {
            throw new io.quotaflow.core.PolicyConfigurationException("quota domain has no registered policies");
        }
        AtomicReference<Version> domain = domains.computeIfAbsent(pending.domain(),
                ignored -> new AtomicReference<>(new Version(null)));
        while (true) {
            Version previous = domain.get();
            if (previous.pending == pending || pending.generation() <= previous.fenceGeneration) return;
            Version next = new Version(previous.root, advance(previous.admissionGeneration), pending, pending.generation());
            if (domain.compareAndSet(previous, next)) return;
        }
    }

    /** Only the holder of the current fence can resume admission. */
    public boolean resume(RecoveryPending pending) {
        AtomicReference<Version> domain = domains.get(pending.domain());
        if (domain == null) return false;
        while (true) {
            Version previous = domain.get();
            if (previous.pending != pending) return false;
            Version next = new Version(previous.root, advance(previous.admissionGeneration), null, previous.fenceGeneration);
            if (domain.compareAndSet(previous, next)) return true;
        }
    }

    /** Retires a guard only under the exact current fence; callers first establish authoritative recovery. */
    public boolean clearFenced(RecoveryPending pending) {
        AtomicReference<Version> domain = domains.get(pending.domain());
        if (domain == null) return false;
        while (true) {
            Version previous = domain.get();
            if (previous.pending != pending) return false;
            if (domain.compareAndSet(previous, new Version(null, previous.admissionGeneration, pending, previous.fenceGeneration))) return true;
        }
    }

    /** Includes full cells; the recovery owner decides which accounting records are safe to retire. */
    public List<BucketState> snapshotFenced(RecoveryPending pending) {
        AtomicReference<Version> domain = domains.get(pending.domain());
        if (domain == null) throw new IllegalStateException("recovery fence is not current");
        Version version = domain.get();
        if (version.pending != pending) throw new IllegalStateException("recovery fence is not current");
        List<BucketState> result = new ArrayList<>();
        collectAll(version.root, nanoClock.getAsLong(), result);
        if (domain.get().pending != pending) throw new IllegalStateException("recovery fence retired during snapshot");
        return List.copyOf(result);
    }

    /** Changes schedules while admissions remain fenced, preserving only known whole credit on a transition. */
    public boolean constrainFenced(RecoveryPending pending, List<GuardConstraint> constraints) {
        List<GuardConstraint> copy = List.copyOf(constraints);
        java.util.Set<BucketIdentity> seen = new java.util.HashSet<>();
        for (GuardConstraint c : copy) {
            if (!c.key().domain().equals(pending.domain()) || !seen.add(c.key()))
                throw new IllegalArgumentException("constraint domain mismatch or duplicate bucket");
        }
        bindings.register(copy.stream().map(c -> PolicyBinding.of(c.key(), c.algorithm())).toList());
        AtomicReference<Version> domain = domains.get(pending.domain());
        if (domain == null) return false;
        while (true) {
            Version previous = domain.get();
            if (previous.pending != pending) return false;
            long now = nanoClock.getAsLong();
            Node next = previous.root;
            for (GuardConstraint constraint : copy) {
                Cell cell = find(next, constraint.key());
                if (cell == null) continue;
                if (constraint.effectiveLimit() == null) {
                    next = remove(next, constraint.key());
                } else {
                    Limit limit = constraint.effectiveLimit();
                    Cell normalized = constraint.resetSchedule()
                            ? new Cell(Math.min(cell.tokens, limit.capacity()), 0,
                                now - cell.timestamp < 0 ? cell.timestamp : now, limit, constraint.algorithm())
                            : normalize(cell, limit, constraint.algorithm(), now);
                    next = put(next, constraint.key(), normalized);
                }
            }
            beforeCommit.run();
            if (domain.compareAndSet(previous, new Version(next, previous.admissionGeneration, pending, previous.fenceGeneration))) return true;
        }
    }

    /** Non-mutating weak observation, useful for a conservative retry hint after guarded dispatch. */
    public java.util.Optional<BucketState> inspect(BucketIdentity key) {
        AtomicReference<Version> domain = domains.get(key.domain());
        Cell cell = domain == null ? null : find(domain.get().root, key);
        if (cell == null) return java.util.Optional.empty();
        Cell state = normalize(cell, cell.limit, cell.algorithm, nanoClock.getAsLong());
        return java.util.Optional.of(new BucketState(key, state.limit, state.algorithm, state.tokens));
    }

    private void collectAll(Node node, long now, List<BucketState> result) {
        if (node == null) return;
        collectAll(node.left, now, result);
        Cell state = normalize(node.cell, node.cell.limit, node.cell.algorithm, now);
        result.add(new BucketState(node.key, state.limit, state.algorithm, state.tokens));
        collectAll(node.right, now, result);
    }

    private static long advance(long generation) {
        if (generation == Long.MAX_VALUE) throw new IllegalStateException("local admission generation exhausted");
        return generation + 1;
    }

    public int cellCount() {
        int count = 0;
        for (AtomicReference<Version> domain : domains.values()) count += size(domain.get().root);
        return count;
    }

    private static int compare(BucketIdentity a, BucketIdentity b) {
        int policy = a.policyId().compareTo(b.policyId());
        if (policy != 0) return policy;
        int scope = a.scope().compareTo(b.scope());
        return scope != 0 ? scope : a.rawKey().compareTo(b.rawKey());
    }

    private static Cell find(Node node, BucketIdentity key) {
        while (node != null) {
            int order = compare(key, node.key);
            if (order == 0) return node.cell;
            node = order < 0 ? node.left : node.right;
        }
        return null;
    }

    private static Node put(Node node, BucketIdentity key, Cell cell) {
        if (node == null) return new Node(key, cell, null, null);
        int order = compare(key, node.key);
        if (order == 0) return new Node(key, cell, node.left, node.right);
        return balance(order < 0 ? new Node(node.key, node.cell, put(node.left, key, cell), node.right)
                : new Node(node.key, node.cell, node.left, put(node.right, key, cell)));
    }

    /** The key is present in this immutable tree (collected from the same version). */
    private static Node remove(Node node, BucketIdentity key) {
        int order = compare(key, node.key);
        if (order < 0) return balance(new Node(node.key, node.cell, remove(node.left, key), node.right));
        if (order > 0) return balance(new Node(node.key, node.cell, node.left, remove(node.right, key)));
        if (node.left == null) return node.right;
        if (node.right == null) return node.left;
        Node successor = node.right;
        while (successor.left != null) successor = successor.left;
        return balance(new Node(successor.key, successor.cell, node.left, remove(node.right, successor.key)));
    }

    private static int height(Node node) { return node == null ? 0 : node.height; }
    private static int size(Node node) { return node == null ? 0 : node.size; }

    private static Node balance(Node node) {
        if (height(node.left) - height(node.right) > 1) {
            if (height(node.left.left) < height(node.left.right)) {
                node = new Node(node.key, node.cell, rotateLeft(node.left), node.right);
            }
            return rotateRight(node);
        }
        if (height(node.right) - height(node.left) > 1) {
            if (height(node.right.right) < height(node.right.left)) {
                node = new Node(node.key, node.cell, node.left, rotateRight(node.right));
            }
            return rotateLeft(node);
        }
        return node;
    }

    private static Node rotateLeft(Node node) {
        Node pivot = node.right;
        return new Node(pivot.key, pivot.cell, new Node(node.key, node.cell, node.left, pivot.left), pivot.right);
    }

    private static Node rotateRight(Node node) {
        Node pivot = node.left;
        return new Node(pivot.key, pivot.cell, pivot.left, new Node(node.key, node.cell, pivot.right, node.right));
    }
    private Cell normalize(Cell state, Limit limit, Algorithm algorithm, long now) {
        if (state == null) return new Cell(initialCredit == InitialCredit.FULL ? limit.capacity() : 0, 0, now, limit, algorithm);
        // Signed subtraction supports nanoTime wrap for elapsed durations below 2^63 ns.
        long elapsed = now - state.timestamp;
        long timestamp = elapsed < 0 ? state.timestamp : now;
        if (state.limit.capacity() != limit.capacity()
                || state.limit.emissionIntervalNanos() != limit.emissionIntervalNanos()) {
            return new Cell(Math.min(state.tokens, limit.capacity()), 0, timestamp, limit, algorithm);
        }
        elapsed = Math.max(0, Math.min(elapsed, Limit.MAX_HORIZON_NANOS));
        long interval = limit.emissionIntervalNanos();
        long progress = state.remainder + elapsed;
        long tokens = Math.min(limit.capacity(), state.tokens + progress / interval);
        return new Cell(tokens, tokens == limit.capacity() ? 0 : progress % interval, timestamp, limit, algorithm);
    }

    private static long millisCeil(long nanos) {
        return nanos / 1_000_000 + (nanos % 1_000_000 == 0 ? 0 : 1);
    }
}
