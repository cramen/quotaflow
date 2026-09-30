package io.quotaflow.fallback;

import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fixed-cohort conservative routing. Every recovery epoch owns its own cold guard and retained metadata. */
public class CoordinatedFallbackStore implements BatchRateLimitStore, RecoveryConfigurationAware, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(CoordinatedFallbackStore.class);
    private enum Mode { UNENROLLED, LOCAL, GUARDED, FENCED, READY, NORMAL, INCOMPATIBLE }
    private final class Configuration {
        final RecoveryPolicySource source; final long revision; final Set<String> policies;
        final AtomicInteger owners = new AtomicInteger(); final AtomicBoolean released = new AtomicBoolean();
        volatile boolean retired;
        Configuration(RecoveryPolicySource source, long revision, PolicySet policies) {
            this.source = source; this.revision = revision;
            this.policies = policies.policies().stream().map(policy -> policy.id()).collect(java.util.stream.Collectors.toUnmodifiableSet());
        }
        void release() { owners.decrementAndGet(); retireIfDrained(); }
        void retireIfDrained() {
            if (retired && owners.get() == 0 && released.compareAndSet(false, true)) {
                long retiredRevision = revision;
                notifyListeners(listener -> listener.onRetired(retiredRevision)); configurations.remove(this);
                if (closed.get() && configurations.isEmpty()) callbacks.forEach(io.quotaflow.core.execution.CallbackDispatcher::close);
            }
        }
    }
    private record Backoff(long started, long duration) { }
    private record ResolverIdentity(long revision, String fingerprint) { }
    private record AppliedTarget(RecoveryPending fence, RecoveryPolicySource.Target target) { }
    private static final ResolverIdentity NO_RESOLVER = new ResolverIdentity(0, LimitSnapshot.NONE_FINGERPRINT);
    private final class Ledger {
        final long bornEpoch;
        final LocalRateLimitStore local = new LocalRateLimitStore(clock, 4096, LocalRateLimitStore.InitialCredit.EMPTY);
        final ConcurrentHashMap<RecoveryTracking.Entry, Boolean> pinned = new ConcurrentHashMap<>();
        final AtomicInteger entrants = new AtomicInteger();
        final AtomicBoolean retired = new AtomicBoolean();
        final AtomicReference<AppliedTarget> applied;
        Ledger(long bornEpoch, List<PolicyBinding> bindings, RecoveryPolicySource.Target target) {
            this.bornEpoch = bornEpoch; applied = new AtomicReference<>(new AppliedTarget(null, target));
            local.registerPolicies(bindings).toCompletableFuture().join();
        }
        void fence(RecoveryPending pending) {
            local.fence(pending);
            applied.updateAndGet(previous -> previous.fence() == null || pending.generation() > previous.fence().generation()
                    ? new AppliedTarget(pending, previous.target()) : previous);
        }
        boolean normalizedFor(RecoveryPolicySource.Target target) { return target != null && target.sameAs(applied.get().target()); }
        void pin(List<RecoveryTracking.Entry> entries) {
            for (RecoveryTracking.Entry entry : entries) {
                if (retired.get()) return;
                if (entry.retain()) {
                    if (pinned.putIfAbsent(entry, Boolean.TRUE) != null) tracking.release(entry);
                    else if (retired.get() && pinned.remove(entry) != null) tracking.release(entry);
                }
            }
        }
        void retire(RecoveryPending fence) {
            if (!retired.compareAndSet(false, true)) return;
            if (fence != null) local.clearFenced(fence);
            pinned.keySet().forEach(entry -> { if (pinned.remove(entry) != null) tracking.release(entry); });
        }
    }
    private final class Route {
        final Mode mode;
        final Ledger ledger;
        final RecoveryContext context;
        final long revision;
        final RecoveryPending pending;
        final CompletableFuture<Void> signal = new CompletableFuture<>();
        final boolean readySent;
        final RecoveryPolicySource.Target target;
        Route(QuotaDomain domain, Mode mode, Ledger ledger, RecoveryContext context, long revision, boolean readySent, RecoveryPolicySource.Target target) {
            this.mode = mode; this.ledger = ledger; this.context = context; this.revision = revision; this.readySent = readySent; this.target = target;
            long generation = generations.incrementAndGet();
            if (generation < 0) throw new IllegalStateException("local recovery generation exhausted");
            pending = new RecoveryPending(domain, generation, signal);
            if (mode == Mode.NORMAL || mode == Mode.LOCAL || mode == Mode.GUARDED) signal.complete(null);
        }
    }
    private final class Domain {
        final QuotaDomain identity;
        final AtomicReference<Route> route;
        final AtomicReference<Object> control = new AtomicReference<>();
        volatile Backoff backoff = new Backoff(0, 0);
        volatile RuntimeException incompatible;
        volatile RecoveryContext abortRequested, observedContext;
        volatile long authorizedRevision = -1;
        volatile RecoveryConfiguration blockedTarget;
        volatile long seenEpoch, seenDispatch, seenConfiguration, seenRetired;
        volatile ResolverIdentity localResolver = NO_RESOLVER, seenResolver = NO_RESOLVER;
        Domain(QuotaDomain identity, long revision) {
            this.identity = identity;
            route = new AtomicReference<>(new Route(identity, Mode.UNENROLLED, null, null, revision, false, null));
        }
    }
    private final class Attempt {
        final Domain domain; final Route route; final RecoveryContext context;
        final List<RecoveryTracking.Entry> entries;
        final AtomicBoolean released = new AtomicBoolean(), terminal = new AtomicBoolean();
        final List<LevelRequest> requests;
        final Configuration cfg;
        final CompletableFuture<ChainResult> result = new CompletableFuture<>();
        Attempt(Domain domain, Route route, List<RecoveryTracking.Entry> entries, List<LevelRequest> requests, Configuration cfg) {
            this.cfg = cfg;
            this.domain = domain; this.route = route; this.context = route.context; this.entries = entries; this.requests = requests;
        }
        void finish(ChainResult value, boolean degraded) {
            if (terminal.compareAndSet(false, true)) result.complete(degraded ? fallback(cfg, requests, value) : value);
        }
        void fail(Throwable failure) {
            if (terminal.compareAndSet(false, true)) result.completeExceptionally(failure);
        }
        void release() {
            if (released.compareAndSet(false, true)) {
                attempts.remove(this); entries.forEach(tracking::release); inFlight.release();
            }
        }
    }

    private final RecoveryPrimary primary;
    private final RecoverySettings settings;
    private final List<DegradationListener> listeners;
    private final List<io.quotaflow.core.execution.CallbackDispatcher> callbacks;
    private final java.util.concurrent.locks.ReentrantLock observationLock = new java.util.concurrent.locks.ReentrantLock();
    private final Set<Configuration> configurations = ConcurrentHashMap.newKeySet();
    private final LongSupplier clock;
    private final RecoveryTracking tracking;
    private final LocalPolicyBindings localBindings = new LocalPolicyBindings();
    private final Set<PolicyBinding> approved = ConcurrentHashMap.newKeySet();
    private final Set<PolicyBinding> history = ConcurrentHashMap.newKeySet();
    private volatile List<PolicyBinding> declared = List.of();
    private final ConcurrentHashMap<QuotaDomain, Domain> domains = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Attempt, Boolean> attempts = new ConcurrentHashMap<>();
    private final AtomicReference<Configuration> configuration = new AtomicReference<>();
    private final AtomicReference<RecoverySession> session = new AtomicReference<>();
    private final AtomicBoolean enrolling = new AtomicBoolean(), closed = new AtomicBoolean();
    private final AtomicBoolean trackingPressureReported = new AtomicBoolean(), dispatchPressureReported = new AtomicBoolean();
    private final AtomicLong generations = new AtomicLong(), revisions = new AtomicLong();
    private final AtomicInteger cursor = new AtomicInteger();
    private final Semaphore inFlight, controlWork = new Semaphore(4);
    private final String nonce = UUID.randomUUID().toString();
    private final AtomicReference<ScheduledFuture<?>> ticker = new AtomicReference<>();
    private final AtomicInteger stateNotifications = new AtomicInteger();
    private final AtomicReference<DegradationState> observed = new AtomicReference<>(DegradationState.OPEN);
    private final Set<String> diagnostics = ConcurrentHashMap.newKeySet();
    private volatile RuntimeException enrollmentFailure;

    public CoordinatedFallbackStore(RecoveryPrimary primary, RecoverySettings settings, List<DegradationListener> listeners) {
        this(primary, settings, listeners, System::nanoTime);
    }
    public CoordinatedFallbackStore(RecoveryPrimary primary, RecoverySettings settings,
                                    List<DegradationListener> listeners, LongSupplier clock) {
        this.primary = Objects.requireNonNull(primary, "primary"); this.settings = Objects.requireNonNull(settings, "settings");
        this.listeners = List.copyOf(listeners);
        this.callbacks = this.listeners.stream().map(listener -> new io.quotaflow.core.execution.CallbackDispatcher(
                1024, Duration.ofSeconds(1), listener::onClosed)).toList();
        this.clock = Objects.requireNonNull(clock, "clock");
        tracking = new RecoveryTracking(settings.maximumTrackedBuckets()); inFlight = new Semaphore(settings.maximumInFlight());
        notifyTransition(DegradationState.CLOSED, DegradationState.OPEN, "recovery ownership is not yet validated");
    }

    @Override public CompletionStage<Void> registerPolicies(List<PolicyBinding> candidate) {
        if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("recovery owner is closed"));
        List<PolicyBinding> bindings = List.copyOf(candidate);
        for (PolicyBinding binding : bindings) if (!binding.domain().namespace().equals(settings.namespace()))
            throw new PolicyConfigurationException("policy namespace differs from the startup accounting target");
        localBindings.register(bindings);
        history.addAll(bindings);
        boolean startup = configuration.get() == null;
        if (approved.containsAll(bindings)) { declared = bindings; return CompletableFuture.completedFuture(null); }
        return call(() -> primary.registerPolicies(bindings)).handle((ignored, failure) -> {
            if (failure == null) { approved.addAll(bindings); declared = bindings; return null; }
            Throwable cause = unwrap(failure);
            if (callerError(cause)) throw new CompletionException(cause);
            if (!startup) throw new PolicyConfigurationException("new policy identities cannot activate before authoritative validation");
            declared = bindings; // Provisional startup is allowed, but grants no unvalidated share.
            return null;
        });
    }

    @Override public void validateRecoveryConfiguration(PolicySet policies, String namespace, LimitResolver resolver) {
        if (!settings.namespace().equals(namespace)) throw new PolicyConfigurationException("accounting namespace is startup-only");
        new RecoveryPolicySource(policies, namespace, resolver).validateCapabilities();
    }

    @Override public CompletionStage<Void> configureRecovery(PolicySet policies, String namespace, LimitResolver resolver) {
        if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("recovery owner is closed"));
        if (!settings.namespace().equals(namespace)) throw new PolicyConfigurationException("accounting namespace is startup-only");
        validateRecoveryConfiguration(policies, namespace, resolver);
        RecoveryPolicySource source = new RecoveryPolicySource(policies, namespace, resolver);
        for (var policy : policies.policies()) policy.limit().ifPresent(limit -> {
            if (DegradedShare.of(limit, settings.cohort().size()).capacity() == 0 && diagnostics.add(policy.id()))
                log.warn("policy '{}' has no positive degraded share for the declared cohort", policy.id());
        });
        long revision;
        observationLock.lock();
        try {
            if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("recovery owner is closed"));
            revision = configuration.get() == null ? 0 : revisions.incrementAndGet();
            var next = new Configuration(source, revision, policies); configurations.add(next);
            var previous = configuration.getAndSet(next);
            long publishedRevision = next.revision; Set<String> publishedPolicies = next.policies;
            notifyListeners(listener -> listener.onConfiguration(publishedRevision, publishedPolicies));
            if (previous != null) { previous.retired = true; previous.retireIfDrained(); }
        } finally { observationLock.unlock(); }
        for (QuotaDomain identity : source.domains()) domains.computeIfAbsent(identity, key -> new Domain(key, revision));
        for (Domain domain : domains.values()) {
            while (true) {
                Route old = domain.route.get();
                if (old.revision == revision) break;
                Route next = new Route(domain.identity, Mode.FENCED, old.ledger, old.context, revision, old.readySent, old.target);
                if (domain.route.compareAndSet(old, next)) {
                    if (next.ledger != null) next.ledger.fence(next.pending);
                    domain.backoff = new Backoff(0, 0); old.signal.complete(null); break;
                }
            }
        }
        start(); updateState();
        return CompletableFuture.completedFuture(null);
    }

    private void start() {
        if (closed.get() || ticker.get() != null) return;
        ScheduledFuture<?> future = RecoveryWork.TIMER.scheduleWithFixedDelay(this::tick, 0,
                settings.controlInterval().toNanos(), TimeUnit.NANOSECONDS);
        if (!ticker.compareAndSet(null, future) || closed.get()) future.cancel(false);
    }
    private void tick() {
        if (closed.get() || configuration.get() == null || enrollmentFailure != null) return;
        if (session.get() == null) {
            if (!enrolling.compareAndSet(false, true)) return;
            List<PolicyBinding> enrollingBindings = declared;
            RecoveryWork.Budget budget = new RecoveryWork.Budget(settings.attemptTimeout(), () -> !closed.get());
            budget.call(primary::probe).thenCompose(ignored -> budget.call(() -> primary.registerPolicies(enrollingBindings)))
                    .thenCompose(ignored -> budget.call(() -> primary.enroll(settings.cohort(), settings.instanceId(), nonce)))
                    .whenComplete((owner, failure) -> {
                        if (!closed.get()) {
                            if (failure == null) {
                                if (owner == null || !owner.cohortDigest().equals(settings.cohort().digest())
                                        || !owner.instanceId().equals(settings.instanceId()) || owner.slot() != settings.cohort().slot(settings.instanceId()))
                                    enrollmentFailure = new StateCompatibilityException("primary returned incompatible session ownership");
                                else { approved.addAll(enrollingBindings); session.compareAndSet(null, owner); }
                            }
                            else if (callerError(unwrap(failure))) enrollmentFailure = asRuntime(unwrap(failure));
                        }
                        enrolling.set(false); updateState();
                    });
            return;
        }
        List<Domain> current = new ArrayList<>(domains.values());
        if (current.isEmpty()) return;
        int first = Math.floorMod(cursor.getAndAdd(4), current.size());
        for (int i = 0; i < Math.min(current.size(), 4); i++) control(current.get((first + i) % current.size()));
    }

    private void control(Domain domain) {
        if (closed.get() || domain.incompatible != null || !controlWork.tryAcquire()) return;
        Object ticket = new Object();
        if (!domain.control.compareAndSet(null, ticket)) { controlWork.release(); return; }
        RecoveryWork.Budget budget = new RecoveryWork.Budget(settings.attemptTimeout(), () -> !closed.get());
        expireIdle(domain);
        Backoff backoff = domain.backoff;
        if (backoff.duration != 0 && clock.getAsLong() - backoff.started < backoff.duration) {
            domain.control.compareAndSet(ticket, null); controlWork.release(); return;
        }
        Configuration cfg = configuration.get();
        AtomicReference<Route> owned = new AtomicReference<>(domain.route.get());
        AtomicBoolean targetReady = new AtomicBoolean();
        List<PolicyBinding> capturedBindings = declared;
        budget.call(() -> CompletableFuture.completedFuture(cfg.source.capture(domain.identity))).thenCompose(captured -> {
            if (!current(domain, owned.get(), cfg)) return CompletableFuture.completedFuture(null);
            if (captured.isEmpty()) { holdTarget(domain, owned, cfg); return CompletableFuture.completedFuture(null); }
            if (!acceptTarget(domain, captured.orElseThrow())) {
                domain.blockedTarget = captured.orElseThrow().configuration(cfg.revision);
                holdTarget(domain, owned, cfg); return CompletableFuture.completedFuture(null);
            }
            RecoveryPolicySource.Target target = captured.orElseThrow();
            return prepareLocalTarget(domain, owned, cfg, target, budget).thenCompose(ignored -> {
                if (!current(domain, owned.get(), cfg)) return CompletableFuture.completedFuture(null);
                targetReady.set(true);
                return budget.call(() -> primary.registerPolicies(capturedBindings)).thenCompose(registered -> {
                    approved.addAll(capturedBindings);
                    return owned.get().context == null ? budget.call(() -> primary.attach(domain.identity, session.get()))
                            : budget.call(() -> primary.read(domain.identity, session.get()));
                }).thenCompose(info -> {
                    if (!current(domain, owned.get(), cfg)) return CompletableFuture.completedFuture(null);
                    observe(domain, info); retireSuperseded(domain, info.context());
                    if (!acceptTarget(domain, target)) {
                        waitForConfiguration(domain, owned.get(), owned, info.context(), cfg, target);
                        return CompletableFuture.completedFuture(null);
                    }
                    RecoveryContext abort = domain.abortRequested;
                    if (abort != null && info.context().phase() == RecoveryPhase.DRAIN && abort.equals(info.context())) {
                        return budget.call(() -> primary.abort(abort)).thenCompose(result -> {
                            if (!current(domain, owned.get(), cfg)) return CompletableFuture.completedFuture(null);
                            observe(domain, result); domain.abortRequested = null;
                            retireSuperseded(domain, result.context());
                            return drive(domain, owned, cfg, target, result, budget);
                        });
                    }
                    domain.abortRequested = null;
                    return drive(domain, owned, cfg, target, info, budget);
                });
            });
        }).whenComplete((ignored, failure) -> {
            if (current(domain, owned.get(), cfg)) {
                if (failure == null) domain.backoff = new Backoff(0, 0);
                else if (!targetReady.get()) {
                    if (callerError(unwrap(failure))) incompatible(domain, asRuntime(unwrap(failure)));
                    else { holdTarget(domain, owned, cfg); backoff(domain); }
                } else controlFailed(domain, owned.get(), unwrap(failure));
            }
            domain.control.compareAndSet(ticket, null); controlWork.release(); updateState();
        });
    }

    private boolean acceptTarget(Domain domain, RecoveryPolicySource.Target target) {
        long revision = target.resolverRevision();
        if (revision == 0) return true;
        for (ResolverIdentity floor : List.of(domain.localResolver, domain.seenResolver)) {
            if (revision < floor.revision()) return false;
            if (revision == floor.revision() && !target.resolverFingerprint().equals(floor.fingerprint()))
                throw new StateCompatibilityException("resolver revision was reused with different content");
        }
        domain.localResolver = new ResolverIdentity(revision, target.resolverFingerprint());
        return true;
    }

    private void holdTarget(Domain domain, AtomicReference<Route> owned, Configuration cfg) {
        Route route = owned.get();
        if (!current(domain, route, cfg) || route.mode == Mode.FENCED) return;
        Route held = new Route(domain.identity, Mode.FENCED, route.ledger, route.context, route.revision, route.readySent, route.target);
        if (replace(domain, route, held)) {
            if (held.ledger != null) held.ledger.fence(held.pending);
            owned.set(held);
        }
    }

    /** Local adoption needs no Redis acknowledgment, but cannot resume a READY/DRAIN owner. */
    private CompletionStage<Void> prepareLocalTarget(Domain domain, AtomicReference<Route> owned, Configuration cfg,
                                                    RecoveryPolicySource.Target target, RecoveryWork.Budget budget) {
        Route route = owned.get();
        RecoveryConfiguration desired = target.configuration(cfg.revision);
        RecoveryContext observed = domain.observedContext;
        if (observed != null && !observed.equals(route.context)) {
            Route aligned = new Route(domain.identity, Mode.FENCED, route.ledger, observed, cfg.revision, route.readySent, route.target);
            if (!replace(domain, route, aligned)) return CompletableFuture.completedFuture(null);
            if (aligned.ledger != null) aligned.ledger.fence(aligned.pending);
            owned.set(aligned); route = aligned;
        }
        if (route.context != null && !target.fingerprint().equals(route.context.configurationFingerprint())
                && (domain.authorizedRevision < 0 || cfg.revision <= domain.authorizedRevision)) {
            domain.blockedTarget = desired; holdTarget(domain, owned, cfg); return CompletableFuture.completedFuture(null);
        }
        if (domain.blockedTarget != null) {
            RecoveryConfiguration blocked = domain.blockedTarget;
            boolean samePolicyProposal = blocked.localRevision() == desired.localRevision()
                    && blocked.policyFingerprint().equals(desired.policyFingerprint());
            boolean policyRejected = route.context != null && !blocked.policyFingerprint().equals(route.context.configurationFingerprint());
            if ((blocked.equals(desired) && (route.context == null || !desired.matches(route.context)))
                    || (samePolicyProposal && policyRejected)) return CompletableFuture.completedFuture(null);
            domain.blockedTarget = null;
        }
        if (route.ledger != null && domain.seenRetired >= route.ledger.bornEpoch) {
            Route retired = new Route(domain.identity, Mode.FENCED, null, route.context, cfg.revision, false, target);
            if (!replace(domain, route, retired)) return CompletableFuture.completedFuture(null);
            route.ledger.fence(route.pending); route.ledger.retire(route.pending); owned.set(retired); route = retired;
        }
        if (route.readySent || (route.context != null && route.context.phase() == RecoveryPhase.DRAIN))
            return CompletableFuture.completedFuture(null);
        if (target.sameAs(route.target) && route.mode != Mode.FENCED
                && (route.ledger == null || route.ledger.normalizedFor(target))) return CompletableFuture.completedFuture(null);
        Route fenced = new Route(domain.identity, Mode.FENCED, route.ledger, route.context, cfg.revision, false, target);
        if (!replace(domain, route, fenced)) return CompletableFuture.completedFuture(null);
        owned.set(fenced);
        if (fenced.ledger == null) return CompletableFuture.completedFuture(null);
        fenced.ledger.fence(fenced.pending);
        return drain(fenced.ledger, budget).thenCompose(ignored -> snapshot(domain, fenced, cfg, budget)).thenAccept(ignored -> {
            if (!current(domain, fenced, cfg)) return;
            fenced.ledger.local.resume(fenced.pending);
            Route local = new Route(domain.identity, Mode.LOCAL, fenced.ledger, fenced.context, cfg.revision, false, target);
            if (replace(domain, fenced, local)) owned.set(local);
        });
    }

    /** Reclaim only fully refilled, idle local state. A domain fence makes recreation cold. */
    private void expireIdle(Domain domain) {
        Route old = domain.route.get();
        if (old.mode != Mode.LOCAL || old.ledger == null) return;
        long now = clock.getAsLong();
        List<RecoveryTracking.Entry> expired = old.ledger.pinned.keySet().stream()
                .filter(entry -> entry.expired(now, settings.attemptTimeout().toNanos())).toList();
        if (expired.isEmpty()) return;
        Route fenced = new Route(domain.identity, Mode.FENCED, old.ledger, old.context, old.revision, false, old.target);
        if (!replace(domain, old, fenced)) return;
        old.ledger.fence(fenced.pending);
        // Existing entrants hold tracking leases, so an eligible entry cannot be in their debit set.
        List<GuardConstraint> removed = expired.stream().filter(entry -> entry.expired(now, settings.attemptTimeout().toNanos()))
                .map(entry -> new GuardConstraint(entry.key, entry.metadata.get().request().algorithm(), null, true)).toList();
        if (old.ledger.local.constrainFenced(fenced.pending, removed)) {
            Set<BucketIdentity> keys = new HashSet<>(); removed.forEach(item -> keys.add(item.key()));
            for (RecoveryTracking.Entry entry : expired)
                if (keys.contains(entry.key) && old.ledger.pinned.remove(entry) != null) tracking.release(entry);
        }
        if (domain.route.get() == fenced) {
            old.ledger.local.resume(fenced.pending);
            replace(domain, fenced, new Route(domain.identity, Mode.LOCAL, old.ledger, old.context, old.revision, false, old.target));
        }
    }

    private CompletionStage<Void> drive(Domain domain, AtomicReference<Route> owned, Configuration cfg,
                                        RecoveryPolicySource.Target target, RecoveryControlResult info, RecoveryWork.Budget budget) {
        RecoveryConfiguration desired = target.configuration(cfg.revision);
        Route route = owned.get(); RecoveryContext context = info.context();
        if (desired.matches(context)) {
            domain.blockedTarget = null; domain.authorizedRevision = Math.max(domain.authorizedRevision, cfg.revision);
        }
        if (route.ledger != null && info.retiredEpoch() >= route.ledger.bornEpoch) {
            Route next = new Route(domain.identity, Mode.FENCED, null, context, cfg.revision, false, target);
            if (!replace(domain, route, next)) return CompletableFuture.completedFuture(null);
            owned.set(next); route.ledger.retire(route.pending); route = next;
        }
        if (context.phase() == RecoveryPhase.NORMAL && route.ledger == null) {
            if (!desired.matches(context) || route.revision != cfg.revision
                    || route.mode == Mode.FENCED) {
                Route captured = route;
                return budget.call(() -> primary.configure(context, desired)).thenAccept(updated -> {
                    if (!current(domain, captured, cfg)) return;
                    observe(domain, updated);
                    if (updated.applied() && desired.matches(updated.context())) {
                        retireSuperseded(domain, updated.context());
                        domain.blockedTarget = null;
                        domain.authorizedRevision = Math.max(domain.authorizedRevision, cfg.revision);
                        Route normal = new Route(domain.identity, Mode.NORMAL, null, updated.context(), cfg.revision, false, target);
                        if (replace(domain, captured, normal)) owned.set(normal);
                    } else if (current(domain, captured, cfg) && !desired.matches(updated.context())) {
                        waitForConfiguration(domain, captured, owned, updated.context(), cfg, target);
                    }
                });
            }
            if (route.mode != Mode.NORMAL || !context.equals(route.context)) {
                Route normal = new Route(domain.identity, Mode.NORMAL, null, context, cfg.revision, false, target);
                if (replace(domain, route, normal)) owned.set(normal);
            }
            return CompletableFuture.completedFuture(null);
        }
        if (context.phase() == RecoveryPhase.NORMAL || !desired.matches(context)
                || route.revision != cfg.revision) {
            Route captured = route;
            return budget.call(() -> primary.begin(context, desired)).thenCompose(opened -> {
                if (!current(domain, captured, cfg)) return CompletableFuture.completedFuture(null);
                observe(domain, opened);
                if (!opened.applied()) {
                    if (!desired.matches(opened.context()))
                        waitForConfiguration(domain, captured, owned, opened.context(), cfg, target);
                    return CompletableFuture.completedFuture(null);
                }
                return drive(domain, owned, cfg, target, opened, budget);
            });
        }
        if (context.phase() == RecoveryPhase.GATHER && route.mode == Mode.GUARDED && context.equals(route.context))
            return CompletableFuture.completedFuture(null);
        if (context.phase() == RecoveryPhase.DRAIN && route.readySent && context.equals(route.context)) {
            Route captured = route;
            return budget.call(() -> primary.ready(context)).thenAccept(ready -> {
                if (current(domain, captured, cfg) && ready.context().phase() == RecoveryPhase.NORMAL) finishNormal(domain, captured, owned, ready, cfg);
            });
        }
        Ledger ledger = route.ledger == null ? new Ledger(info.retiredEpoch() + 1, bindings(domain.identity), target) : route.ledger;
        Route fenced = new Route(domain.identity, Mode.FENCED, ledger, context, cfg.revision, false, target);
        if (!replace(domain, route, fenced)) return CompletableFuture.completedFuture(null);
        owned.set(fenced); ledger.fence(fenced.pending);
        for (Attempt attempt : attempts.keySet()) if (attempt.domain == domain && !attempt.released.get()) ledger.pin(attempt.entries);
        return drain(ledger, budget).thenCompose(ignored -> snapshot(domain, fenced, cfg, budget)).thenCompose(buckets -> {
            if (!current(domain, fenced, cfg)) return CompletableFuture.completedFuture(false);
            return budget.call(() -> primary.seed(context, buckets));
        }).thenCompose(seeded -> {
            if (!seeded || !current(domain, fenced, cfg)) return CompletableFuture.completedFuture(null);
            if (context.phase() == RecoveryPhase.GATHER) {
                return budget.call(() -> primary.join(context)).thenAccept(joined -> {
                    if (!current(domain, fenced, cfg) || !joined.applied()) return;
                    observe(domain, joined);
                    retireSuperseded(domain, joined.context());
                    if (joined.context().phase() == RecoveryPhase.GATHER) {
                        ledger.local.resume(fenced.pending);
                        Route guarded = new Route(domain.identity, Mode.GUARDED, ledger, joined.context(), cfg.revision, false, target);
                        if (replace(domain, fenced, guarded)) owned.set(guarded);
                    }
                    if (joined.context().phase() == RecoveryPhase.DRAIN) {
                        Route draining = new Route(domain.identity, Mode.FENCED, ledger, joined.context(), cfg.revision, false, target);
                        if (replace(domain, fenced, draining)) { ledger.fence(draining.pending); owned.set(draining); }
                    }
                });
            }
            Route ready = new Route(domain.identity, Mode.READY, ledger, context, cfg.revision, true, target);
            if (!replace(domain, fenced, ready)) return CompletableFuture.completedFuture(null);
            ledger.fence(ready.pending); owned.set(ready);
            return budget.call(() -> primary.ready(context)).thenAccept(result -> {
                if (current(domain, ready, cfg) && result.context().phase() == RecoveryPhase.NORMAL)
                    finishNormal(domain, ready, owned, result, cfg);
            });
        });
    }

    private void waitForConfiguration(Domain domain, Route route, AtomicReference<Route> owned,
                                      RecoveryContext context, Configuration cfg, RecoveryPolicySource.Target target) {
        domain.blockedTarget = target.configuration(cfg.revision);
        if (route.mode == Mode.FENCED && context.equals(route.context)) return;
        Route pending = new Route(domain.identity, Mode.FENCED, route.ledger, context, cfg.revision, route.readySent, route.target);
        if (replace(domain, route, pending)) {
            if (pending.ledger != null) pending.ledger.fence(pending.pending);
            owned.set(pending);
        }
    }

    private void finishNormal(Domain domain, Route ready, AtomicReference<Route> owned, RecoveryControlResult result, Configuration cfg) {
        observe(domain, result);
        retireSuperseded(domain, result.context());
        boolean matches = ready.target.configuration(cfg.revision).matches(result.context());
        domain.blockedTarget = matches ? null : ready.target.configuration(cfg.revision);
        if (matches) domain.authorizedRevision = Math.max(domain.authorizedRevision, cfg.revision);
        Route next = new Route(domain.identity, matches ? Mode.NORMAL : Mode.FENCED, null, result.context(), cfg.revision, false, ready.target);
        if (replace(domain, ready, next)) { owned.set(next); ready.ledger.retire(ready.pending); }
    }
    private CompletionStage<List<BucketState>> snapshot(Domain domain, Route route, Configuration cfg, RecoveryWork.Budget budget) {
        return budget.call(() -> {
            if (!current(domain, route, cfg)) return CompletableFuture.failedFuture(new CancellationException("retired recovery attempt"));
            AppliedTarget normalization = route.ledger.applied.get();
            if (normalization.fence() != route.pending) return CompletableFuture.failedFuture(new CancellationException("retired normalization fence"));
            List<GuardConstraint> constraints = new ArrayList<>();
            Map<BucketIdentity, Limit> targets = new LinkedHashMap<>();
            Map<BucketIdentity, Algorithm> algorithms = new HashMap<>();
            Map<RecoveryTracking.Entry, RecoveryTracking.Entry.Metadata> observations = new HashMap<>();
            for (RecoveryTracking.Entry entry : route.ledger.pinned.keySet()) {
                RecoveryTracking.Entry.Metadata observation = entry.metadata.get();
                observations.put(entry, observation);
                LevelRequest metadata = observation.request();
                Limit full = route.target.resolve(entry.key, metadata.keyGroup(), metadata.limit())
                        .orElseThrow(() -> new IllegalStateException("effective limit is unavailable during recovery"));
                Limit local = DegradedShare.of(full, settings.cohort().size()).localLimit().orElse(null);
                boolean changed = full.capacity() != metadata.limit().capacity()
                        || full.emissionIntervalNanos() != metadata.limit().emissionIntervalNanos()
                        || metadata.resolverRevision() != route.target.resolverRevision()
                        || !metadata.resolverFingerprint().equals(route.target.resolverFingerprint());
                constraints.add(new GuardConstraint(entry.key, metadata.algorithm(), local, changed));
                targets.put(entry.key, full); algorithms.put(entry.key, metadata.algorithm());
            }
            if (!current(domain, route, cfg) || !route.ledger.local.constrainFenced(route.pending, constraints))
                return CompletableFuture.failedFuture(new CancellationException("retired recovery attempt"));
            // Retain the normalized full target so a retry does not repeatedly discard elapsed credit.
            for (var captured : observations.entrySet()) {
                RecoveryTracking.Entry entry = captured.getKey();
                LevelRequest before = captured.getValue().request(); Limit full = targets.get(entry.key);
                if (!full.equals(before.limit()) || before.resolverRevision() != route.target.resolverRevision()
                        || !before.resolverFingerprint().equals(route.target.resolverFingerprint())) {
                    LevelRequest normalized = new LevelRequest(entry.key, full, before.algorithm(), before.weight(),
                            before.keyGroup(), route.target.fingerprint(), route.target.resolverRevision(), route.target.resolverFingerprint());
                    entry.metadata.compareAndSet(captured.getValue(), new RecoveryTracking.Entry.Metadata(normalized, cfg.revision, clock.getAsLong()));
                    entry.horizon.accumulateAndGet(full.capacity() * full.emissionIntervalNanos(), Math::max);
                    entry.lastUse.accumulateAndGet(clock.getAsLong(), (old, value) -> value - old > 0 ? value : old);
                }
            }
            Map<BucketIdentity, Long> balances = new HashMap<>();
            route.ledger.local.snapshotFenced(route.pending).forEach(bucket -> balances.put(bucket.storageKey(), bucket.remaining()));
            List<BucketState> result = new ArrayList<>();
            targets.forEach((key, full) -> result.add(new BucketState(key, full, algorithms.get(key),
                    Math.min(DegradedShare.of(full, settings.cohort().size()).capacity(), balances.getOrDefault(key, 0L)))));
            if (!current(domain, route, cfg) || !route.ledger.applied.compareAndSet(normalization, new AppliedTarget(route.pending, route.target)))
                return CompletableFuture.failedFuture(new CancellationException("retired normalization publication"));
            return CompletableFuture.completedFuture(result);
        });
    }
    private CompletionStage<Void> drain(Ledger ledger, RecoveryWork.Budget budget) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        class Check implements Runnable {
            @Override public void run() {
                if (closed.get()) { result.completeExceptionally(new CancellationException("closed")); return; }
                if (ledger.entrants.get() == 0) { result.complete(null); return; }
                if (budget.remaining() == 0) {
                    result.completeExceptionally(new TimeoutException("local admission drain timed out")); return;
                }
                RecoveryWork.TIMER.schedule(this, 10, TimeUnit.MILLISECONDS);
            }
        }
        new Check().run(); return result;
    }

    @Override public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
        Configuration cfg;
        observationLock.lock();
        try {
            if (closed.get()) return CompletableFuture.completedFuture(ChainResult.rejected(0, 0, 0));
            cfg = configuration.get();
            if (cfg == null) throw new PolicyConfigurationException("recovery configuration is absent");
            cfg.owners.incrementAndGet();
        } finally { observationLock.unlock(); }
        var result = new CompletableFuture<ChainResult>();
        try {
            acquireObserved(chain, cfg).whenComplete((value, failure) -> {
                cfg.release();
                if (failure == null) result.complete(value); else result.completeExceptionally(failure);
            });
        } catch (RuntimeException | Error failure) { cfg.release(); throw failure; }
        return result;
    }
    private CompletionStage<ChainResult> acquireObserved(List<LevelRequest> chain, Configuration cfg) {
        List<LevelRequest> requests = List.copyOf(chain); LevelRequest.validateChain(requests);
        if (closed.get()) return CompletableFuture.completedFuture(ChainResult.rejected(0, 0, 0));
        if (enrollmentFailure != null) return CompletableFuture.failedFuture(enrollmentFailure);
        Domain domain = domains.get(requests.get(0).storageKey().domain());
        if (domain == null) throw new PolicyConfigurationException("recovery domain has not been configured");
        if (domain.incompatible != null) return CompletableFuture.failedFuture(domain.incompatible);
        Route route = domain.route.get();
        String fingerprint = cfg.source.domains().contains(domain.identity) ? cfg.source.fingerprint(domain.identity) : null;
        if (fingerprint == null) return CompletableFuture.completedFuture(fallback(cfg, requests, ChainResult.rejected(0, 0, 0)));
        if (route.target == null || route.revision != cfg.revision)
            return CompletableFuture.completedFuture(fallback(cfg, requests, pending(domain)));
        if (domain.blockedTarget != null && domain.blockedTarget.equals(route.target.configuration(route.revision))) {
            quiesce(domain, route); return CompletableFuture.completedFuture(fallback(cfg, requests, pending(domain)));
        }
        for (int i = 0; i < requests.size(); i++) {
            LevelRequest request = requests.get(i);
            if (request.weight() > request.limit().capacity()) return CompletableFuture.completedFuture(ChainResult.rejected(i, 0, 0));
            if (request.configurationFingerprint() != null && !request.configurationFingerprint().equals(fingerprint))
                return CompletableFuture.completedFuture(fallback(cfg, requests, pending(domain)));
            if (!route.target.active(request.storageKey())) return CompletableFuture.completedFuture(fallback(cfg, requests, ChainResult.rejected(i, 0, 0)));
            if (request.resolverRevision() != route.target.resolverRevision()
                    || !request.resolverFingerprint().equals(route.target.resolverFingerprint())) {
                if (request.resolverRevision() > route.target.resolverRevision()) quiesce(domain, route);
                return CompletableFuture.completedFuture(fallback(cfg, requests, pending(domain)));
            }
            if (request.resolverRevision() > 0 && request.resolverRevision() < Math.max(domain.localResolver.revision(), domain.seenResolver.revision())) {
                quiesce(domain, route); return CompletableFuture.completedFuture(fallback(cfg, requests, pending(domain)));
            }
            var effective = route.target.resolve(request.storageKey(), request.keyGroup(), null);
            if (effective.isEmpty()) return CompletableFuture.completedFuture(fallback(cfg, requests, ChainResult.rejected(i, 0, 0)));
            if (effective.orElseThrow().capacity() != request.limit().capacity()
                    || effective.orElseThrow().emissionIntervalNanos() != request.limit().emissionIntervalNanos())
                throw new PolicyConfigurationException("request limit differs from its captured recovery target");
            if (!approved.contains(PolicyBinding.of(request.storageKey(), request.algorithm())))
                return CompletableFuture.completedFuture(fallback(cfg, requests, pending(domain)));
        }
        if (route.mode != Mode.NORMAL && route.mode != Mode.LOCAL && route.mode != Mode.GUARDED)
            return CompletableFuture.completedFuture(fallback(cfg, requests, pending(domain)));
        boolean guarded = route.mode != Mode.NORMAL;
        if (guarded && !route.ledger.normalizedFor(route.target)) {
            quiesce(domain, route); return CompletableFuture.completedFuture(fallback(cfg, requests, pending(domain)));
        }
        if (guarded) for (int i = 0; i < requests.size(); i++) {
            LevelRequest request = requests.get(i);
            if (!DegradedShare.of(request.limit(), settings.cohort().size()).canFit(request.weight())) {
                if (diagnostics.add(request.storageKey().policyId())) log.warn("policy '{}' has no usable degraded share for this weight", request.storageKey().policyId());
                ChainResult rejected = ChainResult.rejected(i, 0, 0);
                if (DegradedShare.of(request.limit(), settings.cohort().size()).capacity() == 0)
                    rejected = rejected.withBudgets(List.of(new LevelBudget(i, new StoreBudget(0, 0, true))));
                return CompletableFuture.completedFuture(fallback(cfg, requests, rejected));
            }
        }
        List<RecoveryTracking.Entry> entries = tracking.retain(requests, clock.getAsLong(), cfg.revision);
        if (entries == null) {
            if (trackingPressureReported.compareAndSet(false, true)) log.warn("recovery tracking capacity reached; new quota identities are being rejected");
            return CompletableFuture.completedFuture(fallback(cfg, requests, ChainResult.rejected(0, 0, 0)));
        }
        if (domain.route.get() != route) { entries.forEach(tracking::release); return CompletableFuture.completedFuture(fallback(cfg, requests, pending(domain))); }
        ChainResult localGrant = null;
        if (guarded) {
            Ledger ledger = route.ledger; ledger.entrants.incrementAndGet();
            try {
                if (domain.route.get() != route || ledger.retired.get()) {
                    entries.forEach(tracking::release); return CompletableFuture.completedFuture(fallback(cfg, requests, pending(domain)));
                }
                ledger.pin(entries);
                List<LevelRequest> scaled = requests.stream().map(request -> new LevelRequest(request.storageKey(),
                        DegradedShare.of(request.limit(), settings.cohort().size()).localLimit().orElseThrow(), request.algorithm(),
                        request.weight(), request.keyGroup())).toList();
                ChainResult local = ledger.local.tryAcquireAll(scaled).toCompletableFuture().join().asDegraded();
                localGrant = local;
                if (!local.acquired() || route.mode == Mode.LOCAL) {
                    entries.forEach(tracking::release);
                    return CompletableFuture.completedFuture(fallback(cfg, requests, domain.route.get() == route ? local : pending(domain)));
                }
            } finally { ledger.entrants.decrementAndGet(); }
        }
        if (!inFlight.tryAcquire()) {
            if (dispatchPressureReported.compareAndSet(false, true)) log.warn("primary attempt capacity reached; new dispatches are being rejected");
            entries.forEach(tracking::release);
            return CompletableFuture.completedFuture(fallback(cfg, requests, ChainResult.rejected(0, 0, 0)));
        }
        ChainResult capturedGuard = localGrant;
        Attempt attempt = new Attempt(domain, route, entries, requests, cfg); attempts.put(attempt, Boolean.TRUE);
        if (closed.get()) {
            attempt.finish(ChainResult.rejected(0, 0, 0), false); attempt.release(); return attempt.result.copy();
        }
        call(() -> {
            if (closed.get() || domain.route.get() != route) return CompletableFuture.failedFuture(
                    new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED));
            return primary.acquire(route.context, requests, guarded, route.pending);
        }).whenComplete((result, failure) -> {
            if (failure == null && (result == null || result.firedLevelIndex() < 0 || result.firedLevelIndex() >= requests.size()
                    || (result.acquired() && result.firedLevelIndex() != requests.size() - 1)
                    || result.remaining() < 0 || result.retryAfterMillis() < 0
                    || (result.recoveryPending() != null && !result.recoveryPending().domain().equals(domain.identity))))
                failure = new StateCompatibilityException("primary returned an invalid acquisition outcome");
            if (failure == null) {
                attempt.release();
                if (domain.route.get() != route) attempt.finish(pending(domain), true);
                else if (result.recoveryPending() != null) {
                    quiesce(domain, route); attempt.finish(pending(domain), true);
                } else attempt.finish(guarded ? guardedOutcome(route.ledger, requests, capturedGuard, result) : result, guarded);
            } else {
                Throwable cause = unwrap(failure);
                if (callerError(cause)) {
                    attempt.release();
                    if (domain.route.get() == route) incompatible(domain, asRuntime(cause));
                    attempt.fail(cause);
                } else {
                    boolean notDispatched = cause instanceof PrimaryDispatchException dispatch
                            && dispatch.outcome() == PrimaryDispatchException.Outcome.NOT_DISPATCHED;
                    if (notDispatched) attempt.release();
                    localAfterFailure(domain, route, entries);
                    attempt.finish(ChainResult.rejected(0, 0, 0), true);
                }
            }
        });
        return attempt.result.copy();
    }
    @Override public StoreResult tryAcquire(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) {
        return tryAcquireAsync(key, limit, algorithm, weight).toCompletableFuture().join();
    }
    @Override public CompletionStage<StoreResult> tryAcquireAsync(BucketIdentity key, Limit limit, Algorithm algorithm, long weight) {
        return tryAcquireAll(List.of(new LevelRequest(key, limit, algorithm, weight))).thenApply(result ->
                result.singleResult());
    }

    private static ChainResult guardedOutcome(Ledger ledger, List<LevelRequest> requests, ChainResult local, ChainResult primary) {
        if (primary.acquired()) {
            var budgets = new java.util.ArrayList<LevelBudget>();
            for (LevelBudget guard : local.budgets()) {
                var remote = primary.budgets().stream().filter(value -> value.level() == guard.level()).findFirst();
                remote.ifPresent(value -> budgets.add(new LevelBudget(guard.level(), new StoreBudget(guard.budget().capacity(),
                        Math.min(guard.budget().remaining(), value.budget().remaining()), true))));
            }
            return ChainResult.acquired(primary.firedLevelIndex(), Math.min(local.remaining(), primary.remaining())).withBudgets(budgets);
        }
        var budgets = new java.util.ArrayList<LevelBudget>();
        long retry = primary.retryAfterMillis(), remaining = primary.remaining();
        for (int i = 0; i < requests.size(); i++) {
            LevelRequest request = requests.get(i);
            var state = ledger.local.inspect(request.storageKey()).orElse(null);
            if (state == null) return ChainResult.rejected(primary.firedLevelIndex(), 0, 0);
            if (i == primary.firedLevelIndex()) remaining = Math.min(remaining, state.remaining());
            int index = i;
            primary.budgets().stream().filter(value -> value.level() == index).findFirst().ifPresent(value ->
                    budgets.add(new LevelBudget(index, new StoreBudget(state.limit().capacity(),
                            Math.min(state.remaining(), value.budget().remaining()), true))));
            if (retry > 0 && state.remaining() < request.weight()) {
                long nanos = (request.weight() - state.remaining()) * state.limit().emissionIntervalNanos();
                retry = Math.max(retry, nanos / 1_000_000 + (nanos % 1_000_000 == 0 ? 0 : 1));
            }
        }
        return ChainResult.rejected(primary.firedLevelIndex(), remaining, retry).withBudgets(budgets);
    }

    private void localAfterFailure(Domain domain, Route captured, List<RecoveryTracking.Entry> entries) {
        if (closed.get() || domain.route.get() != captured || captured.readySent) return;
        RecoveryContext context = domain.observedContext == null ? captured.context : domain.observedContext;
        boolean freshPolicy = domain.authorizedRevision >= 0 && captured.revision > domain.authorizedRevision;
        if (context == null || context.phase() == RecoveryPhase.DRAIN || domain.blockedTarget != null
                || (!freshPolicy && !captured.target.fingerprint().equals(context.configurationFingerprint()))
                || (captured.target.resolverRevision() > 0 && captured.target.resolverRevision() < domain.seenResolver.revision())
                || (captured.ledger != null && domain.seenRetired >= captured.ledger.bornEpoch)) {
            quiesce(domain, captured); return;
        }
        Ledger ledger = captured.ledger != null ? captured.ledger : new Ledger(domain.seenRetired + 1, bindings(domain.identity), captured.target);
        ledger.pin(entries);
        Route local = new Route(domain.identity, Mode.LOCAL, ledger, context, captured.revision, false, captured.target);
        if (replace(domain, captured, local)) updateState();
        else if (captured.ledger == null) ledger.retire(null);
    }
    private void quiesce(Domain domain, Route old) {
        if (old.mode != Mode.NORMAL && old.mode != Mode.LOCAL && old.mode != Mode.GUARDED) return;
        Route next = new Route(domain.identity, Mode.FENCED, old.ledger, old.context, old.revision, old.readySent, old.target);
        if (replace(domain, old, next)) {
            domain.backoff = new Backoff(0, 0);
            if (next.ledger != null) next.ledger.fence(next.pending);
        }
        updateState();
    }
    private void controlFailed(Domain domain, Route route, Throwable failure) {
        if (callerError(failure)) { incompatible(domain, asRuntime(failure)); return; }
        if (route.context != null && route.context.phase() == RecoveryPhase.DRAIN)
            domain.abortRequested = route.context;
        backoff(domain);
        if (domain.blockedTarget != null || (route.ledger != null && !route.ledger.normalizedFor(route.target))) return;
        if ((route.mode == Mode.NORMAL || route.mode == Mode.FENCED) && !route.readySent
                && route.context != null && route.context.phase() == RecoveryPhase.NORMAL && route.target != null) {
            localAfterFailure(domain, route, List.of());
            return;
        }
        if (route.ledger != null && !route.readySent && route.context != null && route.context.phase() == RecoveryPhase.GATHER) {
            route.ledger.local.resume(route.pending);
            replace(domain, route, new Route(domain.identity, Mode.LOCAL, route.ledger, route.context, route.revision, false, route.target));
        }
    }
    private void backoff(Domain domain) {
        long base = settings.controlInterval().toNanos();
        long cap = Math.max(base, Duration.ofSeconds(30).toNanos());
        long delay = domain.backoff.duration == 0 ? base : Math.min(cap, domain.backoff.duration * 2);
        domain.backoff = new Backoff(clock.getAsLong(), delay);
    }
    private void observe(Domain domain, RecoveryControlResult info) {
        RecoveryContext c = info.context();
        int size = settings.cohort().size();
        RecoveryConfiguration.validateResolver(info.resolverFloor(), info.resolverFloorFingerprint());
        if ((c.resolverRevision() > 0 && (c.resolverRevision() != info.resolverFloor()
                || !c.resolverFingerprint().equals(info.resolverFloorFingerprint())))
                || info.resolverFloor() < domain.seenResolver.revision()
                || (info.resolverFloor() == domain.seenResolver.revision() && !info.resolverFloorFingerprint().equals(domain.seenResolver.fingerprint()))
                || (info.resolverFloor() == domain.localResolver.revision() && !info.resolverFloorFingerprint().equals(domain.localResolver.fingerprint())))
            throw new StateCompatibilityException("resolver ordering metadata is inconsistent or moved backward");
        boolean shape = info.retiredEpoch() >= 0 && info.retiredEpoch() <= c.epoch()
                && info.joinedMembers() >= 0 && info.joinedMembers() <= size && info.readyMembers() >= 0 && info.readyMembers() <= size;
        shape &= switch (c.phase()) {
            case NORMAL -> info.joinedMembers() == size && info.readyMembers() == size && info.retiredEpoch() == c.epoch();
            case GATHER -> info.joinedMembers() < size && info.readyMembers() == 0 && info.retiredEpoch() < c.epoch();
            case DRAIN -> info.joinedMembers() == size && info.readyMembers() < size && info.retiredEpoch() < c.epoch();
        };
        if (!shape || !c.domain().equals(domain.identity) || !c.session().equals(session.get()))
            throw new StateCompatibilityException("primary returned incompatible recovery control metadata");
        if (c.epoch() < domain.seenEpoch || c.dispatchGeneration() < domain.seenDispatch
                || c.configurationVersion() < domain.seenConfiguration || info.retiredEpoch() < domain.seenRetired)
            throw new StateCompatibilityException("recovery controller moved backward");
        domain.seenEpoch = c.epoch(); domain.seenDispatch = c.dispatchGeneration();
        domain.seenConfiguration = c.configurationVersion(); domain.seenRetired = info.retiredEpoch();
        domain.seenResolver = new ResolverIdentity(info.resolverFloor(), info.resolverFloorFingerprint());
        domain.observedContext = c;
    }
    private void retireSuperseded(Domain domain, RecoveryContext context) {
        for (Attempt attempt : attempts.keySet()) if (attempt.domain == domain
                && (attempt.context.epoch() < context.epoch() || attempt.context.dispatchGeneration() < context.dispatchGeneration()
                    || attempt.context.configurationVersion() < context.configurationVersion())) {
            attempt.finish(pending(domain), true); attempt.release();
        }
    }
    private boolean current(Domain domain, Route route, Configuration cfg) {
        return !closed.get() && domain.route.get() == route && configuration.get() == cfg;
    }
    private boolean replace(Domain domain, Route old, Route next) {
        if (closed.get() || !domain.route.compareAndSet(old, next)) return false;
        old.signal.complete(null); return true;
    }
    private List<PolicyBinding> bindings(QuotaDomain domain) {
        return history.stream().filter(binding -> binding.domain().equals(domain)).toList();
    }
    private ChainResult pending(Domain domain) { return ChainResult.pending(0, domain.route.get().pending); }
    private <T> CompletableFuture<T> call(Supplier<? extends CompletionStage<T>> work) {
        return RecoveryWork.call(() -> closed.get() ? CompletableFuture.failedFuture(
                new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED)) : work.get(), settings.attemptTimeout());
    }
    private void incompatible(Domain domain, RuntimeException error) {
        domain.incompatible = error;
        Route old = domain.route.get();
        Route blocked = new Route(domain.identity, Mode.INCOMPATIBLE, old.ledger, old.context, old.revision, old.readySent, old.target);
        if (replace(domain, old, blocked) && blocked.ledger != null) blocked.ledger.fence(blocked.pending);
        updateState();
    }
    public DegradationState state() {
        if (closed.get() || session.get() == null || enrollmentFailure != null || domains.isEmpty()) return DegradationState.OPEN;
        for (Domain domain : domains.values()) if (domain.route.get().mode != Mode.NORMAL || domain.incompatible != null) return DegradationState.OPEN;
        return DegradationState.CLOSED;
    }
    public int trackedBuckets() { return tracking.size(); }
    private void updateState() {
        if (stateNotifications.getAndIncrement() != 0) return;
        int consumed = 1;
        do {
            DegradationState next = state(), previous = observed.getAndSet(next);
            if (next != previous) notifyTransition(previous, next, next == DegradationState.CLOSED
                    ? "all domain barriers completed" : "conservative recovery is active");
            consumed = stateNotifications.addAndGet(-consumed);
        } while (consumed != 0);
    }
    private void notifyListeners(java.util.function.Consumer<DegradationListener> event) {
        for (int i = 0; i < listeners.size(); i++) {
            var listener = listeners.get(i); callbacks.get(i).submit(() -> event.accept(listener));
        }
    }
    public CompletionStage<Void> flushObservations() {
        return CompletableFuture.allOf(callbacks.stream().map(io.quotaflow.core.execution.CallbackDispatcher::barrier).toArray(CompletableFuture[]::new));
    }
    public long observationFailures() { return callbacks.stream().mapToLong(io.quotaflow.core.execution.CallbackDispatcher::failures).sum(); }
    private void notifyTransition(DegradationState from, DegradationState to, String reason) {
        log.warn("degradation state changed from {} to {}: {}", from, to, reason);
        notifyListeners(listener -> listener.onTransition(from, to, reason));
    }
    private ChainResult fallback(Configuration cfg, List<LevelRequest> chain, ChainResult result) {
        if (closed.get()) return ChainResult.rejected(result.firedLevelIndex(), 0, 0);
        LevelRequest fired = chain.get(result.firedLevelIndex());
        String fingerprint = fired.configurationFingerprint();
        boolean currentTarget = cfg.policies.contains(fired.storageKey().policyId())
                && cfg.source.domains().contains(fired.storageKey().domain())
                && (fingerprint == null || fingerprint.equals(cfg.source.fingerprint(fired.storageKey().domain())));
        long revision = cfg.revision; String policyId = fired.storageKey().policyId(), group = fired.keyGroup();
        Verdict verdict = result.acquired() ? Verdict.ALLOWED : Verdict.REJECTED;
        notifyListeners(listener -> listener.onFallbackDecision(revision, currentTarget, policyId, group, verdict));
        return result;
    }
    private static Throwable unwrap(Throwable failure) {
        while ((failure instanceof CompletionException || failure instanceof ExecutionException) && failure.getCause() != null) failure = failure.getCause();
        return failure;
    }
    private static boolean callerError(Throwable failure) { return failure instanceof IllegalArgumentException || failure instanceof NullPointerException; }
    private static RuntimeException asRuntime(Throwable failure) { return failure instanceof RuntimeException r ? r : new CompletionException(failure); }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        ScheduledFuture<?> future = ticker.getAndSet(null); if (future != null) future.cancel(false);
        for (Domain domain : domains.values()) {
            Route route = domain.route.get();
            if (route.ledger != null) { route.ledger.fence(route.pending); route.ledger.retire(route.pending); }
            route.signal.completeExceptionally(new CancellationException("recovery owner closed"));
        }
        for (Attempt attempt : attempts.keySet()) {
            attempt.finish(ChainResult.rejected(0, 0, 0), false); attempt.release();
        }
        updateState();
        observationLock.lock();
        try {
            var current = configuration.get();
            if (current != null) { current.retired = true; current.retireIfDrained(); }
            if (configurations.isEmpty()) callbacks.forEach(io.quotaflow.core.execution.CallbackDispatcher::close);
        } finally { observationLock.unlock(); }
    }
}
