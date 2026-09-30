package io.quotaflow.core;

import io.quotaflow.core.store.RateLimitStore;
import io.quotaflow.core.execution.BoundedExecution;
import io.quotaflow.core.execution.DeadlineScheduler;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.function.BooleanSupplier;

/**
 * Completion-driven quota acquisition. Positive budgets start at API entry;
 * zero-wait calls perform one evaluation under the operational timeout.
 * Waiters retain continuations and timers, never a parked library worker.
 * Cancellation suppresses later admission and notification, but cannot undo an
 * already dispatched store debit. Strict priority is FIFO within a priority;
 * deadlines bound starvation. Listener callbacks must be fast and nonblocking.
 */
public final class DefaultQuotaFlow implements QuotaFlow {
    private static final Executor DELIVERY = Executors.newFixedThreadPool(2, task -> {
        Thread thread = new Thread(task, "quotaflow-delivery");
        thread.setDaemon(true); return thread;
    });
    private final AtomicReference<PolicySet> policySets;
    private final PolicyEngine engine;
    private final List<DecisionListener> listeners;
    private final List<WaitListener> waitListeners;
    private final int maxWaitersPerPolicy;
    private final Executor asyncExecutor;
    private final BoundedExecution execution;
    private final long operationNanos;
    private final LongSupplier nanoClock;
    private final ConcurrentHashMap<String, WaiterQueue> queues = new ConcurrentHashMap<>();
    private final AtomicLong waiterSequences = new AtomicLong();
    private final AtomicLong configurationGeneration = new AtomicLong();

    private DefaultQuotaFlow(Builder builder) {
        policySets = new AtomicReference<>(builder.policySet);
        engine = new PolicyEngine(builder.store, builder.defaultResolver, builder.namedResolvers, builder.limitResolver, builder.namespace);
        engine.registerPolicies(builder.policySet).toCompletableFuture().join();
        listeners = List.copyOf(builder.listeners);
        waitListeners = List.copyOf(builder.waitListeners);
        maxWaitersPerPolicy = builder.maxWaitersPerPolicy;
        execution = builder.execution; operationNanos = builder.operationTimeout.toNanos(); nanoClock = builder.nanoClock;
        asyncExecutor = builder.asyncExecutor == null ? Runnable::run : builder.asyncExecutor;
    }
    public static Builder builder(PolicySet policies, RateLimitStore store) { return new Builder(policies, store); }
    public void replacePolicySet(PolicySet policies) {
        engine.registerPolicies(Objects.requireNonNull(policies, "policySet")).toCompletableFuture().join();
        policySets.set(policies); configurationGeneration.incrementAndGet(); queues.values().forEach(WaiterQueue::signalHead);
    }
    public int waitQueueDepth(String policyId) { var queue = queues.get(policyId); return queue == null ? 0 : queue.size(); }
    int retainedQueues() { return queues.size(); }
    @Override public Decision tryAcquire(String id, RateLimitContext context) { return tryAcquire(id, context, 1); }
    @Override public Decision tryAcquire(String id, RateLimitContext context, long weight) {
        return acquire(id, context, weight, Duration.ZERO);
    }
    @Override public CompletionStage<Decision> tryAcquireAsync(String id, RateLimitContext context, long weight) {
        return acquireAsync(id, context, weight, Duration.ZERO);
    }
    @Override public Decision acquire(String id, RateLimitContext context, long weight, Duration timeout) {
        return synchronous(new Acquisition(id, context, weight, timeout, null));
    }
    @Override public Decision acquire(String id, RateLimitContext context, long weight, Duration timeout, int priority) {
        return synchronous(new Acquisition(id, context, weight, timeout, priority));
    }
    @Override public CompletionStage<Decision> acquireAsync(String id, RateLimitContext context, long weight, Duration timeout) {
        return asynchronous(new Acquisition(id, context, weight, timeout, null));
    }
    @Override public CompletionStage<Decision> acquireAsync(String id, RateLimitContext context, long weight, Duration timeout, int priority) {
        return asynchronous(new Acquisition(id, context, weight, timeout, priority));
    }
    private CompletionStage<Decision> asynchronous(Acquisition acquisition) {
        acquisition.armDeadline();
        try { asyncExecutor.execute(acquisition::progress); }
        catch (RejectedExecutionException failure) { acquisition.overflow(); }
        catch (RuntimeException failure) { acquisition.fail(failure); }
        return acquisition.result;
    }
    private Decision synchronous(Acquisition acquisition) {
        acquisition.armDeadline(); acquisition.progress();
        try { return acquisition.result.get(Math.max(1, acquisition.deadline - nanoClock.getAsLong()), TimeUnit.NANOSECONDS); }
        catch (InterruptedException failure) {
            acquisition.timeout(); Thread.currentThread().interrupt(); return acquisition.terminalDecision();
        } catch (TimeoutException failure) {
            acquisition.timeout(); return acquisition.terminalDecision();
        } catch (ExecutionException failure) {
            if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
            throw new CompletionException(failure.getCause());
        }
    }

    private record Terminal(Decision decision, Throwable failure) { }

    /** A completed public future retains its outcome, not the request, queues or owning limiter. */
    private static final class AcquisitionFuture extends CompletableFuture<Decision> {
        private final AtomicReference<BooleanSupplier> cancellation;
        AcquisitionFuture(BooleanSupplier cancellation) { this.cancellation = new AtomicReference<>(cancellation); }
        @Override public boolean cancel(boolean interrupt) {
            var owner = cancellation.get();
            return owner != null && owner.getAsBoolean() && super.cancel(interrupt);
        }
        void detach() { cancellation.set(null); }
    }

    private final class Acquisition {
        final String id;
        final RateLimitContext context;
        final long weight, start, deadline;
        final boolean positive;
        final RateLimitPolicy initial;
        final int priority;
        final AtomicReference<Terminal> terminal = new AtomicReference<>();
        final ReentrantLock lock = new ReentrantLock();
        final AcquisitionFuture result = new AcquisitionFuture(this::cancel);

        boolean cancel() {
            lock.lock();
            try { if (!terminal.compareAndSet(null, new Terminal(null, new CancellationException()))) return false; }
            finally { lock.unlock(); }
            cleanup(); return true;
        }
        Decision terminalDecision() {
            var outcome = terminal.get();
            if (outcome.failure() instanceof RuntimeException runtime) throw runtime;
            if (outcome.failure() != null) throw new CompletionException(outcome.failure());
            return outcome.decision();
        }
        volatile Evaluation last;
        WaiterQueue queue;
        WaiterQueue.Waiter waiter;
        volatile boolean inFlight;
        boolean ready = true;
        volatile long attemptDeadline;
        long attemptSequence;
        volatile boolean queued;
        long due, observedGeneration;
        ScheduledFuture<?> deadlineTimer, retryTimer, operationTimer;
        CompletableFuture<Void> notifications = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> readiness;

        Acquisition(String id, RateLimitContext context, long weight, Duration timeout, Integer priority) {
            start = nanoClock.getAsLong();
            Limit.validateWeight(weight);
            if (Objects.requireNonNull(timeout, "waitTimeout").isNegative()) throw new IllegalArgumentException("waitTimeout must not be negative");
            this.id = id; this.context = Objects.requireNonNull(context, "context"); this.weight = weight;
            initial = policySets.get().policy(id); this.priority = resolvePriority(priority, context, initial);
            positive = !timeout.isZero(); deadline = start + (positive ? timeout.toNanos() : operationNanos);
        }
        void armDeadline() {
            lock.lock();
            try { if (terminal.get() == null) deadlineTimer = DeadlineScheduler.schedule(this::timeout, deadline - nanoClock.getAsLong()); }
            finally { lock.unlock(); }
        }
        boolean active() {
            if (terminal.get() != null) return false;
            if (nanoClock.getAsLong() - deadline >= 0) { timeout(); return false; }
            if (inFlight && nanoClock.getAsLong() - attemptDeadline >= 0) {
                operationExpired(attemptSequence); return false;
            }
            return true;
        }
        void operationExpired(long sequence) {
            Evaluation evaluation = new Evaluation(Decision.rejectedWithoutSchedule(id, initial.scope()), "unresolvable");
            Decision decision;
            lock.lock();
            try {
                if (!inFlight || sequence != attemptSequence || terminal.get() != null) return;
                if (nanoClock.getAsLong() - deadline >= 0) evaluation = timeoutEvaluation();
                decision = claimLocked(evaluation, nanoClock.getAsLong() - deadline >= 0);
            } finally { lock.unlock(); }
            if (decision != null) publish(decision, evaluation.keyGroup());
        }
        void progress() {
            if (!active()) return;
            PolicySet policies;
            lock.lock();
            try {
                if (terminal.get() != null || inFlight) return;
                if (waiter != null) {
                    if (!queue.isHead(waiter)) return;
                    boolean changed = observedGeneration != configurationGeneration.get();
                    if (!changed && !ready) return;
                    if (!changed && due - nanoClock.getAsLong() > 0) {
                        if (retryTimer != null) retryTimer.cancel(false);
                        retryTimer = DeadlineScheduler.schedule(this::progress, due - nanoClock.getAsLong());
                        return;
                    }
                }
                attemptDeadline = nanoClock.getAsLong() + Math.min(operationNanos, Math.max(0, deadline - nanoClock.getAsLong()));
                attemptSequence++;
                inFlight = true;
                if (retryTimer != null) { retryTimer.cancel(false); retryTimer = null; }
                if (readiness != null) { readiness.cancel(false); readiness = null; }
                policies = policySets.get(); observedGeneration = configurationGeneration.get();
            } finally { lock.unlock(); }
            RateLimitPolicy policy;
            try { policy = policies.policy(id); }
            catch (PolicyConfigurationException removed) {
                complete(new Evaluation(Decision.rejectedWithoutSchedule(id, initial.scope()), last == null ? "unresolvable" : last.keyGroup()), false); return;
            }
            lock.lock();
            try {
                if (terminal.get() != null) return;
                if (positive && operationNanos < deadline - nanoClock.getAsLong())
                    { long sequence = attemptSequence;
                      operationTimer = DeadlineScheduler.schedule(() -> operationExpired(sequence), operationNanos); }
            } finally { lock.unlock(); }
            engine.evaluateInternalAsync(policies, id, context, weight, execution, this::active)
                    .whenComplete((evaluation, failure) -> {
                        lock.lock();
                        try { if (operationTimer != null) { operationTimer.cancel(false); operationTimer = null; } }
                        finally { lock.unlock(); }
                        if (!active()) return;
                        if (failure != null) {
                            while (failure instanceof CompletionException && failure.getCause() != null) failure = failure.getCause();
                            if (failure instanceof RejectedExecutionException) overflow(); else fail(failure);
                            return;
                        }
                        if (evaluation.decision().isAllowed() || policy.reaction() == Reaction.REJECT || !positive
                                || (evaluation.decision().retryAfter().isEmpty() && evaluation.recoveryPending() == null)) {
                            complete(evaluation, false); return;
                        }
                        enqueue(evaluation);
                    });
        }
        void enqueue(Evaluation evaluation) {
            boolean overflow = false;
            CompletableFuture<Void> signal = null;
            lock.lock();
            try {
                if (terminal.get() != null) return;
                last = evaluation; inFlight = false;
                if (waiter == null) {
                    waiter = new WaiterQueue.Waiter(priority, waiterSequences.getAndIncrement(), this::progress);
                    queues.compute(id, (key, current) -> {
                        queue = current == null ? new WaiterQueue(maxWaitersPerPolicy) : current;
                        queued = queue.offer(waiter); return queue;
                    });
                    if (!queued) { waiter = null; overflow = true; }
                    else notifications = notifications.thenRunAsync(() -> {
                        for (WaitListener listener : waitListeners) {
                            try { listener.onQueued(id, evaluation.keyGroup()); }
                            catch (RuntimeException failure) { logListenerFailure(failure); }
                        }
                    }, DELIVERY);
                }
                ready = evaluation.recoveryPending() == null;
                due = nanoClock.getAsLong() + evaluation.decision().retryAfter().map(Duration::toNanos).orElse(0L);
                if (!ready && !overflow) { signal = evaluation.recoveryPending().readiness().toCompletableFuture(); readiness = signal; }
            } finally { lock.unlock(); }
            if (overflow) { overflow(false); return; }
            if (signal != null) {
                var captured = signal;
                signal.whenComplete((ignored, failure) -> {
                    lock.lock();
                    try { if (readiness != captured || terminal.get() != null) return; ready = true; }
                    finally { lock.unlock(); }
                    progress();
                });
            }
            progress();
        }
        Evaluation timeoutEvaluation() {
            var previous = last;
            var decision = previous == null ? Decision.rejectedWithoutSchedule(id, initial.scope()) : previous.decision();
            if (positive) decision = decision.withThrottleRejection(ThrottleRejection.WAIT_TIMEOUT);
            return new Evaluation(decision, previous == null ? "unresolvable" : previous.keyGroup());
        }
        void timeout() { complete(timeoutEvaluation(), true); }
        void overflow() { overflow(true); }
        void overflow(boolean dispatchSaturated) {
            var previous = last;
            var decision = previous == null ? Decision.rejectedWithoutSchedule(id, initial.scope()) : previous.decision();
            // Dispatch saturation has no trustworthy schedule; quota queue overflow retains the fired level.
            if (dispatchSaturated && previous != null)
                decision = Decision.rejectedWithoutSchedule(previous.decision().policyId(), previous.decision().scope());
            if (positive && initial.reaction() == Reaction.THROTTLE) decision = decision.withThrottleRejection(ThrottleRejection.QUEUE_OVERFLOW);
            complete(new Evaluation(decision, last == null ? "unresolvable" : last.keyGroup()), false);
        }
        void complete(Evaluation evaluation, boolean timeout) {
            Decision decision;
            lock.lock();
            try {
                if (!timeout && nanoClock.getAsLong() - deadline >= 0) { evaluation = timeoutEvaluation(); timeout = true; }
                decision = claimLocked(evaluation, timeout);
            } finally { lock.unlock(); }
            if (decision != null) publish(decision, evaluation.keyGroup());
        }
        Decision claimLocked(Evaluation evaluation, boolean timeout) {
            Decision decision = evaluation.decision();
            if (queued || (positive && timeout)) decision = decision.withWait(Duration.ofNanos(Math.max(0, nanoClock.getAsLong() - start)));
            return terminal.compareAndSet(null, new Terminal(decision, null)) ? decision : null;
        }
        void publish(Decision decision, String group) {
            cleanup();
            // User listeners and dependent future callbacks never execute on the deadline scheduler.
            lock.lock();
            try { notifications = notifications.thenRunAsync(() -> {
                for (DecisionListener listener : listeners) {
                    try { listener.onDecision(decision, group); }
                    catch (RuntimeException failure) { logListenerFailure(failure); }
                }
                result.complete(decision);
            }, DELIVERY); } finally { lock.unlock(); }
        }
        void fail(Throwable failure) {
            lock.lock();
            try { if (!terminal.compareAndSet(null, new Terminal(null, failure))) return; }
            finally { lock.unlock(); }
            cleanup(); DELIVERY.execute(() -> result.completeExceptionally(failure));
        }
        void cleanup() {
            WaiterQueue owned; WaiterQueue.Waiter entry;
            lock.lock();
            try {
                if (deadlineTimer != null) { deadlineTimer.cancel(false); deadlineTimer = null; }
                if (operationTimer != null) { operationTimer.cancel(false); operationTimer = null; }
                if (retryTimer != null) { retryTimer.cancel(false); retryTimer = null; }
                if (readiness != null) { var detached = readiness; readiness = null; detached.cancel(false); }
                owned = queue; entry = waiter; queue = null; waiter = null; last = null;
                result.detach();
            } finally { lock.unlock(); }
            if (owned != null && entry != null) owned.remove(entry);
            if (owned != null) queues.computeIfPresent(id, (key, current) -> current == owned && current.size() == 0 ? null : current);
        }
    }
    private static void logListenerFailure(RuntimeException failure) {
        org.slf4j.LoggerFactory.getLogger(DefaultQuotaFlow.class).warn("quota listener failed", failure);
    }
    private static int resolvePriority(Integer explicit, RateLimitContext context, RateLimitPolicy policy) {
        if (explicit != null) return explicit;
        var attribute = context.get(RateLimitContext.PRIORITY);
        if (attribute.isEmpty()) return policy.priority();
        if (attribute.get() instanceof Number number) return number.intValue();
        throw new IllegalArgumentException("context priority must be a Number");
    }

    public static final class Builder {
        private final PolicySet policySet;
        private final RateLimitStore store;
        private KeyResolver defaultResolver = KeyResolvers.scopeBased();
        private final Map<String, KeyResolver> namedResolvers = new LinkedHashMap<>();
        private final List<DecisionListener> listeners = new ArrayList<>();
        private final List<WaitListener> waitListeners = new ArrayList<>();
        public Builder addWaitListener(WaitListener listener) {
            waitListeners.add(Objects.requireNonNull(listener, "listener")); return this;
        }
        private LimitResolver limitResolver;
        private String namespace = io.quotaflow.core.store.QuotaDomain.DEFAULT_NAMESPACE;
        private int maxWaitersPerPolicy = 1000;
        private Executor asyncExecutor;
        private io.quotaflow.core.execution.BoundedExecution execution = io.quotaflow.core.execution.BoundedExecution.shared();
        private Duration operationTimeout = Duration.ofSeconds(1);

        /** Owned bounded dispatch service; its lifecycle remains the caller's responsibility. */
        public Builder execution(io.quotaflow.core.execution.BoundedExecution execution) {
            this.execution = Objects.requireNonNull(execution, "execution"); return this;
        }
        /** Finite bound of a nonwaiting evaluation; positive caller budgets are never extended. */
        public Builder operationTimeout(Duration timeout) {
            if (Objects.requireNonNull(timeout, "timeout").isNegative() || timeout.isZero())
                throw new IllegalArgumentException("operationTimeout must be positive");
            timeout.toNanos(); this.operationTimeout = timeout; return this;
        }
        private java.util.function.LongSupplier nanoClock = System::nanoTime;

        Builder nanoClock(java.util.function.LongSupplier clock) { this.nanoClock = clock; return this; }

        private Builder(PolicySet policySet, RateLimitStore store) {
            this.policySet = Objects.requireNonNull(policySet, "policySet");
            this.store = Objects.requireNonNull(store, "store");
        }

        /** Deployment namespace; the Redis namespace must be explicitly provisioned before use. */
        public Builder namespace(String namespace) {
            this.namespace = new io.quotaflow.core.store.QuotaDomain(namespace, "validation").namespace();
            return this;
        }

        /** Default resolver used for policies without a {@code keyResolverId}. */
        public Builder defaultResolver(KeyResolver resolver) {
            this.defaultResolver = Objects.requireNonNull(resolver, "resolver");
            return this;
        }

        /** Registers a resolver under the id policies reference via {@code keyResolverId}. */
        public Builder addResolver(String id, KeyResolver resolver) {
            namedResolvers.put(Objects.requireNonNull(id, "id"), Objects.requireNonNull(resolver, "resolver"));
            return this;
        }

        /**
         * Resolver for policies declaring a dynamic {@code limitRef}. Expected
         * to be a caching wrapper; the engine consults it once per resolution.
         */
        public Builder limitResolver(LimitResolver limitResolver) {
            this.limitResolver = Objects.requireNonNull(limitResolver, "limitResolver");
            return this;
        }

        public Builder addListener(DecisionListener listener) {
            listeners.add(Objects.requireNonNull(listener, "listener"));
            return this;
        }

        /**
         * Bound of each throttle policy's waiter queue (default 1000).
         * Enqueueing onto a full queue rejects immediately with
         * {@link ThrottleRejection#QUEUE_OVERFLOW}.
         */
        public Builder maxWaitersPerPolicy(int maxWaitersPerPolicy) {
            if (maxWaitersPerPolicy < 1) {
                throw new IllegalArgumentException(
                        "maxWaitersPerPolicy must be >= 1, got " + maxWaitersPerPolicy);
            }
            this.maxWaitersPerPolicy = maxWaitersPerPolicy;
            return this;
        }

        /** Continuation admission executor. execute must return promptly; inline execution is supported.
         * Blocking SPI calls always cross the separate bounded dispatch service. */
        public Builder asyncExecutor(Executor executor) {
            this.asyncExecutor = Objects.requireNonNull(executor, "executor");
            return this;
        }

        public DefaultQuotaFlow build() {
            return new DefaultQuotaFlow(this);
        }
    }
}
