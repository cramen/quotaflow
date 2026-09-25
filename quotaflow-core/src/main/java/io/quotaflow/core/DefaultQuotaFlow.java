package io.quotaflow.core;

import io.quotaflow.core.store.RateLimitStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Default {@link QuotaFlow} composing a policy set, key resolvers, a
 * {@link RateLimitStore} and {@link DecisionListener}s.
 *
 * <p>The compiled {@link PolicySet} is held in an {@link AtomicReference} and
 * read exactly once per decision, so {@link #replacePolicySet(PolicySet)}
 * swaps configuration atomically: every decision uses one consistent set,
 * never a mix of old and new. A swap also wakes queue heads for an immediate
 * re-evaluation, so a mid-wait limit increase can free waiters early; a
 * waiter whose policy vanished from the current set is rejected with a
 * configuration-shaped decision (no refill schedule).
 *
 * <p>Throttle is a retry loop above the engine, not an engine mode: when a
 * throttle-mode policy rejects with a refill schedule, the caller enqueues in
 * the policy's bounded priority waiter queue (strict priority, FIFO within a
 * priority, {@code maxWaitersPerPolicy} bound) and parks until the rejection's
 * retry-after plus &plusmn;10% jitter. Only the queue head retries, which is
 * what makes the priority order the service order; a lost retry re-queues the
 * waiter with the new retry-after while its wait timeout lasts. Overflow and
 * expired waits are data rejections carrying a {@link ThrottleRejection}
 * reason. Waiting uses {@link LockSupport#parkNanos} outside every lock, so
 * virtual-thread callers unmount rather than pin a carrier. Every final
 * decision fires exactly one listener event carrying the total wait duration.
 * Degradation is orthogonal: the same loop runs against whatever store the
 * engine holds, including the conservative local fallback.
 *
 * <p>Strict priority can starve low-priority waiters under sustained
 * oversubscription; the caller's wait timeout bounds the harm (starved
 * waiters eventually reject rather than hang). Priority aging is deliberately
 * not implemented. The waiter queue is in-memory and instance-local: the real
 * queue bound across a fleet is instances &times; maxWaitersPerPolicy, so the
 * bound should reflect downstream capacity divided by the expected instance
 * count.
 */
public final class DefaultQuotaFlow implements QuotaFlow {

    /** Wake-time de-correlation: retry-after is jittered by &plusmn;10%. */
    private static final double JITTER = 0.10;

    private final AtomicReference<PolicySet> policySets;
    private final PolicyEngine engine;
    private final List<DecisionListener> listeners;
    private final int maxWaitersPerPolicy;
    private final Executor asyncExecutor;
    private final ConcurrentHashMap<String, WaiterQueue> queues = new ConcurrentHashMap<>();
    private final AtomicLong waiterSequences = new AtomicLong();
    private final AtomicLong configurationGeneration = new AtomicLong();

    private DefaultQuotaFlow(Builder builder) {
        this.policySets = new AtomicReference<>(builder.policySet);
        this.engine = new PolicyEngine(
                builder.store, builder.defaultResolver, builder.namedResolvers, builder.limitResolver);
        this.listeners = List.copyOf(builder.listeners);
        this.maxWaitersPerPolicy = builder.maxWaitersPerPolicy;
        this.asyncExecutor = builder.asyncExecutor != null ? builder.asyncExecutor : DefaultAsyncExecutor.get();
    }

    public static Builder builder(PolicySet policySet, RateLimitStore store) {
        return new Builder(policySet, store);
    }

    /** Atomically replaces the compiled policy set for subsequent decisions. */
    public void replacePolicySet(PolicySet policySet) {
        policySets.set(Objects.requireNonNull(policySet, "policySet"));
        configurationGeneration.incrementAndGet();
        queues.values().forEach(WaiterQueue::signalHead);
    }

    /** Current waiter count of the policy's throttle queue (0 when none exists). */
    public int waitQueueDepth(String policyId) {
        WaiterQueue queue = queues.get(policyId);
        return queue == null ? 0 : queue.size();
    }

    @Override
    public Decision tryAcquire(String policyId, RateLimitContext context) {
        return tryAcquire(policyId, context, 1);
    }

    @Override
    public Decision tryAcquire(String policyId, RateLimitContext context, long weight) {
        Evaluation evaluation = engine.evaluateInternal(policySets.get(), policyId, context, weight);
        notifyListeners(evaluation.decision(), evaluation.keyGroup());
        return evaluation.decision();
    }

    @Override
    public CompletionStage<Decision> tryAcquireAsync(String policyId, RateLimitContext context, long weight) {
        return engine
                .evaluateInternalAsync(policySets.get(), policyId, context, weight)
                .thenApply(evaluation -> {
                    notifyListeners(evaluation.decision(), evaluation.keyGroup());
                    return evaluation.decision();
                });
    }

    @Override
    public Decision acquire(String policyId, RateLimitContext context, long weight, Duration waitTimeout) {
        return acquireInternal(policyId, context, weight, waitTimeout, null);
    }

    @Override
    public Decision acquire(
            String policyId, RateLimitContext context, long weight, Duration waitTimeout, int priority) {
        return acquireInternal(policyId, context, weight, waitTimeout, priority);
    }

    @Override
    public CompletionStage<Decision> acquireAsync(
            String policyId, RateLimitContext context, long weight, Duration waitTimeout) {
        validateAcquireArgs(weight, waitTimeout);
        return CompletableFuture.supplyAsync(
                () -> acquire(policyId, context, weight, waitTimeout), asyncExecutor);
    }

    @Override
    public CompletionStage<Decision> acquireAsync(
            String policyId, RateLimitContext context, long weight, Duration waitTimeout, int priority) {
        validateAcquireArgs(weight, waitTimeout);
        return CompletableFuture.supplyAsync(
                () -> acquire(policyId, context, weight, waitTimeout, priority), asyncExecutor);
    }

    /**
     * The throttle retry loop. A rejected acquisition against a throttle-mode
     * policy with a refill schedule joins the policy's waiter queue and parks
     * until its wake time; only the queue head re-evaluates. Every loop
     * iteration re-reads the current policy set, so hot-reloaded limits and
     * swapped reaction modes govern mid-wait retries.
     */
    private Decision acquireInternal(
            String policyId, RateLimitContext context, long weight, Duration waitTimeout,
            Integer priority) {
        validateAcquireArgs(weight, waitTimeout);
        long startNanos = System.nanoTime();
        // wraparound-safe: the deadline is only ever used in nano differences
        long deadlineNanos = startNanos + waitTimeout.toNanos();
        WaiterQueue queue = null;
        WaiterQueue.Waiter waiter = null;
        Evaluation lastEvaluation = null;
        RateLimitPolicy lastPolicy = null;
        while (true) {
            PolicySet current = policySets.get();
            Evaluation evaluation;
            RateLimitPolicy policy;
            try {
                policy = current.policy(policyId);
                evaluation = engine.evaluateInternal(current, policyId, context, weight);
            } catch (PolicyConfigurationException e) {
                if (waiter == null) {
                    // unknown policy or broken chain configuration on the first
                    // attempt: caller misuse, exactly like tryAcquire
                    throw e;
                }
                // the policy (or its chain configuration) vanished mid-wait:
                // reject with a configuration-shaped decision (no refill schedule)
                return finish(queue, waiter,
                        Decision.rejectedWithoutSchedule(policyId, lastPolicy.scope()),
                        lastEvaluation, startNanos);
            }
            Decision decision = evaluation.decision();
            if (decision.isAllowed() || policy.reaction() == Reaction.REJECT) {
                return finish(queue, waiter, decision, evaluation, startNanos);
            }
            if (decision.retryAfter().isEmpty()) {
                // rejections without a refill schedule (missing key, unresolvable
                // limit) have nothing to wait for
                return finish(queue, waiter, decision, evaluation, startNanos);
            }
            lastEvaluation = evaluation;
            lastPolicy = policy;
            long now = System.nanoTime();
            if (now - deadlineNanos >= 0) {
                return finish(queue, waiter,
                        decision.withThrottleRejection(ThrottleRejection.WAIT_TIMEOUT),
                        evaluation, startNanos);
            }
            if (waiter == null) {
                queue = queues.computeIfAbsent(policyId, id -> new WaiterQueue(maxWaitersPerPolicy));
                waiter = new WaiterQueue.Waiter(
                        resolvePriority(priority, context, policy),
                        waiterSequences.getAndIncrement(),
                        deadlineNanos,
                        Thread.currentThread(),
                        wakeAt(now, decision),
                        configurationGeneration.get());
                if (!queue.offer(waiter)) {
                    return finish(null, null,
                            decision.withThrottleRejection(ThrottleRejection.QUEUE_OVERFLOW),
                            evaluation, startNanos);
                }
            } else {
                waiter.wakeAtNanos = wakeAt(now, decision);
                waiter.generation = configurationGeneration.get();
            }
            if (!parkUntilDue(queue, waiter)) {
                return finish(queue, waiter,
                        decision.withThrottleRejection(ThrottleRejection.WAIT_TIMEOUT),
                        evaluation, startNanos);
            }
        }
    }

    /**
     * Parks the calling thread until its waiter is due for a retry: it must be
     * the queue head and its wake time must have passed (or the configuration
     * changed since its last retry, in which case it re-evaluates immediately
     * against the new set). Non-head waiters park until their deadline and
     * rely on head-transition signals, which are never lost because
     * {@link LockSupport#unpark} permits persist until consumed.
     *
     * <p>An interrupted thread stops waiting early: the interrupt status is
     * preserved and the caller leaves the queue, which surfaces as a
     * wait-timeout rejection.
     *
     * @return {@code true} when the waiter may retry, {@code false} when its
     *         wait timeout expired (or the thread was interrupted) while queued
     */
    private boolean parkUntilDue(WaiterQueue queue, WaiterQueue.Waiter waiter) {
        while (true) {
            long now = System.nanoTime();
            long remaining = waiter.deadlineNanos - now;
            if (remaining <= 0) {
                return false;
            }
            long parkFor;
            if (queue.isHead(waiter)) {
                boolean configurationChanged = waiter.generation != configurationGeneration.get();
                long untilWake = waiter.wakeAtNanos - now;
                if (untilWake <= 0 || configurationChanged) {
                    return true;
                }
                parkFor = Math.min(untilWake, remaining);
            } else {
                parkFor = remaining;
            }
            LockSupport.parkNanos(parkFor);
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /** Wake time: retry-after de-correlated by &plusmn;10% jitter. */
    private static long wakeAt(long nowNanos, Decision rejection) {
        long retryNanos = rejection.retryAfter().orElseThrow().toNanos();
        double factor = 1.0 + ThreadLocalRandom.current().nextDouble(-JITTER, JITTER);
        return nowNanos + Math.max(1, (long) (retryNanos * factor));
    }

    /** Priority default chain: explicit argument, then the context attribute,
     * then the policy default, then zero. */
    private static int resolvePriority(Integer explicit, RateLimitContext context, RateLimitPolicy policy) {
        if (explicit != null) {
            return explicit;
        }
        Optional<Object> attribute = context.get(RateLimitContext.PRIORITY);
        if (attribute.isPresent()) {
            Object value = attribute.get();
            if (value instanceof Number number) {
                return number.intValue();
            }
            throw new IllegalArgumentException("context attribute '" + RateLimitContext.PRIORITY
                    + "' must be a Number, got " + value.getClass().getSimpleName());
        }
        return policy.priority();
    }

    private static void validateAcquireArgs(long weight, Duration waitTimeout) {
        if (weight < 1) {
            throw new IllegalArgumentException("weight must be >= 1, got " + weight);
        }
        Objects.requireNonNull(waitTimeout, "waitTimeout");
        if (waitTimeout.isNegative()) {
            throw new IllegalArgumentException("waitTimeout must not be negative, got " + waitTimeout);
        }
    }

    /**
     * Final decision path: the waiter leaves the queue (waking the next head),
     * the decision is stamped with the total wait duration and reported to the
     * listeners exactly once. Decisions that never joined a queue (instant
     * allows, reject-mode and schedule-less rejections, queue overflow) carry
     * a zero wait.
     */
    private Decision finish(WaiterQueue queue, WaiterQueue.Waiter waiter, Decision decision,
            Evaluation evaluation, long startNanos) {
        if (waiter == null) {
            notifyListeners(decision, evaluation.keyGroup());
            return decision;
        }
        queue.remove(waiter);
        long elapsedNanos = Math.max(0, System.nanoTime() - startNanos);
        Decision finalDecision = decision.withWait(Duration.ofNanos(elapsedNanos));
        notifyListeners(finalDecision, evaluation.keyGroup());
        return finalDecision;
    }

    private void notifyListeners(Decision decision, String keyGroup) {
        for (DecisionListener listener : listeners) {
            listener.onDecision(decision, keyGroup);
        }
    }

    /** Daemon threads for {@code acquireAsync}, created only when first used. */
    private static final class DefaultAsyncExecutor {
        private static final AtomicLong THREADS = new AtomicLong();
        private static volatile Executor instance;

        private static Executor get() {
            Executor executor = instance;
            if (executor == null) {
                synchronized (DefaultAsyncExecutor.class) {
                    executor = instance;
                    if (executor == null) {
                        executor = Executors.newCachedThreadPool(runnable -> {
                            Thread thread = new Thread(
                                    runnable, "quotaflow-acquire-" + THREADS.incrementAndGet());
                            thread.setDaemon(true);
                            return thread;
                        });
                        instance = executor;
                    }
                }
            }
            return executor;
        }
    }

    public static final class Builder {
        private final PolicySet policySet;
        private final RateLimitStore store;
        private KeyResolver defaultResolver = KeyResolvers.scopeBased();
        private final Map<String, KeyResolver> namedResolvers = new LinkedHashMap<>();
        private final List<DecisionListener> listeners = new ArrayList<>();
        private LimitResolver limitResolver;
        private int maxWaitersPerPolicy = 1000;
        private Executor asyncExecutor;

        private Builder(PolicySet policySet, RateLimitStore store) {
            this.policySet = Objects.requireNonNull(policySet, "policySet");
            this.store = Objects.requireNonNull(store, "store");
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

        /**
         * Executor running {@code acquireAsync} waits (default: a shared
         * cached pool of daemon threads). Async waiters consume one executor
         * thread each for the duration of their wait.
         */
        public Builder asyncExecutor(Executor executor) {
            this.asyncExecutor = Objects.requireNonNull(executor, "executor");
            return this;
        }

        public DefaultQuotaFlow build() {
            return new DefaultQuotaFlow(this);
        }
    }
}
