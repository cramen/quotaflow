package io.quotaflow.core;

import io.quotaflow.core.observation.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/** Bounded, ordered delivery per listener. A broken listener cannot occupy another listener's lane. */
final class ObservationDispatcher {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ObservationDispatcher.class);
    private final List<Channel> channels = new ArrayList<>();
    private final java.util.concurrent.atomic.LongAdder failures = new java.util.concurrent.atomic.LongAdder();
    long failures() { return failures.sum() + channels.stream().mapToLong(channel -> channel.delivery.failures()).sum(); }
    ObservationDispatcher(List<DecisionListener> decisions, List<WaitListener> waits,
            List<ObservationListener> observers, ObservationConfiguration initial, int capacity, Duration timeout) {
        var flags = new IdentityHashMap<Object,Integer>();
        decisions.forEach(value -> flags.merge(value, 1, (a,b)->a|b));
        waits.forEach(value -> flags.merge(value, 2, (a,b)->a|b));
        flags.forEach((listener, kind) -> channels.add(new Channel(new ObservationSession() {
            @Override public void onDecision(long generation, Decision decision, String group) {
                if ((kind & 1) != 0) ((DecisionListener)listener).onDecision(decision, group);
            }
            @Override public void onQueued(long generation, String policy, String group) {
                if ((kind & 2) != 0) ((WaitListener)listener).onQueued(policy, group);
            }
        }, false, capacity, timeout)));
        for (ObservationListener observer : observers) {
            try { channels.add(new Channel(Objects.requireNonNull(observer.open(initial), "observation session"), true, capacity, timeout)); }
            catch (RuntimeException failure) { failures.increment(); failed(failure); }
        }
    }
    void configuration(ObservationConfiguration configuration) { send(true, session -> session.onConfiguration(configuration)); }
    void budget(BudgetObservation observation) { send(true, session -> session.onBudget(observation)); }
    void queued(long generation, String policy, String group) { send(false, session -> session.onQueued(generation, policy, group)); }
    CompletionStage<Void> decision(long generation, Decision decision, String group) {
        return send(false, session -> session.onDecision(generation, decision, group));
    }
    void retired(long generation) { send(true, session -> session.onRetired(generation)); }
    private CompletionStage<Void> send(boolean contextualOnly, Consumer<ObservationSession> callback) {
        return CompletableFuture.allOf(channels.stream().filter(channel -> !contextualOnly || channel.contextual)
                .map(channel -> channel.send(callback)).toArray(CompletableFuture[]::new));
    }
    CompletionStage<Void> barrier() {
        return CompletableFuture.allOf(channels.stream().map(Channel::barrier).toArray(CompletableFuture[]::new));
    }
    CompletionStage<Void> close() {
        return CompletableFuture.allOf(channels.stream().map(Channel::close).toArray(CompletableFuture[]::new));
    }
    static void failed(RuntimeException failure) { LOG.warn("quota listener failed ({})", failure.getClass().getSimpleName()); }

    private static final class Channel {
        final ObservationSession session;
        final boolean contextual;
        final io.quotaflow.core.execution.CallbackDispatcher delivery;
        Channel(ObservationSession session, boolean contextual, int capacity, Duration timeout) {
            this.session = session; this.contextual = contextual;
            delivery = new io.quotaflow.core.execution.CallbackDispatcher(capacity, timeout, session::close);
        }
        CompletableFuture<Void> send(Consumer<ObservationSession> callback) { return delivery.submit(() -> callback.accept(session)); }
        CompletableFuture<Void> barrier() { return delivery.barrier(); }
        CompletableFuture<Void> close() { return delivery.closeAsync(); }
    }
}
