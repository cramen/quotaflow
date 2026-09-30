package io.quotaflow.fallback;

import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/** Lazily reconnectable owned primary. An absent connection never fabricates a local deployment. */
public final class ReconnectingRecoveryPrimary implements RecoveryPrimary, AutoCloseable {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ReconnectingRecoveryPrimary.class);
    public record Connection(RecoveryPrimary primary, AutoCloseable resources) implements AutoCloseable {
        public Connection { Objects.requireNonNull(primary, "primary"); Objects.requireNonNull(resources, "resources"); }
        @Override public void close() {
            try { resources.close(); }
            catch (Exception failure) { log.warn("owned recovery resource close failed ({})", failure.getClass().getSimpleName()); }
        }
    }
    private static final Object CLOSED = new Object();
    private static final class Opening {
        final CompletableFuture<Connection> result = new CompletableFuture<>();
    }
    private final AtomicReference<Object> state = new AtomicReference<>();
    private final Supplier<Connection> factory;
    private final Duration connectTimeout;

    /** The factory runs on bounded compatibility workers and must itself bound resource creation. */
    public ReconnectingRecoveryPrimary(Supplier<Connection> factory, Duration connectTimeout) {
        this.factory = Objects.requireNonNull(factory, "factory");
        if (connectTimeout.isZero() || connectTimeout.isNegative() || connectTimeout.compareTo(Duration.ofHours(1)) > 0)
            throw new IllegalArgumentException("connect timeout must be positive and at most one hour");
        this.connectTimeout = connectTimeout;
    }
    private CompletionStage<Connection> connect() {
        while (true) {
            Object current = state.get();
            if (current instanceof Connection connected) return CompletableFuture.completedFuture(connected);
            if (current == CLOSED) return unavailable();
            if (current instanceof Opening opening) return opening.result.copy();
            Opening opening = new Opening();
            if (!state.compareAndSet(null, opening)) continue;
            RecoveryWork.call(() -> {
                Connection candidate = Objects.requireNonNull(factory.get(), "connection factory result");
                if (!state.compareAndSet(opening, candidate)) {
                    candidate.close(); return ReconnectingRecoveryPrimary.<Connection>unavailable();
                }
                opening.result.complete(candidate);
                return CompletableFuture.completedFuture(candidate);
            }, connectTimeout).whenComplete((connected, failure) -> {
                if (failure != null && state.compareAndSet(opening, null)) opening.result.completeExceptionally(failure);
            });
            return opening.result.copy();
        }
    }
    private <T> CompletionStage<T> connected(Function<RecoveryPrimary, CompletionStage<T>> operation) {
        Object current = state.get();
        if (!(current instanceof Connection connection)) return unavailable();
        return operation.apply(connection.primary());
    }
    private static <T> CompletableFuture<T> unavailable() {
        return CompletableFuture.failedFuture(new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED));
    }
    @Override public CompletionStage<Void> probe() { return connect().thenCompose(connection -> connection.primary().probe()); }
    @Override public CompletionStage<RecoverySession> enroll(RecoveryCohort cohort, String id, String nonce) {
        return connected(primary -> primary.enroll(cohort, id, nonce));
    }
    @Override public CompletionStage<RecoveryControlResult> attach(QuotaDomain domain, RecoverySession session) {
        return connected(primary -> primary.attach(domain, session));
    }
    @Override public CompletionStage<RecoveryControlResult> read(QuotaDomain domain, RecoverySession session) {
        return connected(primary -> primary.read(domain, session));
    }
    @Override public CompletionStage<RecoveryControlResult> begin(RecoveryContext context, RecoveryConfiguration configuration) {
        return connected(primary -> primary.begin(context, configuration));
    }
    @Override public CompletionStage<RecoveryControlResult> configure(RecoveryContext context, RecoveryConfiguration configuration) {
        return connected(primary -> primary.configure(context, configuration));
    }
    @Override public CompletionStage<RecoveryControlResult> join(RecoveryContext context) { return connected(primary -> primary.join(context)); }
    @Override public CompletionStage<RecoveryControlResult> ready(RecoveryContext context) { return connected(primary -> primary.ready(context)); }
    @Override public CompletionStage<RecoveryControlResult> abort(RecoveryContext context) { return connected(primary -> primary.abort(context)); }
    @Override public CompletionStage<ChainResult> acquire(RecoveryContext context, List<LevelRequest> chain, boolean guarded, RecoveryPending pending) {
        return connected(primary -> primary.acquire(context, chain, guarded, pending));
    }
    @Override public CompletionStage<Boolean> seed(RecoveryContext context, List<BucketState> buckets) {
        return connected(primary -> primary.seed(context, buckets));
    }
    @Override public CompletionStage<Void> registerPolicies(List<PolicyBinding> bindings) {
        return connected(primary -> primary.registerPolicies(bindings));
    }
    @Override public void close() {
        Object previous = state.getAndSet(CLOSED);
        if (previous instanceof Connection connection) connection.close();
        if (previous instanceof Opening opening) opening.result.completeExceptionally(new CancellationException("primary owner closed"));
    }
}
