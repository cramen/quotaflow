package io.quotaflow.core.store;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** A generation-scoped request to re-evaluate later, never a quota grant or refill promise. */
public final class RecoveryPending {
    private final QuotaDomain domain;
    private final long generation;
    private final CompletableFuture<Void> signal;

    public RecoveryPending(QuotaDomain domain, long generation, CompletionStage<Void> readiness) {
        this.domain = Objects.requireNonNull(domain, "domain");
        if (generation < 0) throw new IllegalArgumentException("recovery generation must not be negative");
        this.generation = generation;
        this.signal = Objects.requireNonNull(readiness, "readiness").toCompletableFuture().copy();
    }

    public QuotaDomain domain() { return domain; }
    public long generation() { return generation; }
    /** Each caller owns a detached view; cancellation cannot affect the coordinator or other callers. */
    public CompletionStage<Void> readiness() { return signal.copy(); }
}
