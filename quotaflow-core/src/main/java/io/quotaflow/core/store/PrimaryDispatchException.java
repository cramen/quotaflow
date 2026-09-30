package io.quotaflow.core.store;

/** Explicit transport outcome. Cancellation or a timeout is never proof that an operation was not dispatched. */
public final class PrimaryDispatchException extends RuntimeException {
    public enum Outcome { NOT_DISPATCHED, UNCERTAIN }
    private final Outcome outcome;
    public PrimaryDispatchException(Outcome outcome) {
        super(outcome == Outcome.NOT_DISPATCHED ? "primary operation was not dispatched" : "primary operation outcome is uncertain");
        this.outcome = java.util.Objects.requireNonNull(outcome, "outcome");
    }
    public Outcome outcome() { return outcome; }
}
