package io.quotaflow.core.observation;

/** Opens one owned observation session per limiter. No network or tariff lookup belongs in callbacks. */
@FunctionalInterface
public interface ObservationListener {
    ObservationSession open(ObservationConfiguration initial);
}
