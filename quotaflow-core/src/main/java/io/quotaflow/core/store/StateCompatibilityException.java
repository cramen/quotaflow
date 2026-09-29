package io.quotaflow.core.store;

import io.quotaflow.core.PolicyConfigurationException;

/** Incompatible or corrupt quota state; never a transient outage authorizing fallback. */
public final class StateCompatibilityException extends PolicyConfigurationException {
    public StateCompatibilityException(String message) { super(message); }
}
