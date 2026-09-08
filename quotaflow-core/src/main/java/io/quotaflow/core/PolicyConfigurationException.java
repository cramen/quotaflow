package io.quotaflow.core;

/**
 * Thrown when a policy set fails compilation (cycles, unknown parents, invalid
 * limits, scope-order violations, unresolvable key-resolver references) or when
 * a caller requests a policy id absent from the compiled set. These are
 * configuration errors and caller misuse, never rate limit outcomes.
 */
public class PolicyConfigurationException extends IllegalArgumentException {

    public PolicyConfigurationException(String message) {
        super(message);
    }
}
