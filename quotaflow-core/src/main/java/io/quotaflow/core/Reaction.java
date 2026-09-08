package io.quotaflow.core;

/**
 * How an exhausted policy reacts. {@link #THROTTLE} is reserved: the policy
 * engine currently evaluates throttle policies exactly like {@link #REJECT}
 * until queue semantics land.
 */
public enum Reaction {
    REJECT,
    THROTTLE
}
