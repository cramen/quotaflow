package io.quotaflow.core;

/**
 * How an exhausted policy reacts. {@link #REJECT} returns the rejection
 * immediately. {@link #THROTTLE} lets the caller wait for quota instead of
 * being rejected: the wait (bounded priority queue, wait timeout) is
 * implemented by the {@code DefaultQuotaFlow} facade above the engine; the
 * engine itself evaluates both modes identically and returns pure decisions.
 */
public enum Reaction {
    REJECT,
    THROTTLE
}
