package io.quotaflow.core;

/** One successful queue admission. Separate from terminal decisions; callbacks must be fast and nonblocking. */
@FunctionalInterface
public interface WaitListener {
    /** Requested leaf queue identity and the bounded blocking-level group captured at first enqueue. */
    void onQueued(String queuePolicyId, String keyGroup);
}
