package io.quotaflow.micrometer;

/** Frozen metric names and tag keys of the limiter's metric contract. */
public final class QuotaFlowMetrics {

    /** Counter of limiter decisions, tagged by result, policy and key-group. */
    public static final String DECISIONS = "quotaflow.decisions";

    /** Last-value gauge (0..1) of consumed capacity per policy and key-group. */
    public static final String UTILIZATION = "quotaflow.utilization";

    /** Gauge holding 1 while the store is degraded, 0 while healthy. */
    public static final String DEGRADED = "quotaflow.degraded";

    /** Counter of decisions served by the local fallback while degraded. */
    public static final String FALLBACK_DECISIONS = "quotaflow.fallback.decisions";

    /** Histogram of time spent waiting in throttle queues (zero-wait included). */
    public static final String WAIT_DURATION = "quotaflow.wait.duration";

    /** Gauge of throttle waiter queue depth per policy. */
    public static final String WAIT_QUEUE_DEPTH = "quotaflow.wait.queue.depth";

    /** Counter of throttle waiters rejected because their wait timeout expired. */
    public static final String WAIT_TIMEOUTS = "quotaflow.wait.timeouts";

    public static final String TAG_RESULT = "result";
    public static final String TAG_POLICY = "policy";
    public static final String TAG_KEY_GROUP = "key-group";

    public static final String RESULT_ALLOW = "allow";
    public static final String RESULT_REJECT = "reject";

    private QuotaFlowMetrics() {
    }
}
