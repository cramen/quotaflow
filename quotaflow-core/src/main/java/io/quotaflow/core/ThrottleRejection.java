package io.quotaflow.core;

/**
 * Why a throttled acquisition was finally rejected without being served.
 * Present only on rejections produced by the throttle wait machinery
 * ({@link QuotaFlow#acquire}); ordinary quota rejections carry no throttle
 * rejection reason.
 */
public enum ThrottleRejection {

    /** The caller's wait timeout expired before quota became available. */
    WAIT_TIMEOUT,

    /** The policy's waiter queue was full when the caller tried to enqueue. */
    QUEUE_OVERFLOW
}
