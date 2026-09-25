package io.quotaflow.tck;

import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.RateLimitStore;
import org.junit.jupiter.api.Test;

/**
 * Throttle conformance against the in-memory store (fast timing profile).
 */
class ThrottleLocalTckTest {

    private final RateLimitStore store = new LocalRateLimitStore();
    private final ThrottleConformance.Profile profile = ThrottleConformance.Profile.local();

    @Test
    void oversubscribedPolicyServesAllWaiters() throws Exception {
        ThrottleConformance.oversubscribedPolicyServesAllWaiters(
                store, TckContainers.uniqueKey("throttle:oversubscribed:local"), profile);
    }

    @Test
    void queueOverflowRejectsImmediately() throws Exception {
        ThrottleConformance.queueOverflowRejectsImmediately(
                store, TckContainers.uniqueKey("throttle:overflow:local"), profile);
    }

    @Test
    void highPriorityWaiterServedFirst() throws Exception {
        ThrottleConformance.highPriorityWaiterServedFirst(
                store, TckContainers.uniqueKey("throttle:priority:local"), profile);
    }

    @Test
    void waitTimeoutRejects() throws Exception {
        ThrottleConformance.waitTimeoutRejects(
                store, TckContainers.uniqueKey("throttle:timeout:local"), profile);
    }
}
