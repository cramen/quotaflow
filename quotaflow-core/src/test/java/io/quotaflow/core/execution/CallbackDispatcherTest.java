package io.quotaflow.core.execution;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CallbackDispatcherTest {
    @Test void invalidBoundsRejectBeforeAllocatingDeliveryResources() {
        assertThrows(IllegalArgumentException.class, () -> new CallbackDispatcher(0, Duration.ofSeconds(1), () -> {}));
        assertThrows(IllegalArgumentException.class, () -> new CallbackDispatcher(1, Duration.ZERO, () -> {}));
        assertThrows(IllegalArgumentException.class, () -> new CallbackDispatcher(1, Duration.ofSeconds(-1), () -> {}));
    }
    @Test void saturationDisablesOnlyTheOwnedLaneAndClosesAfterPhysicalWorkReturns() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var closed = new CountDownLatch(1);
        var delivered = new AtomicInteger();
        var lane = new CallbackDispatcher(1, Duration.ofSeconds(3), closed::countDown);
        try {
            var running = lane.submit(() -> { entered.countDown(); try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } });
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            var queued = lane.submit(delivered::incrementAndGet);
            lane.submit(delivered::incrementAndGet).get(1, TimeUnit.SECONDS);
            queued.get(1, TimeUnit.SECONDS);
            lane.submit(delivered::incrementAndGet).get(1, TimeUnit.SECONDS);
            assertEquals(1, lane.failures()); assertEquals(0, delivered.get());
            lane.closeAsync().get(1, TimeUnit.SECONDS);
            release.countDown(); running.get(1, TimeUnit.SECONDS);
            assertTrue(closed.await(1, TimeUnit.SECONDS));
            lane.close();
        } finally { release.countDown(); lane.close(); }
    }
    @Test void orderlyCloseAndCallbackFailuresRemainObservable() throws Exception {
        var closed = new AtomicInteger();
        var lane = new CallbackDispatcher(4, Duration.ofSeconds(2), () -> { closed.incrementAndGet(); throw new IllegalStateException("private-close-detail"); });
        lane.submit(() -> { throw new IllegalArgumentException("private-event-detail"); }).get(1, TimeUnit.SECONDS);
        lane.closeAsync().get(1, TimeUnit.SECONDS);
        lane.closeAsync().get(1, TimeUnit.SECONDS);
        lane.submit(() -> fail("closed lane invoked callback")).get(1, TimeUnit.SECONDS);
        assertEquals(1, closed.get()); assertEquals(2, lane.failures());
    }
}
