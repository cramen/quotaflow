package io.quotaflow.spring;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AdvisorInitializationTest {
    @Test void pointcutAndAdviceDiscoveryDoNotCreateRuntimeDependencies() throws Throwable {
        var resolutions = new AtomicInteger();
        var marker = new IllegalStateException("runtime-only resolution");
        var advisor = RateLimitedAdvisor.deferred(() -> { resolutions.incrementAndGet(); throw marker; });
        assertNotNull(advisor.getPointcut());
        var advice = advisor.getAdvice();
        assertEquals(0, resolutions.get(), "AOT discovery must not bind policies or start recovery");
        assertSame(marker, assertThrows(IllegalStateException.class, () -> advice.invoke(null)));
        assertEquals(1, resolutions.get());
    }
}
