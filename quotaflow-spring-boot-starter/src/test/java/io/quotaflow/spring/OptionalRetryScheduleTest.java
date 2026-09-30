package io.quotaflow.spring;

import io.quotaflow.core.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OptionalRetryScheduleTest {
    @Test void onlySafePositiveSchedulesAreRoundedUpWithoutOverflow() {
        for (Duration duration : new Duration[]{Duration.ZERO, Duration.ofNanos(-1), Duration.ofSeconds(Long.MAX_VALUE,1)})
            assertTrue(RateLimitProblem.of(Decision.rejected("p",Scope.GLOBAL,0,duration)).retryAfterSeconds().isEmpty());
        assertEquals(1,RateLimitProblem.of(Decision.rejected("p",Scope.GLOBAL,0,Duration.ofNanos(1))).retryAfterSeconds().orElseThrow());
        assertEquals(2,RateLimitProblem.of(Decision.rejected("p",Scope.GLOBAL,0,Duration.ofMillis(1001))).retryAfterSeconds().orElseThrow());
        assertEquals(Long.MAX_VALUE,RateLimitProblem.of(Decision.rejected("p",Scope.GLOBAL,0,Duration.ofSeconds(Long.MAX_VALUE))).retryAfterSeconds().orElseThrow());
        for(var reason:ThrottleRejection.values()) {
            var problem=RateLimitProblem.of(Decision.rejectedWithoutSchedule("p",Scope.USER).withThrottleRejection(reason));
            assertTrue(problem.retryAfterSeconds().isEmpty());assertEquals(429,problem.status());
        }
    }
}
