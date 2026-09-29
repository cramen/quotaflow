package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class NumericEnvelopeTest {
    @Test void boundaryAndConservativeQuantization() {
        assertEquals(142_857_143, new Limit(1, 7, Duration.ofSeconds(1)).emissionIntervalNanos());
        assertEquals(1_000, new Limit(Limit.MAX_TOKENS, Limit.MAX_TOKENS, Duration.ofSeconds(1000)).emissionIntervalNanos());
        assertEquals(Limit.MAX_HORIZON_NANOS, new Limit(1, 1, Duration.ofDays(32)).emissionIntervalNanos());
        assertEquals(1000, new Limit(1, 1, Duration.ofNanos(1000)).emissionIntervalNanos());
        Limit.validateWeight(Limit.MAX_TOKENS);
    }
    @Test void unsupportedValuesFailBeforeOverflow() {
        assertThrows(IllegalArgumentException.class, () -> new Limit(1_000_000_001L, 1, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new Limit(1, 1_000_000_001L, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new Limit(1, 1, Duration.ofNanos(999)));
        assertThrows(IllegalArgumentException.class, () -> new Limit(1, 1, Duration.ofDays(32).plusNanos(1)));
        assertThrows(IllegalArgumentException.class, () -> new Limit(1, 1, Duration.ofSeconds(Long.MAX_VALUE)));
        assertThrows(IllegalArgumentException.class, () -> new Limit(1, 2, Duration.ofNanos(1999)));
        assertThrows(IllegalArgumentException.class, () -> new Limit(2, 1, Duration.ofDays(32)));
        assertThrows(IllegalArgumentException.class, () -> Limit.validateWeight(0));
        assertThrows(IllegalArgumentException.class, () -> Limit.validateWeight(Limit.MAX_TOKENS + 1));
    }
}
