package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class LimitTest {

    @Test
    void validLimitExposesComponents() {
        Limit limit = new Limit(10, 2, Duration.ofSeconds(4));
        assertEquals(10, limit.capacity());
        assertEquals(2, limit.refillAmount());
        assertEquals(Duration.ofSeconds(4), limit.refillPeriod());
        assertEquals(2_000_000_000.0, limit.emissionIntervalNanos());
    }

    @Test
    void rejectsCapacityBelowOne() {
        assertThrows(IllegalArgumentException.class, () -> new Limit(0, 1, Duration.ofSeconds(1)));
    }

    @Test
    void rejectsRefillAmountBelowOne() {
        assertThrows(IllegalArgumentException.class, () -> new Limit(1, 0, Duration.ofSeconds(1)));
    }

    @Test
    void rejectsNonPositivePeriod() {
        assertThrows(IllegalArgumentException.class, () -> new Limit(1, 1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new Limit(1, 1, Duration.ofMillis(-1)));
    }

    @Test
    void rejectsNullPeriod() {
        assertThrows(NullPointerException.class, () -> new Limit(1, 1, null));
    }

    @Test
    void recordEquality() {
        Limit a = new Limit(1, 1, Duration.ofSeconds(1));
        Limit b = new Limit(1, 1, Duration.ofSeconds(1));
        Limit c = new Limit(2, 1, Duration.ofSeconds(1));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertEquals("Limit[capacity=1, refillAmount=1, refillPeriod=PT1S]", a.toString());
    }
}
