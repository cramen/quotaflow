package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.Limit;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Random;
import org.junit.jupiter.api.Test;

class DegradedShareTest {
    @Test void dividesBurstAndFractionalRefillWithoutRoundingUp() {
        var share = DegradedShare.of(new Limit(100, 1, Duration.ofSeconds(1)), 4);
        assertEquals(25, share.capacity());
        assertEquals(4_000_000_000L, share.localLimit().orElseThrow().emissionIntervalNanos());
        assertTrue(share.canFit(25));
        assertFalse(share.canFit(26));
    }
    @Test void fractionalSchedulesAndMaximumHorizonStayConservative() {
        var fractional = DegradedShare.of(new Limit(100, 7, Duration.ofSeconds(1)), 4);
        assertEquals(571_428_572L, fractional.localLimit().orElseThrow().emissionIntervalNanos());
        var maximum = DegradedShare.of(new Limit(1_000_000_000, 1_000_000_000, Duration.ofDays(32)), 1_000_000_000);
        assertEquals(1, maximum.capacity());
        assertEquals(Limit.MAX_HORIZON_NANOS, maximum.localLimit().orElseThrow().emissionIntervalNanos());
    }

    @Test void zeroBurstAndLargestInstanceCountDoNotCreateFakeLimits() {
        var share = DegradedShare.of(new Limit(1, 1, Duration.ofDays(32)), Integer.MAX_VALUE);
        assertEquals(0, share.capacity());
        assertTrue(share.localLimit().isEmpty());
        assertFalse(share.canFit(1));
        assertThrows(IllegalArgumentException.class, () -> share.canFit(0));
        assertThrows(IllegalArgumentException.class, () -> DegradedShare.of(new Limit(1, 1, Duration.ofSeconds(1)), 0));
    }
    @Test void generatedSharesRespectRationalFleetEnvelopeAndNumericHorizon() {
        Random random = new Random(20260929);
        for (int i = 0; i < 10_000; i++) {
            long capacity = random.nextInt(1_000_000_000) + 1L;
            long nanos = 1_000_000_000L;
            long refill = 1_000_000;
            Limit full = new Limit(capacity, refill, Duration.ofNanos(nanos));
            int n = random.nextInt(Integer.MAX_VALUE) + 1;
            DegradedShare share = DegradedShare.of(full, n);
            assertTrue(BigInteger.valueOf(share.capacity()).multiply(BigInteger.valueOf(n)).compareTo(BigInteger.valueOf(capacity)) <= 0);
            if (share.localLimit().isPresent()) {
                Limit local = share.localLimit().orElseThrow();
                BigInteger lhs = BigInteger.valueOf(n).multiply(BigInteger.valueOf(nanos));
                BigInteger rhs = BigInteger.valueOf(refill).multiply(BigInteger.valueOf(local.emissionIntervalNanos()));
                assertTrue(lhs.compareTo(rhs) <= 0, "sum of refill rates must not exceed the distributed rate");
                assertTrue(BigInteger.valueOf(local.capacity()).multiply(BigInteger.valueOf(local.emissionIntervalNanos()))
                        .compareTo(BigInteger.valueOf(Limit.MAX_HORIZON_NANOS)) <= 0);
            }
        }
    }
}
