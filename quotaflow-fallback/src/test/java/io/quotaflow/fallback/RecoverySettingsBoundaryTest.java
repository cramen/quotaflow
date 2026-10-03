package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.store.RecoveryCohort;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RecoverySettingsBoundaryTest {
    @Test void declaredDurationRangeIncludesBothEndpoints() {
        assertDoesNotThrow(() -> settings(Duration.ofNanos(1), Duration.ofHours(1)));
        assertDoesNotThrow(() -> settings(Duration.ofHours(1), Duration.ofNanos(1)));
        for (Duration invalid : new Duration[]{Duration.ZERO, Duration.ofNanos(-1), Duration.ofHours(1).plusNanos(1)}) {
            assertThrows(IllegalArgumentException.class, () -> settings(invalid, Duration.ofSeconds(1)));
            assertThrows(IllegalArgumentException.class, () -> settings(Duration.ofSeconds(1), invalid));
        }
    }
    @Test void lazyConnectionAcceptsTheMaximumTimeoutWithoutOpeningResources() {
        try (var primary = new ReconnectingRecoveryPrimary(() -> { fail("construction must not connect"); return null; }, Duration.ofHours(1))) {
            assertNotNull(primary);
        }
        assertThrows(IllegalArgumentException.class, () -> new ReconnectingRecoveryPrimary(() -> null, Duration.ofHours(1).plusNanos(1)));
    }
    private static RecoverySettings settings(Duration interval, Duration timeout) {
        return new RecoverySettings("default", "test", "single", RecoveryCohort.single(), 1, 1, interval, timeout);
    }
}
