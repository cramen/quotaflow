package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class RedisStoreConfigTest {

    @Test
    void defaultsAreOrdered() {
        RedisStoreConfig config = RedisStoreConfig.defaults();
        assertTrue(config.commandTimeout().compareTo(config.businessTimeout()) < 0);
    }

    @Test
    void storeTimeoutEqualToBusinessTimeoutFailsFast() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new RedisStoreConfig(Duration.ofMillis(500), Duration.ofMillis(500)));
        assertTrue(e.getMessage().contains("strictly below"));
        assertTrue(e.getMessage().contains(Duration.ofMillis(500).toString()));
    }

    @Test
    void storeTimeoutAboveBusinessTimeoutFailsFast() {
        assertThrows(IllegalArgumentException.class,
                () -> new RedisStoreConfig(Duration.ofSeconds(2), Duration.ofSeconds(1)));
    }

    @Test
    void nonPositiveTimeoutsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new RedisStoreConfig(Duration.ZERO, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
                () -> new RedisStoreConfig(Duration.ofMillis(1), Duration.ofMillis(-1)));
    }

    @Test
    void nullsRejected() {
        assertThrows(NullPointerException.class,
                () -> new RedisStoreConfig(null, Duration.ofSeconds(1)));
        assertThrows(NullPointerException.class,
                () -> new RedisStoreConfig(Duration.ofMillis(1), null));
    }

    @Test
    void validCustomConfigAccepted() {
        RedisStoreConfig config = new RedisStoreConfig(Duration.ofMillis(50), Duration.ofMillis(200));
        assertEquals(Duration.ofMillis(50), config.commandTimeout());
        assertEquals(Duration.ofMillis(200), config.businessTimeout());
    }
}
