package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class LimitKeyTest {

    @Test
    void exposesRawKeyAndKeyGroup() {
        LimitKey key = new LimitKey("alice", "principal");
        assertEquals("alice", key.rawKey());
        assertEquals("principal", key.keyGroup());
    }

    @Test
    void rejectsBlankComponents() {
        assertThrows(IllegalArgumentException.class, () -> new LimitKey("", "g"));
        assertThrows(IllegalArgumentException.class, () -> new LimitKey(" ", "g"));
        assertThrows(IllegalArgumentException.class, () -> new LimitKey(null, "g"));
        assertThrows(IllegalArgumentException.class, () -> new LimitKey("k", ""));
        assertThrows(IllegalArgumentException.class, () -> new LimitKey("k", null));
    }

    @Test
    void recordEquality() {
        LimitKey a = new LimitKey("k", "g");
        LimitKey b = new LimitKey("k", "g");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, new LimitKey("k", "other"));
        assertTrue(a.toString().contains("k"));
    }
}
