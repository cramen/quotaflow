package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ModulePlaceholderTest {

    @Test
    void namesItself() {
        assertEquals("quotaflow-fallback", ModulePlaceholder.name());
    }
}
