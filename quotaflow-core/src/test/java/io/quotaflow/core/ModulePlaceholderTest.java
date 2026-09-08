package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ModulePlaceholderTest {

    @Test
    void describesItself() {
        assertEquals("quotaflow-core placeholder", ModulePlaceholder.describe(false));
        assertTrue(ModulePlaceholder.describe(true).contains("verbose"));
    }
}
