package io.quotaflow.kotlin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ModulePlaceholderTest {

    @Test
    void namesItself() {
        assertEquals("quotaflow-kotlin", ModulePlaceholder.name());
    }
}
