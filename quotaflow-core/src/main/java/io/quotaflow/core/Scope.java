package io.quotaflow.core;

import java.util.Locale;

/**
 * Breadth of a policy level. Declared broadest-first: a parent scope must be
 * strictly broader (lower ordinal) than its child's scope.
 */
public enum Scope {
    GLOBAL,
    TENANT,
    USER,
    KEY;

    /** Lowercase wire form used in storage keys. */
    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** Whether this scope is strictly broader than {@code other}. */
    public boolean isBroaderThan(Scope other) {
        return ordinal() < other.ordinal();
    }
}
