package io.quotaflow.core.store;

import java.util.Objects;

/** Budget indexed by the original atomic chain, never by a raw key. */
public record LevelBudget(int level, StoreBudget budget) {
    public LevelBudget {
        if (level < 0) throw new IllegalArgumentException("budget level must not be negative");
        Objects.requireNonNull(budget, "budget");
    }
}
