package io.quotaflow.core.store;

/**
 * Outcome of one atomic chain evaluation by a {@link BatchRateLimitStore}.
 *
 * <p>{@code firedLevelIndex} is the index into the evaluated level list of the
 * level whose verdict the result reports: the first rejecting level, or the
 * last (leaf) level when the chain allowed the request.
 *
 * <p>{@code remaining} mirrors the per-level {@link StoreResult} contract so
 * decisions read identically on both evaluation paths: on rejection it is the
 * fired level's remaining capacity; when the chain allowed the request it is
 * the minimum remaining capacity across all levels.
 *
 * <p>{@code retryAfterMillis} is the positive delay after which the fired
 * level would admit the request; it is undefined (zero) when {@code acquired}
 * is true.
 */
public record ChainResult(boolean acquired, int firedLevelIndex, long remaining, long retryAfterMillis, RecoveryPending recoveryPending, java.util.List<LevelBudget> budgets) {

    public ChainResult(boolean acquired, int firedLevelIndex, long remaining, long retryAfterMillis, RecoveryPending pending) {
        this(acquired, firedLevelIndex, remaining, retryAfterMillis, pending, java.util.List.of());
    }
    public ChainResult withBudgets(java.util.List<LevelBudget> budgets) {
        return new ChainResult(acquired, firedLevelIndex, remaining, retryAfterMillis, recoveryPending, budgets);
    }
    public StoreResult singleResult() {
        StoreBudget budget = budgets.stream().filter(sample -> sample.level() == 0).map(LevelBudget::budget).findFirst().orElse(null);
        return new StoreResult(acquired, remaining, retryAfterMillis, recoveryPending, budget);
    }
    public ChainResult asDegraded() {
        return withBudgets(budgets.stream().map(sample -> new LevelBudget(sample.level(), sample.budget().asDegraded())).toList());
    }

    public ChainResult(boolean acquired, int firedLevelIndex, long remaining, long retryAfterMillis) {
        this(acquired, firedLevelIndex, remaining, retryAfterMillis, null);
    }

    public ChainResult {
        budgets = java.util.List.copyOf(budgets);
        java.util.HashSet<Integer> levels = new java.util.HashSet<>();
        for (LevelBudget budget : budgets) if (!levels.add(budget.level())) throw new IllegalArgumentException("duplicate budget level");
        if (recoveryPending != null && (acquired || remaining != 0 || retryAfterMillis != 0 || !budgets.isEmpty())) {
            throw new IllegalArgumentException("recovery-pending outcome cannot grant quota or a refill schedule");
        }
    }

    public static ChainResult pending(int firedLevelIndex, RecoveryPending pending) {
        return new ChainResult(false, firedLevelIndex, 0, 0, java.util.Objects.requireNonNull(pending, "pending"));
    }

    public static ChainResult acquired(int firedLevelIndex, long minRemaining) {
        return new ChainResult(true, firedLevelIndex, minRemaining, 0);
    }

    public static ChainResult rejected(int firedLevelIndex, long remaining, long retryAfterMillis) {
        return new ChainResult(false, firedLevelIndex, remaining, retryAfterMillis);
    }
}
