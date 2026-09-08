package io.quotaflow.core.store;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A {@link RateLimitStore} that can evaluate a whole policy chain atomically
 * in one store-side execution: tokens are deducted at every level only if all
 * levels admit the request; a rejection at any level consumes nothing at any
 * level.
 *
 * <p>The engine prefers this batch path when the store implements it — one
 * round-trip regardless of chain depth and no over-accounting at parent
 * levels. Stores that cannot batch keep serving the per-level
 * {@link #tryAcquire} contract, which the engine falls back to with the
 * documented bounded over-accounting trade-off.
 */
public interface BatchRateLimitStore extends RateLimitStore {

    /**
     * Atomically evaluates {@code chain} (root-to-leaf) and consumes each
     * level's weight at every level if all levels admit the request; consumes
     * nothing anywhere otherwise.
     *
     * @param chain levels ordered root-to-leaf; must be non-empty
     * @return the chain outcome; see {@link ChainResult} for the exact
     *         remaining/retry-after semantics
     */
    CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain);
}
