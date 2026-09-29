package io.quotaflow.core.store;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.util.Objects;
import java.util.List;

/**
 * One level of a policy chain as evaluated by a {@link BatchRateLimitStore}:
 * the engine-resolved storage key, the level's limit and algorithm, and the
 * requested weight. Levels are ordered root-to-leaf (broadest scope first).
 */
public record LevelRequest(BucketIdentity storageKey, Limit limit, Algorithm algorithm, long weight) {

    public LevelRequest {
        Objects.requireNonNull(storageKey, "storageKey");
        Objects.requireNonNull(limit, "limit");
        Objects.requireNonNull(algorithm, "algorithm");
        Limit.validateWeight(weight);
    }
    /** Validates an atomic batch before registration or any quota mutation. */
    public static void validateChain(List<LevelRequest> chain) {
        Objects.requireNonNull(chain, "chain");
        if (chain.isEmpty()) throw new IllegalArgumentException("chain must not be empty");
        QuotaDomain domain = chain.get(0).storageKey().domain();
        for (int i = 0; i < chain.size(); i++) {
            BucketIdentity identity = chain.get(i).storageKey();
            if (!domain.equals(identity.domain())) {
                throw new IllegalArgumentException("atomic chain must use one quota domain");
            }
            for (int j = 0; j < i; j++) {
                if (identity.equals(chain.get(j).storageKey())) {
                    throw new IllegalArgumentException("atomic chain must not repeat a canonical bucket");
                }
            }
        }
    }

}
