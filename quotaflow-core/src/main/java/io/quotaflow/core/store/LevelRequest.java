package io.quotaflow.core.store;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.util.Objects;
import java.util.List;

/**
 * One level of a policy chain as evaluated by a {@link BatchRateLimitStore}:
 * the engine-resolved storage key, the level's limit and algorithm, and the
 * requested weight. Levels are ordered root-to-leaf (broadest scope first).
 * Configuration fingerprint and resolver identity describe one common captured
 * view for the complete canonical root domain, including static ancestors.
 */
public record LevelRequest(BucketIdentity storageKey, Limit limit, Algorithm algorithm, long weight, String keyGroup, String configurationFingerprint,
                           long resolverRevision, String resolverFingerprint) {

    public LevelRequest(BucketIdentity storageKey, Limit limit, Algorithm algorithm, long weight, String keyGroup, String configurationFingerprint) {
        this(storageKey, limit, algorithm, weight, keyGroup, configurationFingerprint, 0, io.quotaflow.core.LimitSnapshot.NONE_FINGERPRINT);
    }

    public LevelRequest(BucketIdentity storageKey, Limit limit, Algorithm algorithm, long weight) {
        this(storageKey, limit, algorithm, weight, storageKey.scope().wireName(), null);
    }

    public LevelRequest(BucketIdentity storageKey, Limit limit, Algorithm algorithm, long weight, String keyGroup) {
        this(storageKey, limit, algorithm, weight, keyGroup, null);
    }

    public LevelRequest {
        RecoveryConfiguration.validateResolver(resolverRevision, resolverFingerprint);
        Objects.requireNonNull(storageKey, "storageKey");
        Objects.requireNonNull(limit, "limit");
        Objects.requireNonNull(algorithm, "algorithm");
        Objects.requireNonNull(keyGroup, "keyGroup");
        Limit.validateWeight(weight);
    }
    /** Validates an atomic batch before registration or any quota mutation. */
    public static void validateChain(List<LevelRequest> chain) {
        Objects.requireNonNull(chain, "chain");
        if (chain.isEmpty()) throw new IllegalArgumentException("chain must not be empty");
        LevelRequest first = chain.get(0);
        QuotaDomain domain = first.storageKey().domain();
        for (int i = 0; i < chain.size(); i++) {
            LevelRequest current = chain.get(i);
            if (current.resolverRevision() != first.resolverRevision() || !current.resolverFingerprint().equals(first.resolverFingerprint())
                    || !Objects.equals(current.configurationFingerprint(), first.configurationFingerprint()))
                throw new IllegalArgumentException("atomic chain must use one captured configuration and resolver snapshot");
            BucketIdentity identity = current.storageKey();
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
