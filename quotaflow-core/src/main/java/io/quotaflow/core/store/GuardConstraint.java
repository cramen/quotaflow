package io.quotaflow.core.store;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.util.Objects;

/** Fenced control-plane normalization. A null effective limit disables the share without a fake zero-capacity Limit. */
public record GuardConstraint(BucketIdentity key, Algorithm algorithm, Limit effectiveLimit, boolean resetSchedule) {
    public GuardConstraint { Objects.requireNonNull(key, "key"); Objects.requireNonNull(algorithm, "algorithm"); }
}
