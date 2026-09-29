package io.quotaflow.core.store;

import io.quotaflow.core.Scope;
import io.quotaflow.core.Algorithm;
import java.util.Objects;

/** Immutable algorithm, scope and tree placement of a policy within its deployment namespace. */
public record PolicyBinding(QuotaDomain domain, String policyId, Scope scope, Algorithm algorithm) {
    public PolicyBinding {
        Objects.requireNonNull(domain, "domain");
        QuotaDomain.requireIdentity(policyId, "policyId");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(algorithm, "algorithm");
    }

    public static PolicyBinding of(BucketIdentity bucket, Algorithm algorithm) {
        return new PolicyBinding(bucket.domain(), bucket.policyId(), bucket.scope(), algorithm);
    }
}
