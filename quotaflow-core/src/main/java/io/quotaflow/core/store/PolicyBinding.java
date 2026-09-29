package io.quotaflow.core.store;

import io.quotaflow.core.Scope;
import java.util.Objects;

/** Immutable scope and tree placement of a policy within its deployment namespace. */
public record PolicyBinding(QuotaDomain domain, String policyId, Scope scope) {
    public PolicyBinding {
        Objects.requireNonNull(domain, "domain");
        QuotaDomain.requireIdentity(policyId, "policyId");
        Objects.requireNonNull(scope, "scope");
    }

    public static PolicyBinding of(BucketIdentity bucket) {
        return new PolicyBinding(bucket.domain(), bucket.policyId(), bucket.scope());
    }
}
