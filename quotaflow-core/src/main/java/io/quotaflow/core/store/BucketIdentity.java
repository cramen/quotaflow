package io.quotaflow.core.store;

import io.quotaflow.core.Scope;
import java.util.Objects;

/**
 * Canonical quota identity, independent of the requesting leaf or recovery epoch.
 * The raw key participates in equality, never in telemetry aggregation.
 */
public record BucketIdentity(QuotaDomain domain, String policyId, Scope scope, String rawKey) {

    public BucketIdentity {
        Objects.requireNonNull(domain, "domain");
        QuotaDomain.requireIdentity(policyId, "policyId");
        Objects.requireNonNull(scope, "scope");
        QuotaDomain.requireIdentity(rawKey, "rawKey");
    }

    /** Avoid accidental raw-key disclosure by logs, collections and exceptions. */
    @Override
    public String toString() {
        return "BucketIdentity[policyId=" + policyId + ", scope=" + scope + ", rawKey=<redacted>]";
    }
}
