package io.quotaflow.testing;

import io.quotaflow.core.Scope;
import io.quotaflow.core.store.BucketIdentity;
import io.quotaflow.core.store.QuotaDomain;
import java.util.Locale;

/** Explicit test deployment/domain shared by manually assembled fixture chains. */
public final class TestIdentities {
    public static final QuotaDomain DOMAIN = new QuotaDomain("default", "test-root");
    private TestIdentities() { }
    public static BucketIdentity key(BucketIdentity key) { return key; }
    public static BucketIdentity key(String legacyFixture) {
        if (legacyFixture == null) return null;
        String[] fields = legacyFixture.split(":", 3);
        if (fields.length == 3) {
            try {
                return new BucketIdentity(DOMAIN, fields[0],
                        Scope.valueOf(fields[1].toUpperCase(Locale.ROOT)), fields[2]);
            } catch (IllegalArgumentException unknownFixtureScope) {
                return new BucketIdentity(DOMAIN, fields[0], Scope.KEY, fields[1] + ":" + fields[2]);
            }
        }
        return new BucketIdentity(DOMAIN, "fixture", Scope.KEY, legacyFixture);
    }
}
