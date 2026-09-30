package io.quotaflow.tck;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class CanonicalHierarchyTckTest extends TckContainers {
    @Test void weightedSharedHierarchyOnRedis() throws Exception { verify(redisUri()); }
    @Test void weightedSharedHierarchyOnValkey() throws Exception { verify(valkeyUri()); }
    @Test void weightedSharedHierarchyOnLocalStore() throws Exception {
        for (Algorithm algorithm : Algorithm.values()) HierarchyConformance.verify(new LocalRateLimitStore(), algorithm);
    }
    private void verify(String uri) throws Exception {
        try (var store = io.quotaflow.testing.RecoveryStoreFixture.create(newClient(uri),
                new RedisStoreConfig(Duration.ofSeconds(2), Duration.ofSeconds(4)))) {
            for (Algorithm algorithm : Algorithm.values()) HierarchyConformance.verify(store, algorithm);
        }
    }
}
