package io.quotaflow.tck;

import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;
import org.junit.jupiter.api.Test;

class NumericConformanceTckTest extends TckContainers {
    @Test void localNumericContract() { NumericConformance.verify(new LocalRateLimitStore()); }
    @Test void redisNumericContract() { verify(redisUri()); }
    @Test void valkeyNumericContract() { verify(valkeyUri()); }
    private void verify(String uri) {
        try (var connection = newClient(uri).connect();
                var store = new io.quotaflow.testing.RecoveryStoreFixture(connection, RedisStoreConfig.defaults())) {
            NumericConformance.verify(store);
        }
    }
}
