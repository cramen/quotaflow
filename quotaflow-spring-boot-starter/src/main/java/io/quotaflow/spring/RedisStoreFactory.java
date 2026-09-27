package io.quotaflow.spring;

import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;

/**
 * Isolates the store-creation path of the auto-configuration. This class must
 * only be touched after a presence check for {@code io.lettuce.core.RedisClient},
 * so applications without the driver never load it.
 *
 * <p>Client creation itself — including the guard against an incomplete netty
 * native-transport stack — lives in the store module
 * ({@link RedisRateLimitStore#connect}), so non-Spring consumers get the same
 * protection; the auto-configuration additionally catches {@link LinkageError}
 * as a last-resort safety net when falling back to local-only mode.
 */
final class RedisStoreFactory {

    private RedisStoreFactory() {
    }

    /** Opens a client and a dedicated connection; both are closed by the returned holder. */
    static PrimaryStoreHolder connect(QuotaFlowProperties.Redis properties) {
        RedisStoreConfig config = new RedisStoreConfig(
                properties.getCommandTimeout(), properties.getBusinessTimeout());
        RedisRateLimitStore store =
                RedisRateLimitStore.connect(properties.getUrl(), config, properties.getConnectTimeout());
        return new PrimaryStoreHolder(store, store, store::close);
    }
}
