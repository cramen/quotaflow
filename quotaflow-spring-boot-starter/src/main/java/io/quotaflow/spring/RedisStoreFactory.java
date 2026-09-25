package io.quotaflow.spring;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SocketOptions;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;

/**
 * Isolates every direct Lettuce reference of the auto-configuration. This
 * class must only be touched after a presence check for
 * {@code io.lettuce.core.RedisClient}, so applications without the driver
 * never load it.
 */
final class RedisStoreFactory {

    private RedisStoreFactory() {
    }

    /** Opens a client and a dedicated connection; both are closed by the returned holder. */
    static PrimaryStoreHolder connect(QuotaFlowProperties.Redis properties) {
        RedisClient client = RedisClient.create(properties.getUrl());
        try {
            client.setOptions(ClientOptions.builder()
                    .socketOptions(SocketOptions.builder()
                            .connectTimeout(properties.getConnectTimeout())
                            .build())
                    .build());
            RedisStoreConfig config = new RedisStoreConfig(
                    properties.getCommandTimeout(), properties.getBusinessTimeout());
            RedisRateLimitStore store = RedisRateLimitStore.create(client, config);
            return new PrimaryStoreHolder(store, store, () -> {
                store.close();
                client.shutdown();
            });
        } catch (RuntimeException e) {
            client.shutdown();
            throw e;
        }
    }
}
