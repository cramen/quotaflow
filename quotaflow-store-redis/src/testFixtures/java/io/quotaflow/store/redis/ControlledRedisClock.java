package io.quotaflow.store.redis;

import io.quotaflow.core.store.QuotaDomain;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * Test-only clock substitution for exact trace/oracle comparisons. The production guards,
 * numeric arithmetic and chain/seed bodies remain unchanged. Real-TIME tests run separately.
 * Install before any operation; real Redis TTLs remain much longer than these bounded traces.
 */
public final class ControlledRedisClock {
    private ControlledRedisClock() { }
    public static String install(RedisRateLimitStore store, QuotaDomain domain) {
        String clockKey = "qf-test-clock:{" + RedisKeyScheme.domainDigest(domain) + "}";
        try {
            var constructor = LuaScript.class.getDeclaredConstructor(String.class, String.class); constructor.setAccessible(true);
            for (String name : new String[]{"guardedChain", "guardedSeed"}) {
                var field = RedisRateLimitStore.class.getDeclaredField(name); field.setAccessible(true);
                LuaScript original = (LuaScript) field.get(store);
                String marker = "local t = redis.call('TIME')";
                if (!original.source().contains(marker)) throw new IllegalStateException("numeric clock hook changed");
                String source = original.source().replace(marker,
                        "local n = tonumber(redis.call('GET', '" + clockKey + "'))\n"
                        + "    local t = {math.floor(n / 1000000000), math.floor((n % 1000000000) / 1000)}");
                String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-1").digest(source.getBytes(StandardCharsets.UTF_8)));
                field.set(store, constructor.newInstance(source, digest));
            }
            return clockKey;
        } catch (ReflectiveOperationException | java.security.NoSuchAlgorithmException failure) { throw new AssertionError(failure); }
    }
}
