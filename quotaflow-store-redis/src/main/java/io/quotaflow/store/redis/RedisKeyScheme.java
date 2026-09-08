package io.quotaflow.store.redis;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Maps engine storage keys ({@code <policyId>:<scope>:<rawKey>}) onto
 * Cluster-safe Redis keys. Every key of one chain shares the hash tag
 * {@code {leafPolicyId:leafRawKey}}, so the multi-key chain script is a legal
 * same-slot script in Redis Cluster while chains of different tenants or users
 * spread across slots. The single-key scripts use the same format with the
 * level's own identity as the tag, so standalone and Cluster deployments share
 * one code path.
 *
 * <p>Raw keys longer than {@code maxRawKeyBytes} (UTF-8) are replaced by a
 * tagged SHA-256 digest ({@code sha256:<hex>}) to bound key size.
 */
public final class RedisKeyScheme {

    public static final int DEFAULT_MAX_RAW_KEY_BYTES = 128;

    private static final String HASH_PREFIX = "sha256:";

    private final int maxRawKeyBytes;

    public RedisKeyScheme(int maxRawKeyBytes) {
        if (maxRawKeyBytes < 1) {
            throw new IllegalArgumentException("maxRawKeyBytes must be >= 1, got " + maxRawKeyBytes);
        }
        this.maxRawKeyBytes = maxRawKeyBytes;
    }

    public static RedisKeyScheme defaults() {
        return new RedisKeyScheme(DEFAULT_MAX_RAW_KEY_BYTES);
    }

    /** Key for a single-level script call; the level's own identity is the hash tag. */
    public String singleKey(String storageKey) {
        Parsed parsed = parse(storageKey);
        return tag(parsed) + ':' + rewrittenTail(parsed);
    }

    /**
     * Keys for a chain script call, one per level, ordered like
     * {@code storageKeys}; the leaf (last) level's identity is the shared tag.
     */
    public List<String> chainKeys(List<String> storageKeys) {
        if (storageKeys.isEmpty()) {
            throw new IllegalArgumentException("storageKeys must not be empty");
        }
        String tag = tag(parse(storageKeys.get(storageKeys.size() - 1)));
        List<String> keys = new ArrayList<>(storageKeys.size());
        for (String storageKey : storageKeys) {
            keys.add(tag + ':' + rewrittenTail(parse(storageKey)));
        }
        return keys;
    }

    /**
     * Key for seeding one level of a chain whose leaf is
     * {@code leafStorageKey}; identical to the mapping {@link #chainKeys}
     * applies to that level.
     */
    public String chainLevelKey(String leafStorageKey, String levelStorageKey) {
        return tag(parse(leafStorageKey)) + ':' + rewrittenTail(parse(levelStorageKey));
    }

    private String tag(Parsed leaf) {
        return '{' + leaf.policyId() + ':' + bounded(leaf.rawKey()) + '}';
    }

    private String rewrittenTail(Parsed parsed) {
        return parsed.policyId() + ':' + parsed.scope() + ':' + bounded(parsed.rawKey());
    }

    private String bounded(String rawKey) {
        if (rawKey.getBytes(StandardCharsets.UTF_8).length <= maxRawKeyBytes) {
            return rawKey;
        }
        return HASH_PREFIX + HexFormat.of().formatHex(sha256(rawKey));
    }

    private static Parsed parse(String storageKey) {
        int first = storageKey.indexOf(':');
        int second = first < 0 ? -1 : storageKey.indexOf(':', first + 1);
        if (second < 0) {
            // Raw keys are sensitive: report the shape violation without echoing the key.
            throw new IllegalArgumentException(
                    "storage key must have the form <policyId>:<scope>:<rawKey> (got a key of length "
                            + storageKey.length() + " without two separators)");
        }
        return new Parsed(
                storageKey.substring(0, first),
                storageKey.substring(first + 1, second),
                storageKey.substring(second + 1));
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java platform", e);
        }
    }

    private record Parsed(String policyId, String scope, String rawKey) {
    }
}
