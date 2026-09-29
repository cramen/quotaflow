package io.quotaflow.store.redis;

import io.quotaflow.core.Scope;
import io.quotaflow.core.store.BucketIdentity;
import io.quotaflow.core.store.QuotaDomain;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Canonical v2 keys: every bucket in a connected tree shares its root domain slot. */
public final class RedisKeyScheme {
    public static final int BUCKET_KEY_BYTES = 137;
    private static final byte[] FORMAT = "quotaflow-key-v2".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] DOMAIN = field("domain");
    private static final byte[] BUCKET = field("bucket");
    private static final byte[] NAMESPACE = field("namespace");
    private static final byte[] ROOT = field("root");
    private static final byte[] POLICY = field("policy");
    private static final byte[] SCOPE = field("scope");
    private static final byte[] KEY = field("key");
    private static final byte[][] SCOPE_VALUES = java.util.Arrays.stream(Scope.values())
            .map(scope -> field(scope.wireName())).toArray(byte[][]::new);

    public static RedisKeyScheme defaults() { return new RedisKeyScheme(); }

    public String singleKey(BucketIdentity bucket) {
        Objects.requireNonNull(bucket, "bucket");
        MessageDigest digest = sha256();
        byte[] domain = domainFields(bucket.domain());
        return prefix(digest, domain) + bucketDigest(digest, domain, bucket);
    }

    public List<String> chainKeys(List<BucketIdentity> buckets) {
        Objects.requireNonNull(buckets, "buckets");
        if (buckets.isEmpty()) throw new IllegalArgumentException("buckets must not be empty");
        QuotaDomain domain = buckets.get(0).domain();
        for (BucketIdentity bucket : buckets) requireDomain(domain, bucket);
        MessageDigest digest = sha256();
        byte[] encodedDomain = domainFields(domain);
        String prefix = prefix(digest, encodedDomain);
        List<String> keys = new ArrayList<>(buckets.size());
        for (BucketIdentity bucket : buckets) keys.add(prefix + bucketDigest(digest, encodedDomain, bucket));
        return List.copyOf(keys);
    }

    public String chainLevelKey(QuotaDomain domain, BucketIdentity bucket) {
        requireDomain(domain, bucket);
        return singleKey(bucket);
    }

    /** Reserved placement for the separately implemented recovery controller. */
    public String controlKey(QuotaDomain domain) {
        return prefix(sha256(), domainFields(domain)) + "control";
    }

    public String manifestKey(String namespace) {
        new QuotaDomain(namespace, "validation");
        MessageDigest digest = start(sha256(), NAMESPACE);
        updateField(digest, namespace);
        return "qf:v2:namespace:" + finish(digest);
    }

    static String policyDigest(String policyId) {
        MessageDigest digest = start(sha256(), POLICY);
        updateField(digest, policyId);
        return finish(digest);
    }

    static String domainDigest(QuotaDomain domain) {
        MessageDigest digest = start(sha256(), DOMAIN);
        digest.update(domainFields(domain));
        return finish(digest);
    }

    private static String prefix(MessageDigest digest, byte[] domain) {
        start(digest, DOMAIN).update(domain);
        return "qf:v2:{" + finish(digest) + "}:";
    }

    private static String bucketDigest(MessageDigest digest, byte[] domain, BucketIdentity bucket) {
        start(digest, BUCKET).update(domain);
        digest.update(POLICY);
        updateField(digest, bucket.policyId());
        digest.update(SCOPE);
        digest.update(SCOPE_VALUES[bucket.scope().ordinal()]);
        digest.update(KEY);
        updateField(digest, bucket.rawKey());
        return finish(digest);
    }

    private static void requireDomain(QuotaDomain domain, BucketIdentity bucket) {
        Objects.requireNonNull(domain, "domain");
        Objects.requireNonNull(bucket, "bucket");
        if (!domain.equals(bucket.domain())) {
            throw new IllegalArgumentException("all buckets must belong to the supplied quota domain");
        }
    }

    /** Encode the shared domain once per call, never retain arbitrary raw-key data. */
    private static byte[] domainFields(QuotaDomain domain) {
        Objects.requireNonNull(domain, "domain");
        byte[] namespace = domain.namespace().getBytes(StandardCharsets.UTF_8);
        byte[] root = domain.rootPolicyId().getBytes(StandardCharsets.UTF_8);
        int length = Math.addExact(Math.addExact(namespace.length, root.length), NAMESPACE.length + ROOT.length + 8);
        return ByteBuffer.allocate(length).put(NAMESPACE).putInt(namespace.length).put(namespace)
                .put(ROOT).putInt(root.length).put(root).array();
    }

    private static byte[] field(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(Math.addExact(4, bytes.length)).putInt(bytes.length).put(bytes).array();
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java platform", e);
        }
    }

    private static MessageDigest start(MessageDigest digest, byte[] kind) {
        digest.reset();
        digest.update(FORMAT);
        digest.update(kind);
        return digest;
    }

    private static String finish(MessageDigest digest) {
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void updateField(MessageDigest digest, String field) {
        byte[] bytes = field.getBytes(StandardCharsets.UTF_8);
        digest.update((byte) (bytes.length >>> 24));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
    }
}
