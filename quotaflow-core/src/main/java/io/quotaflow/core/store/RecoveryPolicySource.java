package io.quotaflow.core.store;

import io.quotaflow.core.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

/** Immutable policy view; dynamic resolution is invoked only on bounded recovery workers. */
public final class RecoveryPolicySource {
    private final Map<String, RateLimitPolicy> policies;
    private final Map<QuotaDomain, String> fingerprints;
    private final LimitResolver resolver;
    private final Set<QuotaDomain> dynamicDomains;
    private final java.util.concurrent.ConcurrentHashMap<QuotaDomain, String> retiredFingerprints = new java.util.concurrent.ConcurrentHashMap<>();
    private final String namespace;

    public RecoveryPolicySource(PolicySet set, String namespace, LimitResolver resolver) {
        this.namespace = new QuotaDomain(namespace, "validation").namespace();
        this.resolver = resolver;
        policies = set.policies().stream().collect(Collectors.toUnmodifiableMap(RateLimitPolicy::id, p -> p));
        Map<QuotaDomain, List<RateLimitPolicy>> roots = new HashMap<>();
        for (RateLimitPolicy policy : set.policies()) roots.computeIfAbsent(new QuotaDomain(namespace, set.rootPolicyId(policy.id())),
                ignored -> new ArrayList<>()).add(policy);
        Map<QuotaDomain, String> hashes = new HashMap<>();
        roots.forEach((domain, tree) -> hashes.put(domain, set.recoveryFingerprint(domain.rootPolicyId())));
        fingerprints = Map.copyOf(hashes);
        dynamicDomains = roots.keySet().stream().filter(domain -> set.hasDynamicLimits(domain.rootPolicyId())).collect(Collectors.toUnmodifiableSet());
    }
    public String namespace() { return namespace; }
    public Set<QuotaDomain> domains() { return fingerprints.keySet(); }
    public String fingerprint(QuotaDomain domain) {
        String fingerprint = fingerprints.get(domain);
        if (fingerprint == null) throw new PolicyConfigurationException("recovery policy domain is not active");
        return fingerprint;
    }
    public void validateCapabilities() {
        if (!dynamicDomains.isEmpty() && !(resolver instanceof VersionedLimitResolver))
            throw new PolicyConfigurationException("coordinated dynamic policies require a VersionedLimitResolver");
    }
    /** Captures one complete immutable resolver view, including when only a static ancestor is requested. */
    public Optional<Target> capture(QuotaDomain domain) {
        if (!domain.namespace().equals(namespace)) throw new PolicyConfigurationException("recovery target namespace mismatch");
        String policyFingerprint = fingerprints.get(domain);
        if (policyFingerprint == null) policyFingerprint = retiredFingerprints.computeIfAbsent(domain, key -> {
            MessageDigest digest = sha256(); field(digest, "quotaflow-retired-domain-v1");
            field(digest, key.namespace()); field(digest, key.rootPolicyId());
            return HexFormat.of().formatHex(digest.digest());
        });
        if (!dynamicDomains.contains(domain)) return Optional.of(new Target(domain, policyFingerprint, null));
        validateCapabilities();
        Optional<LimitSnapshot> snapshot = Objects.requireNonNull(((VersionedLimitResolver) resolver).snapshot(), "snapshot result");
        String captured = policyFingerprint;
        return snapshot.map(view -> new Target(domain, captured, view));
    }
    /** One domain's captured quota target. Retired cells retain their last validated parameters. */
    public final class Target {
        private final QuotaDomain domain;
        private final String fingerprint;
        private final LimitSnapshot snapshot;
        private Target(QuotaDomain domain, String fingerprint, LimitSnapshot snapshot) {
            this.domain = domain; this.fingerprint = fingerprint; this.snapshot = snapshot;
        }
        public QuotaDomain domain() { return domain; }
        public String fingerprint() { return fingerprint; }
        public long resolverRevision() { return snapshot == null ? 0 : snapshot.revision(); }
        public String resolverFingerprint() { return snapshot == null ? LimitSnapshot.NONE_FINGERPRINT : snapshot.fingerprint(); }
        public RecoveryConfiguration configuration(long revision) {
            return new RecoveryConfiguration(fingerprint, revision, resolverRevision(), resolverFingerprint());
        }
        public boolean sameAs(Target other) {
            return other != null && domain.equals(other.domain) && fingerprint.equals(other.fingerprint)
                    && resolverRevision() == other.resolverRevision() && resolverFingerprint().equals(other.resolverFingerprint());
        }
        public boolean active(BucketIdentity bucket) { return fingerprints.containsKey(domain) && policies.containsKey(bucket.policyId()); }
        public Optional<Limit> resolve(BucketIdentity bucket, String keyGroup, Limit lastKnown) {
            if (!domain.equals(bucket.domain())) throw new PolicyConfigurationException("snapshot bucket belongs to another domain");
            RateLimitPolicy policy = policies.get(bucket.policyId());
            if (policy == null) return Optional.ofNullable(lastKnown);
            if (policy.limit().isPresent()) return policy.limit();
            return snapshot == null ? Optional.empty() : snapshot.resolve(policy.limitRef().orElseThrow(), keyGroup);
        }
    }

    /** Removed policies retain their last full-limit metadata until debt has been reconciled. */
    public Optional<Limit> resolve(BucketIdentity bucket, String keyGroup, Limit lastKnown) {
        RateLimitPolicy policy = policies.get(bucket.policyId());
        if (policy == null) return Optional.of(lastKnown);
        if (policy.limit().isPresent()) return policy.limit();
        return resolver == null ? Optional.empty() : resolver.resolve(policy.limitRef().orElseThrow(), keyGroup);
    }
    /** Namespace is validated independently by the operation domain and cohort incarnation. */
    public static String fingerprintTree(PolicySet set, String root) {
        List<RateLimitPolicy> tree = set.policies().stream().filter(p -> set.rootPolicyId(p.id()).equals(root))
                .sorted(Comparator.comparing(RateLimitPolicy::id)).toList();
        MessageDigest digest = sha256(); field(digest, "quotaflow-policy-view-v1"); field(digest, root);
        for (RateLimitPolicy policy : tree) {
            field(digest, policy.id()); field(digest, policy.scope().name()); field(digest, policy.algorithm().name());
            field(digest, policy.parentId().orElse("")); field(digest, policy.keyResolverId().orElse(""));
            field(digest, policy.defaultKey().orElse(""));
            if (policy.limit().isPresent()) {
                field(digest, "static"); field(digest, Long.toString(policy.limit().orElseThrow().capacity()));
                field(digest, Long.toString(policy.limit().orElseThrow().emissionIntervalNanos()));
            } else { field(digest, "dynamic"); field(digest, policy.limitRef().orElseThrow()); }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 is required", e); }
    }
    private static void field(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array()); digest.update(bytes);
    }
}
