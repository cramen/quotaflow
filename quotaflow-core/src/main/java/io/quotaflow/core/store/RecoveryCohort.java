package io.quotaflow.core.store;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Exact fixed membership declaration; membership never expires because a peer is silent. */
public final class RecoveryCohort {
    public static final String DEFAULT_INSTANCE_ID = "single";
    private final List<String> members;
    private final String digest;

    public RecoveryCohort(List<String> members) {
        Objects.requireNonNull(members, "members");
        if (members.isEmpty()) throw new IllegalArgumentException("recovery cohort must not be empty");
        for (String member : members) QuotaDomain.requireIdentity(member, "cohort member");
        this.members = members.stream().sorted().toList();
        if (this.members.stream().distinct().count() != members.size()) {
            throw new IllegalArgumentException("recovery cohort contains duplicate members");
        }
        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            hash.update("quotaflow-cohort-v1".getBytes(StandardCharsets.UTF_8));
            for (String member : this.members) {
                byte[] bytes = member.getBytes(StandardCharsets.UTF_8);
                hash.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
                hash.update(bytes);
            }
            digest = HexFormat.of().formatHex(hash.digest());
        } catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 is required", e); }
    }

    public static RecoveryCohort single() { return new RecoveryCohort(List.of(DEFAULT_INSTANCE_ID)); }
    public List<String> members() { return members; }
    public int size() { return members.size(); }
    public String digest() { return digest; }
    public int slot(String instanceId) {
        int slot = members.indexOf(instanceId);
        if (slot < 0) throw new IllegalArgumentException("own instance is not declared in the recovery cohort");
        return slot;
    }
    public void validate(int expectedInstances, String ownId) {
        if (expectedInstances != size()) throw new IllegalArgumentException("cohort size differs from expected instances");
        slot(ownId);
    }
    @Override public boolean equals(Object other) {
        return other instanceof RecoveryCohort cohort && members.equals(cohort.members);
    }
    @Override public int hashCode() { return members.hashCode(); }
    @Override public String toString() { return "RecoveryCohort[size=" + size() + ", digest=" + digest + "]"; }
}
