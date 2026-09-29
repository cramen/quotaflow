package io.quotaflow.core.store;

/** The deployment namespace and root policy that own one atomic quota tree. */
public record QuotaDomain(String namespace, String rootPolicyId) {

    public static final String DEFAULT_NAMESPACE = "default";

    public QuotaDomain {
        requireIdentity(namespace, "namespace");
        requireIdentity(rootPolicyId, "rootPolicyId");
    }

    static void requireIdentity(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        // Malformed UTF-16 must not alias another identity through UTF-8 replacement.
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i == value.length() || !Character.isLowSurrogate(value.charAt(i))) {
                    throw new IllegalArgumentException(name + " must contain valid Unicode");
                }
            } else if (Character.isLowSurrogate(c)) {
                throw new IllegalArgumentException(name + " must contain valid Unicode");
            }
        }
    }
}
