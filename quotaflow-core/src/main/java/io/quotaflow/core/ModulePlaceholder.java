package io.quotaflow.core;

/** Placeholder proving the module toolchain; replaced by the policy engine. */
public final class ModulePlaceholder {

    private ModulePlaceholder() {
    }

    public static String describe(boolean verbose) {
        if (verbose) {
            return "quotaflow-core placeholder (verbose)";
        }
        return "quotaflow-core placeholder";
    }
}
