package io.quotaflow.config;

/**
 * Outcome of one reload pass. A rejected reload carries the actionable error
 * (offending key or validation failure); the serving policy set is unchanged
 * in that case.
 */
public record ReloadResult(boolean applied, int policyCount, String error) {

    public static ReloadResult applied(int policyCount) {
        return new ReloadResult(true, policyCount, null);
    }

    public static ReloadResult rejected(String error) {
        return new ReloadResult(false, 0, error);
    }
}
