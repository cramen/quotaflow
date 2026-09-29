package io.quotaflow.store.redis;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.quotaflow.core.PolicyConfigurationException;
import io.quotaflow.core.store.LocalPolicyBindings;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Blocking offline provisioning and migration assessment. Never used by acquisition.
 * Callers own connections and attest that all old writers, including local fallback,
 * remain stopped through cutover. This tool does not stop application processes.
 */
public final class RedisNamespaceAdmin {
    public record LegacyInventory(Set<String> ownedPolicies, Set<String> excludedPolicies,
            Set<String> expectedPrimaryIds, boolean writersQuiesced, boolean complete, boolean localDebtDrained) {
        public LegacyInventory {
            ownedPolicies = Set.copyOf(ownedPolicies);
            excludedPolicies = Set.copyOf(excludedPolicies);
            expectedPrimaryIds = Set.copyOf(expectedPrimaryIds);
            if (ownedPolicies.isEmpty() || expectedPrimaryIds.isEmpty()) {
                throw new IllegalArgumentException("ownership and primary inventory must not be empty");
            }
            if (ownedPolicies.stream().anyMatch(excludedPolicies::contains)) {
                throw new IllegalArgumentException("owned and excluded policies must be disjoint");
            }
        }
    }

    /** Aggregate diagnostics contain no raw Redis keys or quota identities. */
    public record MigrationAssessment(long outstandingKeys, long noExpiryKeys,
            long unknownOwnershipKeys, long maximumTtlMillis, Set<String> primaryIds) {
        public MigrationAssessment { primaryIds = Set.copyOf(primaryIds); }
        public boolean drained() {
            return outstandingKeys == 0 && noExpiryKeys == 0 && unknownOwnershipKeys == 0;
        }
    }

    private final StatefulRedisConnection<String, String> standalone;
    private final StatefulRedisClusterConnection<String, String> cluster;
    private final RedisScriptingAsyncCommands<String, String> scripts;
    private final long timeoutNanos;
    private final RedisKeyScheme keys = RedisKeyScheme.defaults();
    private final LuaScript provision = LuaScript.load("/lua/provision_namespace.lua");

    public RedisNamespaceAdmin(StatefulRedisConnection<String, String> connection) {
        this(connection, null, Duration.ofSeconds(30));
    }

    public RedisNamespaceAdmin(StatefulRedisClusterConnection<String, String> connection) {
        this(null, connection, Duration.ofSeconds(30));
    }

    public RedisNamespaceAdmin(StatefulRedisConnection<String, String> connection, Duration timeout) {
        this(connection, null, timeout);
    }

    public RedisNamespaceAdmin(StatefulRedisClusterConnection<String, String> connection, Duration timeout) {
        this(null, connection, timeout);
    }

    private RedisNamespaceAdmin(StatefulRedisConnection<String, String> standalone,
            StatefulRedisClusterConnection<String, String> cluster, Duration timeout) {
        if (standalone == null && cluster == null) throw new NullPointerException("connection");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofHours(1)) > 0) {
            throw new IllegalArgumentException("administration timeout must be positive and at most one hour");
        }
        this.standalone = standalone;
        this.cluster = cluster;
        this.scripts = standalone != null ? standalone.async() : cluster.async();
        this.timeoutNanos = timeout.toNanos();
    }

    /** Explicitly declares a new deployment with no preceding quota consumption. */
    public void provisionFresh(String namespace, boolean noPreviousQuotaState) {
        provisionFresh(namespace, noPreviousQuotaState, LocalPolicyBindings.DEFAULT_MAX_REGISTERED_POLICIES);
    }

    public void provisionFresh(String namespace, boolean noPreviousQuotaState, int maximumPolicies) {
        if (!noPreviousQuotaState) {
            throw new PolicyConfigurationException("existing deployments require migration assessment, not fresh provisioning");
        }
        provision(namespace, maximumPolicies, "fresh", System.nanoTime() + timeoutNanos);
    }

    /** Primary IDs for the operator's explicit inventory; refreshed from live server metadata. */
    public Set<String> primaryIds() {
        return Set.copyOf(primaryConnections(System.nanoTime() + timeoutNanos).keySet());
    }

    public MigrationAssessment assessMigration(LegacyInventory inventory) {
        return assess(inventory, System.nanoTime() + timeoutNanos);
    }

    /** Reassesses immediately; an earlier successful report alone never authorizes cutover. */
    public MigrationAssessment provisionMigrated(String namespace, int maximumPolicies, LegacyInventory inventory) {
        long deadline = System.nanoTime() + timeoutNanos;
        MigrationAssessment assessment = assess(inventory, deadline);
        if (!assessment.drained()) {
            throw new PolicyConfigurationException("legacy quota is not drained: outstanding="
                    + assessment.outstandingKeys() + ", noExpiry=" + assessment.noExpiryKeys()
                    + ", unknownOwnership=" + assessment.unknownOwnershipKeys());
        }
        provision(namespace, maximumPolicies, "migrated", deadline);
        return assessment;
    }

    private MigrationAssessment assess(LegacyInventory inventory, long deadline) {
        Objects.requireNonNull(inventory, "inventory");
        if (!inventory.writersQuiesced() || !inventory.complete() || !inventory.localDebtDrained()) {
            throw new PolicyConfigurationException("migration requires complete ownership inventory, stopped writers and proved local fallback debt drain");
        }
        Map<String, StatefulRedisConnection<String, String>> primaries = primaryConnections(deadline);
        String topology = topologyFingerprint(primaries, deadline);
        if (!primaries.keySet().equals(inventory.expectedPrimaryIds())) {
            throw new PolicyConfigurationException("primary topology differs from the explicit migration inventory");
        }
        long outstanding = 0, noExpiry = 0, unknown = 0, maxTtl = 0;
        for (StatefulRedisConnection<String, String> connection : primaries.values()) {
            ScanCursor cursor = ScanCursor.INITIAL;
            do {
                KeyScanCursor<String> page = await(connection.async().scan(cursor, ScanArgs.Builder.limit(256)), deadline);
                for (String key : page.getKeys()) {
                    if (!key.startsWith("{") || !key.contains("}:")) continue;
                    Set<String> owners = legacyOwners(key);
                    if (owners.isEmpty()) continue; // Not the legacy three-component bucket layout.
                    if (owners.size() != 1) { unknown++; continue; }
                    String owner = owners.iterator().next();
                    if (inventory.excludedPolicies().contains(owner)) continue;
                    if (!inventory.ownedPolicies().contains(owner) || !hasSupportedLegacyScope(key)) { unknown++; continue; }
                    long ttl = await(connection.async().pttl(key), deadline);
                    if (ttl == -1) noExpiry++;
                    else if (ttl >= 0) { outstanding++; maxTtl = Math.max(maxTtl, ttl); }
                    else if (ttl != -2) unknown++;
                }
                cursor = page;
            } while (!cursor.isFinished());
        }
        Map<String, StatefulRedisConnection<String, String>> after = primaryConnections(deadline);
        if (!after.keySet().equals(primaries.keySet()) || !topology.equals(topologyFingerprint(after, deadline))) {
            throw new PolicyConfigurationException("primary topology changed during migration assessment");
        }
        return new MigrationAssessment(outstanding, noExpiry, unknown, maxTtl, primaries.keySet());
    }

    private static Set<String> legacyOwners(String key) {
        Set<String> owners = new HashSet<>();
        for (int at = key.indexOf("}:"); at >= 0; at = key.indexOf("}:", at + 2)) {
            String[] tail = key.substring(at + 2).split(":", 3);
            if (tail.length == 3) owners.add(tail[0]);
        }
        return owners;
    }

    private static boolean hasSupportedLegacyScope(String key) {
        for (int at = key.indexOf("}:"); at >= 0; at = key.indexOf("}:", at + 2)) {
            String[] tail = key.substring(at + 2).split(":", 3);
            if (tail.length == 3 && Set.of("global", "tenant", "user", "key").contains(tail[1])) return true;
        }
        return false;
    }

    private Map<String, StatefulRedisConnection<String, String>> primaryConnections(long deadline) {
        if (standalone != null) {
            String info = await(standalone.async().info("server"), deadline);
            String id = info.lines().filter(line -> line.startsWith("run_id:"))
                    .map(line -> line.substring(7).trim()).findFirst()
                    .orElseThrow(() -> new PolicyConfigurationException("server identity is unavailable"));
            return Map.of(id, standalone);
        }
        Set<String> live = livePrimaries(await(cluster.async().clusterNodes(), deadline));
        Map<String, StatefulRedisConnection<String, String>> nodes = new LinkedHashMap<>();
        for (RedisClusterNode node : cluster.getPartitions()) {
            if (node.getRole().isUpstream()) {
                nodes.put(node.getNodeId(), await(cluster.getConnectionAsync(node.getNodeId()), deadline));
            }
        }
        if (!nodes.keySet().equals(live)) {
            throw new PolicyConfigurationException("cluster topology must be refreshed before administration");
        }
        for (StatefulRedisConnection<String, String> node : nodes.values()) {
            if (!livePrimaries(await(node.async().clusterNodes(), deadline)).equals(live)) {
                throw new PolicyConfigurationException("cluster primaries disagree about migration topology");
            }
        }
        return nodes;
    }

    private String topologyFingerprint(Map<String, StatefulRedisConnection<String, String>> nodes, long deadline) {
        StringBuilder fingerprint = new StringBuilder();
        java.util.TreeMap<String, String> expectedSlots = null;
        for (String id : new TreeSet<>(nodes.keySet())) {
            var node = nodes.get(id);
            String runId = await(node.async().info("server"), deadline).lines()
                    .filter(line -> line.startsWith("run_id:")).findFirst()
                    .orElseThrow(() -> new PolicyConfigurationException("server incarnation is unavailable"));
            fingerprint.append(id).append('/').append(runId).append(';');
            if (cluster != null) {
                String description = await(node.async().clusterNodes(), deadline);
                livePrimaries(description);
                java.util.TreeMap<String, String> slots = new java.util.TreeMap<>();
                for (String line : description.split("\\R")) {
                    String[] fields = line.trim().split(" +");
                    if (List.of(fields[2].split(",")).contains("master")) {
                        slots.put(fields[0], fields[6] + ":" + String.join(",",
                                java.util.Arrays.copyOfRange(fields, 8, fields.length)));
                    }
                }
                if (expectedSlots != null && !expectedSlots.equals(slots)) {
                    throw new PolicyConfigurationException("cluster slot ownership is inconsistent");
                }
                expectedSlots = slots;
            }
        }
        return fingerprint.append(expectedSlots).toString();
    }

    private static Set<String> livePrimaries(String description) {
        Set<String> result = new TreeSet<>();
        for (String line : description.split("\\R")) {
            String[] fields = line.trim().split(" +");
            if (fields.length < 8 || line.contains("[") || fields[2].contains("fail")
                    || fields[2].contains("handshake") || fields[2].contains("noaddr")
                    || !fields[7].equals("connected")) {
                throw new PolicyConfigurationException("cluster topology is unstable or incomplete");
            }
            if (List.of(fields[2].split(",")).contains("master")) result.add(fields[0]);
        }
        if (result.isEmpty()) throw new PolicyConfigurationException("no cluster primary is available");
        return result;
    }

    private void provision(String namespace, int maximum, String mode, long deadline) {
        if (maximum < 1) throw new IllegalArgumentException("maximumPolicies must be positive");
        List<Object> result = await(scripts.<List<Object>>eval(provision.source(), ScriptOutputType.MULTI,
                new String[] {keys.manifestKey(namespace)}, Integer.toString(maximum), mode), deadline);
        if (((Number) result.get(0)).longValue() != 1) {
            throw new PolicyConfigurationException("namespace already exists or provisioning was refused; no state was changed");
        }
    }

    private static <T> T await(CompletionStage<T> future, long deadline) {
        try {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new java.util.concurrent.TimeoutException();
            return future.toCompletableFuture().get(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PolicyConfigurationException("namespace administration interrupted; readiness is unconfirmed");
        } catch (Exception e) {
            throw new PolicyConfigurationException("namespace administration could not verify server state; readiness is unconfirmed");
        }
    }
}
