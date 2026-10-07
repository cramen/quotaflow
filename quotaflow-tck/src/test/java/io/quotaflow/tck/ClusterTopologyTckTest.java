package io.quotaflow.tck;

import static io.quotaflow.testing.TestIdentities.key;
import io.quotaflow.core.store.BucketIdentity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.StoreResult;
import io.quotaflow.store.redis.RedisKeyScheme;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Chain evaluation against a real Redis Cluster: every level key of a chain
 * shares one hash tag, so the multi-key chain script is a legal same-slot
 * script and decisions are identical to standalone mode.
 *
 * <p>The single-container cluster (three masters, three replicas on ports
 * 17000-17005) announces 127.0.0.1 addresses and is bound to the same fixed
 * host ports, so Lettuce topology discovery works from the test JVM. The
 * high port range avoids collisions with macOS system services on 7000.
 */
class ClusterTopologyTckTest {

    private static final int FIRST_PORT = 17000;
    private static final int NODE_COUNT = 6;
    private static final boolean VALKEY = Boolean.getBoolean("quotaflow.topology.valkey");
    private static final String CLUSTER_IMAGE = VALKEY ? "valkey/valkey:8.0-alpine" : "redis:6.2.24-alpine";
    private static final String CLI = VALKEY ? "valkey-cli" : "redis-cli";

    private static String clusterCommand() {
        String server = VALKEY ? "valkey-server" : "redis-server";
        StringBuilder script = new StringBuilder("set -e\n");
        for (int port = FIRST_PORT; port < FIRST_PORT + NODE_COUNT; port++) {
            script.append("mkdir -p /tmp/node-").append(port).append("\n")
                    .append(server).append(" --port ").append(port)
                    .append(" --bind 0.0.0.0 --protected-mode no --cluster-enabled yes --cluster-node-timeout 30000")
                    .append(" --cluster-announce-ip 127.0.0.1 --dir /tmp/node-").append(port)
                    .append(" --cluster-config-file nodes.conf --daemonize yes\n");
        }
        script.append(CLI).append(" --cluster create");
        for (int port = FIRST_PORT; port < FIRST_PORT + NODE_COUNT; port++) script.append(" 127.0.0.1:").append(port);
        script.append(" --cluster-replicas 1 --cluster-yes\necho 'CLUSTER READY'\nexec tail -f /dev/null\n");
        return script.toString();
    }

    private static GenericContainer<?> cluster;
    private static GenericContainer<?> standalone;
    private static RedisClusterClient clusterClient;

    @BeforeAll
    static void startCluster() {
        Assumptions.assumeTrue(
                DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");

        // Fixed host ports: the nodes announce 127.0.0.1:17000-17005 (env IP +
        // INITIAL_PORT), so host port numbers must match the announced ones
        // for Lettuce topology discovery to work from the test JVM. The fixed
        // ports race with the asynchronous reaper of the previous test JVM's
        // cluster container, so stale containers are removed first and the
        // start retried while the port range settles.
        removeStaleClusterContainers();
        cluster = startWithPortRetry(3);

        standalone = new GenericContainer<>(DockerImageName.parse(CLUSTER_IMAGE))
                .withExposedPorts(6379);
        standalone.start();

        // Readiness is probed with a plain client: polling with a cluster
        // client while the cluster is still forming caches a slot-less
        // topology that poisons every later connection.
        awaitClusterReady(Duration.ofSeconds(120));
        clusterClient = RedisClusterClient.create(
                RedisURI.create("127.0.0.1", FIRST_PORT));
        awaitTopologyUsable(Duration.ofSeconds(60));
        try (var connection = clusterClient.connect()) {
            new io.quotaflow.store.redis.RedisNamespaceAdmin(connection).provisionFresh("default", true);
        }
        RedisClient provisioning = RedisClient.create("redis://" + standalone.getHost() + ":" + standalone.getMappedPort(6379));
        try (var connection = provisioning.connect()) {
            new io.quotaflow.store.redis.RedisNamespaceAdmin(connection).provisionFresh("default", true);
        } finally { provisioning.shutdown(); }
    }

    @org.junit.jupiter.api.BeforeEach
    void waitForAnAuthoritativeTopologyBeforeEachScenario() throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        RuntimeException last = null;
        do {
            clusterClient.reloadPartitions();
            try (var connection = clusterClient.connect()) {
                assertEquals(3, new io.quotaflow.store.redis.RedisNamespaceAdmin(connection).primaryIds().size());
                return;
            } catch (io.quotaflow.core.PolicyConfigurationException | io.lettuce.core.RedisException transientTopology) {
                last = transientTopology;
            }
            Thread.sleep(100);
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("cluster did not converge to an administrable topology", last);
    }

    private static GenericContainer<?> startWithPortRetry(int attempts) {
        ContainerLaunchException lastFailure = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            List<String> portBindings = new ArrayList<>();
            for (int port = FIRST_PORT; port < FIRST_PORT + NODE_COUNT; port++) {
                portBindings.add(port + ":" + port);
            }
            GenericContainer<?> candidate =
                    new GenericContainer<>(DockerImageName.parse(CLUSTER_IMAGE))
                            .withLabel("quotaflow.test.topology", "cluster")
                            .withCommand("sh", "-c", clusterCommand())
                            .waitingFor(org.testcontainers.containers.wait.strategy.Wait.forLogMessage(".*CLUSTER READY.*", 1));
            candidate.setPortBindings(portBindings);
            try {
                candidate.start();
                return candidate;
            } catch (ContainerLaunchException e) {
                lastFailure = e;
                removeStaleClusterContainers();
                try {
                    Thread.sleep(2_000);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        throw lastFailure;
    }

    private static void removeStaleClusterContainers() {
        DockerClientFactory.instance().client()
                .listContainersCmd()
                .withShowAll(true)
                .withLabelFilter(java.util.Map.of("quotaflow.test.topology", "cluster"))
                .exec()
                .forEach(stale -> DockerClientFactory.instance().client()
                        .removeContainerCmd(stale.getId())
                        .withForce(true)
                        .exec());
    }

    @AfterAll
    static void stopCluster() {
        if (clusterClient != null) clusterClient.shutdown();
        if (cluster != null) cluster.stop();
        if (standalone != null) standalone.stop();
    }

    private static void awaitClusterReady(Duration deadline) {
        Instant giveUp = Instant.now().plus(deadline);
        Exception lastFailure = null;
        while (Instant.now().isBefore(giveUp)) {
            RedisClient probeClient =
                    RedisClient.create(RedisURI.create("127.0.0.1", FIRST_PORT));
            try (StatefulRedisConnection<String, String> probe = probeClient.connect()) {
                String info = probe.sync().clusterInfo();
                if (info.contains("cluster_state:ok")) {
                    probeClient.shutdown();
                    return;
                }
            } catch (Exception e) {
                lastFailure = e;
            } finally {
                probeClient.shutdown();
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while waiting for the cluster to form");
            }
        }
        fail("cluster did not reach cluster_state:ok within " + deadline
                + (lastFailure == null ? "" : "; last failure: " + lastFailure)
                + "\n--- container logs ---\n" + cluster.getLogs());
    }

    /**
     * A slot-routed write must succeed several times in a row before tests
     * begin: right after formation the emulated cluster nodes can flap
     * (CLUSTERDOWN) while the bus settles.
     */
    private static void awaitTopologyUsable(Duration deadline) {
        int consecutiveSuccesses = 0;
        Instant giveUp = Instant.now().plus(deadline);
        Exception lastFailure = null;
        while (Instant.now().isBefore(giveUp)) {
            try (StatefulRedisClusterConnection<String, String> probe = clusterClient.connect()) {
                probe.sync().eval(
                        "redis.call('SET', KEYS[1], '1'); redis.call('DEL', KEYS[1]); return 1",
                        io.lettuce.core.ScriptOutputType.INTEGER,
                        new String[] {"{quotaflow:probe}:a", "{quotaflow:probe}:b"});
                consecutiveSuccesses++;
                if (consecutiveSuccesses >= 5) {
                    return;
                }
            } catch (Exception e) {
                lastFailure = e;
                consecutiveSuccesses = 0;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while waiting for the cluster topology");
            }
        }
        fail("cluster topology was not stably usable within " + deadline
                + (lastFailure == null ? "" : "; last failure: " + lastFailure));
    }

    private static List<LevelRequest> chain(String run) {
        String suffix = '-' + run;
        return List.of(
                new LevelRequest(key("g:global:global" + suffix),
                        new Limit(2, 1, Duration.ofSeconds(10)), Algorithm.TOKEN_BUCKET, 1),
                new LevelRequest(key("t:tenant:acme" + suffix),
                        new Limit(5, 1, Duration.ofSeconds(10)), Algorithm.TOKEN_BUCKET, 1),
                new LevelRequest(key("u:user:alice" + suffix),
                        new Limit(3, 1, Duration.ofSeconds(10)), Algorithm.TOKEN_BUCKET, 1));
    }

    @Test void fixedCohortRecoveryRunsThroughSameSlotControllersOnCluster() throws Exception {
        String namespace = "cluster-recovery-" + UUID.randomUUID();
        var domain = new io.quotaflow.core.store.QuotaDomain(namespace, "root");
        var cohort = new io.quotaflow.core.store.RecoveryCohort(List.of("a", "b"));
        var policies = io.quotaflow.core.PolicySet.compile(List.of(io.quotaflow.core.RateLimitPolicy.builder("root")
                .scope(io.quotaflow.core.Scope.GLOBAL).limit(new Limit(10, 10, Duration.ofSeconds(1))).build()));
        var safe = RedisClusterClient.create(RedisURI.create("127.0.0.1", FIRST_PORT));
        var transport = io.lettuce.core.cluster.ClusterClientOptions.builder();
        transport.disconnectedBehavior(io.lettuce.core.ClientOptions.DisconnectedBehavior.REJECT_COMMANDS);
        transport.requestQueueSize(4096); transport.replayFilter(command -> false);
        transport.timeoutOptions(io.lettuce.core.TimeoutOptions.enabled(Duration.ofSeconds(2)));
        safe.setOptions(transport.build());
        try (var ca = safe.connect(); var cb = safe.connect(); var sa = new RedisRateLimitStore(ca, RedisStoreConfig.defaults());
             var sb = new RedisRateLimitStore(cb, RedisStoreConfig.defaults())) {
            try (var administrative = clusterClient.connect(); var administrativeStore = new RedisRateLimitStore(administrative, RedisStoreConfig.defaults())) {
                new io.quotaflow.store.redis.RedisNamespaceAdmin(administrative).provisionFresh(namespace, true);
                var bindings = List.of(new io.quotaflow.core.store.PolicyBinding(domain, "root", io.quotaflow.core.Scope.GLOBAL, Algorithm.TOKEN_BUCKET));
                administrativeStore.registerPolicies(bindings).toCompletableFuture().join();
                var admin = new io.quotaflow.store.redis.RedisRecoveryController(administrative, Duration.ofSeconds(2));
                admin.provisionCohort(namespace, cohort, "initial", true, true).toCompletableFuture().join();
                admin.provisionDomain(domain, cohort, "initial", policies.recoveryFingerprint("root"), true, true).toCompletableFuture().join();
            }
            try (var a = new io.quotaflow.fallback.FallbackRateLimitStore(sa.recoveryPrimary(namespace, Duration.ofSeconds(2), true),
                    new io.quotaflow.fallback.RecoverySettings(namespace, "test", "a", cohort, 100, 100, Duration.ofMillis(20), Duration.ofSeconds(2)), List.of());
                 var b = new io.quotaflow.fallback.FallbackRateLimitStore(sb.recoveryPrimary(namespace, Duration.ofSeconds(2), true),
                         new io.quotaflow.fallback.RecoverySettings(namespace, "test", "b", cohort, 100, 100, Duration.ofMillis(20), Duration.ofSeconds(2)), List.of())) {
                var flow = io.quotaflow.core.DefaultQuotaFlow.builder(policies, a).namespace(namespace).build();
                io.quotaflow.core.DefaultQuotaFlow.builder(policies, b).namespace(namespace).build();
                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while ((a.state() != io.quotaflow.fallback.DegradationState.CLOSED || b.state() != io.quotaflow.fallback.DegradationState.CLOSED)
                        && System.nanoTime() < deadline) Thread.sleep(20);
                assertEquals(io.quotaflow.fallback.DegradationState.CLOSED, a.state());
                assertEquals(io.quotaflow.fallback.DegradationState.CLOSED, b.state());
                assertTrue(flow.tryAcquire("root", io.quotaflow.core.RateLimitContext.empty()).isAllowed());
            }
        } finally { safe.shutdown(); }
    }


    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(Algorithm.class)
    void slotMigrationRedirectionAndReconnectNeverDuplicateTheSharedBudget(Algorithm algorithm) throws Exception {
        clusterClient.reloadPartitions();
        String namespace = "moved-" + UUID.randomUUID();
        try (var connection = clusterClient.connect();
             var store = new io.quotaflow.testing.RecoveryStoreFixture(connection, RedisStoreConfig.defaults())) {
            var admin = new io.quotaflow.store.redis.RedisNamespaceAdmin(connection);
            admin.provisionFresh(namespace, true);
            var primaryIds = admin.primaryIds();
            assertEquals(3, primaryIds.size());
            var domain = new io.quotaflow.core.store.QuotaDomain(namespace, "parent");
            var parent = new BucketIdentity(domain, "parent", io.quotaflow.core.Scope.GLOBAL, "shared");
            var limit = new Limit(3, 1, Duration.ofHours(1));
            var chain = List.of(new LevelRequest(parent, limit, algorithm, 1),
                    new LevelRequest(new BucketIdentity(domain, "child", io.quotaflow.core.Scope.USER, "one"), limit, algorithm, 1));
            assertTrue(store.tryAcquireAll(chain).toCompletableFuture().join().acquired());
            int slot = io.lettuce.core.cluster.SlotHash.getSlot(RedisKeyScheme.defaults().singleKey(parent));
            var source = connection.getPartitions().getPartitionBySlot(slot);
            assertTrue(primaryIds.contains(source.getNodeId()));
            var target = connection.getPartitions().stream().filter(node -> primaryIds.contains(node.getNodeId())
                    && !node.getNodeId().equals(source.getNodeId())).findFirst().orElseThrow();
            var from = connection.getConnection(source.getNodeId()).sync();
            var to = connection.getConnection(target.getNodeId()).sync();
            assertEquals(source.getNodeId(), cluster.execInContainer(CLI, "-p", Integer.toString(source.getUri().getPort()), "CLUSTER", "MYID").getStdout().trim());
            assertEquals(target.getNodeId(), cluster.execInContainer(CLI, "-p", Integer.toString(target.getUri().getPort()), "CLUSTER", "MYID").getStdout().trim());
            assertEquals("OK", cluster.execInContainer(CLI, "-p", Integer.toString(target.getUri().getPort()),
                    "CLUSTER", "SETSLOT", Integer.toString(slot), "IMPORTING", source.getNodeId()).getStdout().trim());
            assertEquals("OK", cluster.execInContainer(CLI, "-p", Integer.toString(source.getUri().getPort()),
                    "CLUSTER", "SETSLOT", Integer.toString(slot), "MIGRATING", target.getNodeId()).getStdout().trim());
            var keys = from.clusterGetKeysInSlot(slot, 1000);
            assertFalse(keys.isEmpty());
            var migrate = new ArrayList<String>(List.of(CLI, "-p", Integer.toString(source.getUri().getPort()),
                    "MIGRATE", "127.0.0.1", Integer.toString(target.getUri().getPort()), "", "0", "5000", "KEYS"));
            migrate.addAll(keys);
            var moved = cluster.execInContainer(migrate.toArray(String[]::new));
            assertEquals("OK", moved.getStdout().trim());
            to.scriptFlush();
            // Slot still belongs to the source: this invocation must follow ASK and reload Lua.
            assertTrue(store.tryAcquireAll(chain).toCompletableFuture().join().acquired());
            for (var node : connection.getPartitions()) if (primaryIds.contains(node.getNodeId()))
                assertEquals("OK", cluster.execInContainer(CLI, "-p", Integer.toString(node.getUri().getPort()),
                        "CLUSTER", "SETSLOT", Integer.toString(slot), "NODE", target.getNodeId()).getStdout().trim());
            to.scriptFlush();
            // Keep the client's old slot map: the next invocation must handle MOVED.
            assertTrue(store.tryAcquireAll(chain).toCompletableFuture().join().acquired());
            var killed = cluster.execInContainer(CLI, "-p", Integer.toString(target.getUri().getPort()),
                    "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes");
            assertEquals(0, killed.getExitCode());
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                try { if ("PONG".equals(connection.getConnection(target.getNodeId()).sync().ping())) break; }
                catch (io.lettuce.core.RedisException reconnecting) { Thread.sleep(50); }
            }
            assertFalse(store.tryAcquire(parent, limit, algorithm, 1).acquired());
            assertFalse(store.tryAcquireAll(chain).toCompletableFuture().join().acquired());
        } finally { clusterClient.reloadPartitions(); }
    }

    @Test
    void numericContractOnCluster() {
        try (var connection = clusterClient.connect();
                var store = new io.quotaflow.testing.RecoveryStoreFixture(connection, RedisStoreConfig.defaults())) {
            NumericConformance.verify(store);
        }
    }

    @Test
    void sharedWeightedHierarchyAndDirectParentsOnCluster() throws Exception {
        try (var connection = clusterClient.connect(); var store = new io.quotaflow.testing.RecoveryStoreFixture(connection,
                new RedisStoreConfig(Duration.ofSeconds(2), Duration.ofSeconds(4)))) {
            for (Algorithm algorithm : Algorithm.values()) HierarchyConformance.verify(store, algorithm);
        }
    }

    private static io.lettuce.core.cluster.api.StatefulRedisClusterConnection<String, String>
            awaitAdministrationTopology() throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        io.quotaflow.core.PolicyConfigurationException lastFailure = null;
        while (System.nanoTime() < deadline) {
            clusterClient.reloadPartitions();
            var connection = clusterClient.connect();
            try {
                var primaries = new io.quotaflow.store.redis.RedisNamespaceAdmin(connection).primaryIds();
                if (primaries.size() == 3) return connection;
            } catch (io.quotaflow.core.PolicyConfigurationException forming) {
                lastFailure = forming;
            }
            connection.close();
            Thread.sleep(100);
        }
        throw new AssertionError("Cluster administration topology did not stabilize", lastFailure);
    }

    @Test
    void migrationAssessmentIncludesEveryPrimaryShard() throws Exception {
        // Slot routing can be ready before all nodes agree on replica roles.
        // Refresh until the administration guard observes the complete primary set.
        try (var connection = awaitAdministrationTopology()) {
            var admin = new io.quotaflow.store.redis.RedisNamespaceAdmin(connection);
            var primaries = admin.primaryIds();
            assertEquals(3, primaries.size());
            var written = new java.util.HashSet<String>();
            for (int i = 0; written.size() < primaries.size() && i < 1000; i++) {
                String legacy = "{migration-" + i + "}:migration:global:g";
                int slot = io.lettuce.core.cluster.SlotHash.getSlot(legacy);
                String primary = connection.getPartitions().getPartitionBySlot(slot).getNodeId();
                if (written.add(primary)) connection.sync().psetex(legacy, 5000, "0:1:0");
            }
            assertEquals(primaries, written);
            var inventory = new io.quotaflow.store.redis.RedisNamespaceAdmin.LegacyInventory(
                    java.util.Set.of("migration"), java.util.Set.of(), primaries, true, true, true);
            var assessment = admin.assessMigration(inventory);
            assertEquals(3, assessment.outstandingKeys());
            assertThrows(io.quotaflow.core.PolicyConfigurationException.class,
                    () -> admin.provisionMigrated("cluster-migration", 4096, inventory));
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!admin.assessMigration(inventory).drained() && System.nanoTime() < deadline) Thread.sleep(50);
            assertTrue(admin.provisionMigrated("cluster-migration", 4096, inventory).drained());
        }
    }

    @Test
    void allChainKeysLandOnOneSlot() {
        try (StatefulRedisClusterConnection<String, String> connection = clusterClient.connect()) {
            List<LevelRequest> chain = chain(UUID.randomUUID().toString());
            List<String> keys = RedisKeyScheme.defaults()
                    .chainKeys(chain.stream().map(LevelRequest::storageKey).toList());
            List<Long> slots = keys.stream()
                    .map(key -> connection.sync().clusterKeyslot(key))
                    .toList();
            assertEquals(1, slots.stream().distinct().count(),
                    "all chain keys must share one slot, got " + slots);
        }
    }

    @Test
    void chainDecisionsOnClusterAreIdenticalToStandalone() {
        String run = UUID.randomUUID().toString();
        List<LevelRequest> chain = chain(run);

        List<ChainResult> clusterDecisions;
        try (StatefulRedisClusterConnection<String, String> connection = clusterClient.connect();
                RedisRateLimitStore clusterStore =
                        new io.quotaflow.testing.RecoveryStoreFixture(connection, RedisStoreConfig.defaults())) {
            clusterDecisions = scriptedSequence(clusterStore, chain);
        }

        String uri = "redis://" + standalone.getHost() + ":" + standalone.getMappedPort(6379);
        try (RedisClient standaloneClient = RedisClient.create(uri);
                StatefulRedisConnection<String, String> connection = standaloneClient.connect();
                RedisRateLimitStore standaloneStore =
                        new io.quotaflow.testing.RecoveryStoreFixture(connection, RedisStoreConfig.defaults())) {
            List<ChainResult> standaloneDecisions = scriptedSequence(standaloneStore, chain(run));
            assertEquals(standaloneDecisions, clusterDecisions,
                    "cluster decisions must equal standalone decisions step by step");
        }
    }

    /**
     * allow (global 1 left), allow (global drained), reject at the global
     * level. Remaining values are exact because the 10 s refill period dwarfs
     * the elapsed time.
     */
    private static List<ChainResult> scriptedSequence(RedisRateLimitStore store, List<LevelRequest> chain) {
        ChainResult first = store.tryAcquireAll(chain).toCompletableFuture().join();
        assertTrue(first.acquired());
        ChainResult second = store.tryAcquireAll(chain).toCompletableFuture().join();
        assertTrue(second.acquired());
        ChainResult third = store.tryAcquireAll(chain).toCompletableFuture().join();
        assertFalse(third.acquired());
        assertEquals(0, third.firedLevelIndex());
        assertTrue(third.retryAfterMillis() > 0);
        // retry-after carries sub-second jitter; compare the deterministic part
        return List.of(
                new ChainResult(first.acquired(), first.firedLevelIndex(), first.remaining(), 0),
                new ChainResult(second.acquired(), second.firedLevelIndex(), second.remaining(), 0),
                new ChainResult(third.acquired(), third.firedLevelIndex(), third.remaining(), 0));
    }

    @Test
    void singleKeyAlgorithmsWorkOnCluster() {
        try (StatefulRedisClusterConnection<String, String> connection = clusterClient.connect();
                RedisRateLimitStore store = new io.quotaflow.testing.RecoveryStoreFixture(connection, RedisStoreConfig.defaults())) {
            Limit limit = new Limit(1, 1, Duration.ofSeconds(10));
            for (Algorithm algorithm : Algorithm.values()) {
                String key = "cluster-" + algorithm.name().toLowerCase() + ":user:" + '-' + UUID.randomUUID();
                StoreResult allowed = store.tryAcquire(key(key), limit, algorithm, 1);
                assertTrue(allowed.acquired(), algorithm + " first acquisition");
                assertFalse(store.tryAcquire(key(key), limit, algorithm, 1).acquired(),
                        algorithm + " second acquisition must be rejected");
            }
        }
    }
}
