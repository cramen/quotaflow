package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    private static GenericContainer<?> cluster;
    private static GenericContainer<?> standalone;
    private static RedisClusterClient clusterClient;

    @BeforeAll
    static void startCluster() {
        Assumptions.assumeTrue(
                DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");

        List<String> portBindings = new ArrayList<>();
        for (int port = FIRST_PORT; port < FIRST_PORT + NODE_COUNT; port++) {
            portBindings.add(port + ":" + port);
        }
        // Fixed host ports: the nodes announce 127.0.0.1:17000-17005 (env IP +
        // INITIAL_PORT), so host port numbers must match the announced ones
        // for Lettuce topology discovery to work from the test JVM.
        cluster = new GenericContainer<>(DockerImageName.parse("grokzen/redis-cluster:6.2.14"))
                .withEnv("IP", "127.0.0.1")
                .withEnv("INITIAL_PORT", String.valueOf(FIRST_PORT));
        cluster.setPortBindings(portBindings);
        cluster.start();

        standalone = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine"))
                .withExposedPorts(6379);
        standalone.start();

        // Readiness is probed with a plain client: polling with a cluster
        // client while the cluster is still forming caches a slot-less
        // topology that poisons every later connection.
        awaitClusterReady(Duration.ofSeconds(120));
        clusterClient = RedisClusterClient.create(
                RedisURI.create("127.0.0.1", FIRST_PORT));
        awaitTopologyUsable(Duration.ofSeconds(60));
    }

    @AfterAll
    static void stopCluster() {
        if (clusterClient != null) {
            clusterClient.shutdown();
        }
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
                new LevelRequest("g:global:global" + suffix,
                        new Limit(2, 1, Duration.ofSeconds(10)), Algorithm.TOKEN_BUCKET, 1),
                new LevelRequest("t:tenant:acme" + suffix,
                        new Limit(5, 1, Duration.ofSeconds(10)), Algorithm.TOKEN_BUCKET, 1),
                new LevelRequest("u:user:alice" + suffix,
                        new Limit(3, 1, Duration.ofSeconds(10)), Algorithm.TOKEN_BUCKET, 1));
    }

    @Test
    void allChainKeysLandOnOneSlot() {
        try (StatefulRedisClusterConnection<String, String> connection = clusterClient.connect()) {
            List<LevelRequest> chain = chain(UUID.randomUUID().toString());
            List<String> keys = new RedisKeyScheme(RedisKeyScheme.DEFAULT_MAX_RAW_KEY_BYTES)
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
                        new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            clusterDecisions = scriptedSequence(clusterStore, chain);
        }

        String uri = "redis://" + standalone.getHost() + ":" + standalone.getMappedPort(6379);
        try (RedisClient standaloneClient = RedisClient.create(uri);
                StatefulRedisConnection<String, String> connection = standaloneClient.connect();
                RedisRateLimitStore standaloneStore =
                        new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
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
                RedisRateLimitStore store = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            Limit limit = new Limit(1, 1, Duration.ofSeconds(10));
            for (Algorithm algorithm : Algorithm.values()) {
                String key = "cluster:user:" + algorithm.name().toLowerCase() + '-' + UUID.randomUUID();
                StoreResult allowed = store.tryAcquire(key, limit, algorithm, 1);
                assertTrue(allowed.acquired(), algorithm + " first acquisition");
                assertFalse(store.tryAcquire(key, limit, algorithm, 1).acquired(),
                        algorithm + " second acquisition must be rejected");
            }
        }
    }
}
