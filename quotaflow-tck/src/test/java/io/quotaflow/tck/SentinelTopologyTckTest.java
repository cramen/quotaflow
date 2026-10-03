package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.*;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.store.redis.*;
import io.quotaflow.testing.RecoveryStoreFixture;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** A real Sentinel promotion, not a mocked endpoint replacement. */
class SentinelTopologyTckTest {
    private static final boolean VALKEY = Boolean.getBoolean("quotaflow.topology.valkey");
    private static final String CLI = VALKEY ? "valkey-cli" : "redis-cli";
    @ParameterizedTest @EnumSource(Algorithm.class)
    void promotedReplicaPreservesTheSharedQuotaAndReloadsScripts(Algorithm algorithm) throws Exception {
        String script = """
                mkdir -p /tmp/master /tmp/replica
                redis-server --port 17100 --bind 0.0.0.0 --protected-mode no --dir /tmp/master --daemonize yes
                redis-server --port 17101 --bind 0.0.0.0 --protected-mode no --dir /tmp/replica --replicaof 127.0.0.1 17100 --daemonize yes
                cat > /tmp/sentinel.conf <<'CONFIG'
                port 17102
                bind 0.0.0.0
                protected-mode no
                sentinel monitor quota-primary 127.0.0.1 17100 1
                sentinel down-after-milliseconds quota-primary 500
                sentinel failover-timeout quota-primary 5000
                sentinel parallel-syncs quota-primary 1
                CONFIG
                exec redis-server /tmp/sentinel.conf --sentinel
                """.replace("redis-server", VALKEY ? "valkey-server" : "redis-server");
        try (var server = new GenericContainer<>(DockerImageName.parse(VALKEY ? "valkey/valkey:8.0-alpine" : "redis:6.2.24-alpine"))
                .withCommand("sh", "-c", script).waitingFor(Wait.forLogMessage(".*Sentinel ID.*", 1))) {
            server.setPortBindings(List.of("17100:17100", "17101:17101", "17102:17102"));
            server.start();
            String uri = RedisURI.Builder.sentinel("127.0.0.1", 17102, "quota-primary").build().toURI().toString();
            var client = RedisClientFactory.createClient(uri, Duration.ofSeconds(2), Duration.ofSeconds(2));
            try (client; var connection = client.connect(); var store = new RecoveryStoreFixture(connection, RedisStoreConfig.defaults())) {
                new RedisNamespaceAdmin(connection).provisionFresh("sentinel", true);
                var domain = new QuotaDomain("sentinel", "parent");
                var parent = new BucketIdentity(domain, "parent", Scope.GLOBAL, "shared");
                var limit = new Limit(2, 1, Duration.ofHours(1));
                var chainA = List.of(new LevelRequest(parent, limit, algorithm, 1),
                        new LevelRequest(new BucketIdentity(domain, "child", Scope.USER, "a"), limit, algorithm, 1));
                var chainB = List.of(chainA.get(0), new LevelRequest(new BucketIdentity(domain, "child", Scope.USER, "b"), limit, algorithm, 1));
                assertTrue(store.tryAcquireAll(chainA).toCompletableFuture().join().acquired());
                long replicatedBy = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                long replicated = 0;
                while (replicated == 0 && System.nanoTime() < replicatedBy) replicated = connection.sync().waitForReplication(1, 200);
                assertEquals(1L, replicated, "promotion requires acknowledged replicated test state");
                long discoveredBy = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                String replicas = "";
                while (System.nanoTime() < discoveredBy) {
                    replicas = server.execInContainer(CLI, "-p", "17102", "SENTINEL", "replicas", "quota-primary").getStdout();
                    if (replicas.contains("17101") && !replicas.contains("disconnected")) break;
                    Thread.sleep(100);
                }
                assertTrue(replicas.contains("17101") && !replicas.contains("disconnected"), "Sentinel has not discovered a promotable replica");
                assertEquals(0, server.execInContainer(CLI, "-p", "17100", "SHUTDOWN", "NOSAVE").getExitCode());
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                boolean promoted = false;
                while (System.nanoTime() < deadline) {
                    var reply = server.execInContainer(CLI, "-p", "17102", "SENTINEL", "get-master-addr-by-name", "quota-primary");
                    if (reply.getStdout().contains("17101")) { promoted = true; break; }
                    Thread.sleep(100);
                }
                assertTrue(promoted, () -> "Sentinel did not promote the replica: " + server.getLogs());
                // Flush the promoted node's script cache; the existing store must reconnect
                // through Sentinel and recover NOSCRIPT without replaying a successful debit.
                assertEquals(0, server.execInContainer(CLI, "-p", "17101", "SCRIPT", "FLUSH").getExitCode());
                while (System.nanoTime() < deadline) {
                    try { if ("PONG".equals(connection.sync().ping())) break; }
                    catch (io.lettuce.core.RedisException reconnecting) { Thread.sleep(100); }
                }
                assertEquals("PONG", connection.sync().ping());
                assertTrue(store.tryAcquireAll(chainB).toCompletableFuture().get(3, TimeUnit.SECONDS).acquired());
                assertFalse(store.tryAcquireAll(chainA).toCompletableFuture().get(3, TimeUnit.SECONDS).acquired());
                assertFalse(store.tryAcquire(parent, limit, algorithm, 1).acquired(), "direct parent and both children share the promoted balance");
            }
        }
    }
}
