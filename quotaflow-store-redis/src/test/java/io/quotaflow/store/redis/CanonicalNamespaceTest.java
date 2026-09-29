package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.*;

import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class CanonicalNamespaceTest extends RedisContainerSupport {
    private static final Limit ONE = new Limit(1, 1, Duration.ofHours(1));
    private static final Limit CHILD = new Limit(20, 1, Duration.ofHours(1));
    private static final QuotaDomain DOMAIN = new QuotaDomain("default", "provider");

    private BucketIdentity bucket(String policy, Scope scope, String raw) {
        return new BucketIdentity(DOMAIN, policy, scope, raw);
    }

    private static Throwable failure(Runnable call) {
        return assertThrows(CompletionException.class, call::run).getCause();
    }

    @Test
    void sharedCapacityOneRejectsSecondLeafAndDirectParent() {
        try (var connection = client().connect(); var store = newStore(connection)) {
            var parent = bucket("provider", Scope.GLOBAL, "shared");
            var a = List.of(new LevelRequest(parent, ONE, Algorithm.TOKEN_BUCKET, 1),
                    new LevelRequest(bucket("user", Scope.USER, "alice"), CHILD, Algorithm.TOKEN_BUCKET, 1));
            var b = List.of(a.get(0), new LevelRequest(bucket("user", Scope.USER, "bob"), CHILD, Algorithm.TOKEN_BUCKET, 1));
            assertTrue(store.tryAcquireAll(a).toCompletableFuture().join().acquired());
            assertFalse(store.tryAcquireAll(b).toCompletableFuture().join().acquired());
            assertFalse(store.tryAcquire(parent, ONE, Algorithm.TOKEN_BUCKET, 1).acquired());
            assertEquals(0, store.tryAcquireAll(b).toCompletableFuture().join().firedLevelIndex());
        }
    }

    @Test
    void parentSeedIsVisibleThroughEveryCallShape() {
        try (var connection = client().connect(); var store = newStore(connection)) {
            var parent = bucket("provider", Scope.GLOBAL, "shared");
            var local = new LocalRateLimitStore(() -> 0L);
            local.tryAcquire(parent, ONE, Algorithm.TOKEN_BUCKET, 1);
            store.seed(DOMAIN, local.snapshot()).toCompletableFuture().join();
            assertFalse(store.tryAcquire(parent, ONE, Algorithm.TOKEN_BUCKET, 1).acquired());
            for (String user : List.of("alice", "bob")) {
                var result = store.tryAcquireAll(List.of(new LevelRequest(parent, ONE, Algorithm.TOKEN_BUCKET, 1),
                        new LevelRequest(bucket("user", Scope.USER, user), CHILD, Algorithm.TOKEN_BUCKET, 1)))
                        .toCompletableFuture().join();
                assertFalse(result.acquired());
                assertEquals(0, result.firedLevelIndex());
            }
            var wrong = new BucketState(new BucketIdentity(new QuotaDomain("other", "provider"),
                    "user", Scope.USER, "secret"), CHILD, Algorithm.TOKEN_BUCKET, 0);
            assertThrows(IllegalArgumentException.class, () -> store.seed(DOMAIN, List.of(local.snapshot().get(0), wrong)));
        }
    }

    @Test
    void namespaceMustBeProvisionedAndExistingReadinessCannotBeOverwritten() {
        try (var connection = client().connect(); var store = newStore(connection)) {
            var key = new BucketIdentity(new QuotaDomain("fresh", "p"), "p", Scope.GLOBAL, "g");
            assertInstanceOf(PolicyConfigurationException.class, failure(() -> store.tryAcquire(key, ONE, Algorithm.TOKEN_BUCKET, 1)));
            assertEquals(0L, connection.sync().exists(RedisKeyScheme.defaults().singleKey(key)));
            var admin = new RedisNamespaceAdmin(connection);
            assertThrows(PolicyConfigurationException.class, () -> admin.provisionFresh("fresh", false));
            admin.provisionFresh("fresh", true);
            assertTrue(store.tryAcquire(key, ONE, Algorithm.TOKEN_BUCKET, 1).acquired());
            assertThrows(PolicyConfigurationException.class, () -> admin.provisionFresh("fresh", true));
            assertFalse(store.tryAcquire(key, ONE, Algorithm.TOKEN_BUCKET, 1).acquired());
            assertThrows(IllegalArgumentException.class, () -> admin.provisionFresh("bad", true, 0));
            assertThrows(IllegalArgumentException.class, () -> new RedisNamespaceAdmin(connection, Duration.ZERO));
        }
    }

    @Test
    void invalidCandidateAndBudgetOverflowNeverPartiallyRegister() {
        try (var connection = client().connect(); var first = newStore(connection); var second = newStore(connection)) {
            new RedisNamespaceAdmin(connection).provisionFresh("bounded", true, 2);
            var d = new QuotaDomain("bounded", "root");
            var original = new PolicyBinding(d, "one", Scope.USER);
            first.registerPolicies(List.of(original)).toCompletableFuture().join();
            var extra = new PolicyBinding(d, "two", Scope.USER);
            var conflict = new PolicyBinding(new QuotaDomain("bounded", "other"), "one", Scope.USER);
            assertInstanceOf(PolicyConfigurationException.class, failure(() -> second.registerPolicies(List.of(extra, conflict)).toCompletableFuture().join()));
            var keys = RedisKeyScheme.defaults();
            assertEquals("1", connection.sync().hget(keys.manifestKey("bounded"), "count"));
            assertNull(connection.sync().hget(keys.manifestKey("bounded"), "p:" + RedisKeyScheme.policyDigest("two")));
            assertInstanceOf(PolicyConfigurationException.class, failure(() -> first.registerPolicies(List.of(extra,
                    new PolicyBinding(d, "three", Scope.USER))).toCompletableFuture().join()));
            assertEquals("1", connection.sync().hget(keys.manifestKey("bounded"), "count"));
            first.registerPolicies(List.of()).toCompletableFuture().join();
            assertInstanceOf(PolicyConfigurationException.class, failure(() -> second.registerPolicies(List.of(conflict)).toCompletableFuture().join()));
            second.registerPolicies(List.of(extra)).toCompletableFuture().join();
            assertEquals("2", connection.sync().hget(keys.manifestKey("bounded"), "count"));
        }
    }

    @Test
    void independentClientsCannotOverfillTheRegistrationBudget() {
        try (var c1 = client().connect(); var c2 = client().connect(); var a = newStore(c1); var b = newStore(c2)) {
            new RedisNamespaceAdmin(c1).provisionFresh("race", true, 3);
            var d = new QuotaDomain("race", "root");
            var one = a.registerPolicies(List.of(new PolicyBinding(d, "a1", Scope.USER), new PolicyBinding(d, "a2", Scope.USER))).toCompletableFuture();
            var two = b.registerPolicies(List.of(new PolicyBinding(d, "b1", Scope.USER), new PolicyBinding(d, "b2", Scope.USER))).toCompletableFuture();
            int accepted = 0;
            for (var future : List.of(one, two)) {
                try { future.join(); accepted++; }
                catch (CompletionException e) { assertInstanceOf(PolicyConfigurationException.class, e.getCause()); }
            }
            assertEquals(1, accepted);
            assertEquals("2", c1.sync().hget(RedisKeyScheme.defaults().manifestKey("race"), "count"));
        }
    }

    @Test
    void corruptMetadataIsAConfigurationFailureNotAnOutageFallback() {
        try (var connection = client().connect(); var store = newStore(connection)) {
            var manifest = RedisKeyScheme.defaults().manifestKey("default");
            connection.sync().hset(manifest, "count", "bad");
            assertInstanceOf(PolicyConfigurationException.class, failure(() -> store.tryAcquire(
                    bucket("provider", Scope.GLOBAL, "g"), ONE, Algorithm.TOKEN_BUCKET, 1)));
            connection.sync().del(manifest);
            connection.sync().set(manifest, "wrong-type");
            assertInstanceOf(PolicyConfigurationException.class, failure(() -> store.registerPolicies(
                    List.of(new PolicyBinding(DOMAIN, "provider", Scope.GLOBAL))).toCompletableFuture().join()));
        }
    }

    private RedisNamespaceAdmin.LegacyInventory inventory(RedisNamespaceAdmin admin, boolean stopped, boolean complete) {
        return new RedisNamespaceAdmin.LegacyInventory(Set.of("provider"), Set.of("other-app"), admin.primaryIds(), stopped, complete, true);
    }

    @Test
    void migrationBlocksUnknownOwnershipNoExpiryIncompleteInventoryAndLiveQuota() {
        try (var connection = client().connect()) {
            var admin = new RedisNamespaceAdmin(connection);
            var good = inventory(admin, true, true);
            connection.sync().set("{cache}:entry", "unrelated application data");
            connection.sync().set("{cache}:other-app:custom:entry", "explicitly excluded application data");
            assertTrue(admin.assessMigration(good).drained());
            assertThrows(PolicyConfigurationException.class, () -> admin.assessMigration(inventory(admin, false, true)));
            assertThrows(PolicyConfigurationException.class, () -> admin.assessMigration(inventory(admin, true, false)));
            assertThrows(PolicyConfigurationException.class, () -> admin.assessMigration(
                    new RedisNamespaceAdmin.LegacyInventory(Set.of("provider"), Set.of(), Set.of("wrong-server"), true, true, true)));
            assertThrows(PolicyConfigurationException.class, () -> admin.assessMigration(
                    new RedisNamespaceAdmin.LegacyInventory(Set.of("provider"), Set.of(), admin.primaryIds(), true, true, false)));
            connection.sync().set("{user:a}:provider:global:g", "0:1:0");
            var assessment = admin.assessMigration(good);
            assertEquals(1, assessment.noExpiryKeys());
            assertThrows(PolicyConfigurationException.class, () -> admin.provisionMigrated("migrated", 10, good));
            connection.sync().pexpire("{user:a}:provider:global:g", 60_000);
            assessment = admin.assessMigration(good);
            assertEquals(1, assessment.outstandingKeys());
            assertTrue(assessment.maximumTtlMillis() > 0);
            connection.sync().set("{user:b}:unknown:global:g", "0:1:0");
            connection.sync().set("{user:b}:other-app:global:g", "0:1:0");
            assertEquals(1, admin.assessMigration(good).unknownOwnershipKeys());
            assertFalse(admin.assessMigration(good).toString().contains("user:a"));
            assertEquals(0L, connection.sync().exists(RedisKeyScheme.defaults().manifestKey("migrated")));
        }
    }

    @Test
    void migrationReassessesNaturalDrainAndNeverDeletesSpentQuota() throws Exception {
        try (var connection = client().connect()) {
            var admin = new RedisNamespaceAdmin(connection);
            var inventory = inventory(admin, true, true);
            connection.sync().psetex("{user:a}:provider:global:g", 180, "0:1:0");
            assertThrows(PolicyConfigurationException.class, () -> admin.provisionMigrated("migrated", 10, inventory));
            assertNotNull(connection.sync().get("{user:a}:provider:global:g"));
            Thread.sleep(200);
            assertTrue(admin.provisionMigrated("migrated", 10, inventory).drained());
            try (var store = newStore(connection)) {
                var parent = new BucketIdentity(new QuotaDomain("migrated", "provider"), "provider", Scope.GLOBAL, "g");
                assertTrue(store.tryAcquire(parent, ONE, Algorithm.TOKEN_BUCKET, 1).acquired());
                assertFalse(store.tryAcquire(parent, ONE, Algorithm.TOKEN_BUCKET, 1).acquired());
            }
            assertThrows(PolicyConfigurationException.class, () -> admin.provisionFresh("migrated", true));
        }
    }
    @Test
    void rollbackRehearsalKeepsLegacyTrafficStoppedUntilCanonicalDebtExpires() throws Exception {
        try (var connection = client().connect(); var store = newStore(connection)) {
            var identity = bucket("provider", Scope.GLOBAL, "rollback");
            var limit = new Limit(1, 1, Duration.ofMillis(500));
            var trace = new java.util.ArrayList<String>();
            assertTrue(store.tryAcquire(identity, limit, Algorithm.TOKEN_BUCKET, 1).acquired());
            trace.add("canonical admission completed; all canonical and fallback writers stopped");
            String canonical = RedisKeyScheme.defaults().singleKey(identity);
            String legacy = "{old:rollback}:provider:global:rollback";
            assertTrue(connection.sync().pttl(canonical) > 0);
            assertEquals(0L, connection.sync().exists(legacy));
            trace.add("rollback withheld while canonical debt exists; no legacy admission");
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (connection.sync().exists(canonical) != 0 && System.nanoTime() < deadline) Thread.sleep(20);
            assertEquals(0L, connection.sync().exists(canonical));
            assertEquals(0L, connection.sync().exists(legacy));
            trace.add("complete fixture inventory naturally drained; no DEL or TTL shortening");
            List<Object> result = connection.sync().eval(LuaScript.load("/lua/token_bucket.lua").source(),
                    io.lettuce.core.ScriptOutputType.MULTI, new String[] {legacy}, "1", "1", "500000", "1");
            assertEquals(1L, ((Number) result.get(0)).longValue());
            trace.add("legacy writer resumed after verified drain; layouts never admitted concurrently");
            System.out.println("Rollback rehearsal: " + String.join(" -> ", trace));
        }
    }

}
