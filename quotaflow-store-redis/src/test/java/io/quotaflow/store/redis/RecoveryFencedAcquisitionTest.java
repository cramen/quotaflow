package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class RecoveryFencedAcquisitionTest extends RedisContainerSupport {
    @Test void phaseAndGenerationChecksPrecedeEveryQuotaWrite() {
        try (var connection = client().connect(); var store = newStore(connection)) {
            var domain = new QuotaDomain("default", "root");
            var bucket = new BucketIdentity(domain, "root", Scope.GLOBAL, "shared");
            var cohort = new RecoveryCohort(List.of("a", "b"));
            var controller = new RedisRecoveryController(connection, Duration.ofSeconds(2));
            var limit = new Limit(10, 1, Duration.ofHours(1));
            var request = List.of(new LevelRequest(bucket, limit, Algorithm.GCRA, 1));
            store.registerPolicies(List.of(PolicyBinding.of(bucket, Algorithm.GCRA))).toCompletableFuture().join();
            controller.provisionCohort("default", cohort, "inc", true, true).toCompletableFuture().join();
            controller.provisionDomain(domain, cohort, "inc", "a".repeat(64), true, true).toCompletableFuture().join();
            var a = controller.enroll("default", cohort, "a", "a").toCompletableFuture().join();
            var b = controller.enroll("default", cohort, "b", "b").toCompletableFuture().join();
            var aGather = controller.attach(domain, a).toCompletableFuture().join().context();
            var bGather = controller.attach(domain, b).toCompletableFuture().join().context();
            var pending = new RecoveryPending(domain, 0, new CompletableFuture<>());
            String redisKey = RedisKeyScheme.defaults().singleKey(bucket);
            assertNotNull(store.tryAcquireAll(aGather, request, false, pending).toCompletableFuture().join().recoveryPending());
            assertNull(connection.sync().get(redisKey));
            assertTrue(store.seed(aGather, List.of(new BucketState(bucket, limit, Algorithm.GCRA, 5))).toCompletableFuture().join());
            controller.join(aGather).toCompletableFuture().join();
            assertTrue(store.tryAcquireAll(aGather, request, true, pending).toCompletableFuture().join().acquired());
            var bDrain = controller.join(bGather).toCompletableFuture().join().context();
            String before = connection.sync().get(redisKey);
            assertNotNull(store.tryAcquireAll(aGather, request, true, pending).toCompletableFuture().join().recoveryPending());
            assertFalse(store.seed(aGather, List.of(new BucketState(bucket, limit, Algorithm.GCRA, 0))).toCompletableFuture().join());
            assertEquals(before, connection.sync().get(redisKey));
            assertNotNull(store.tryAcquireAll(bDrain, request, true, pending).toCompletableFuture().join().recoveryPending());
            var aDrain = controller.read(domain, a).toCompletableFuture().join().context();
            assertTrue(store.seed(aDrain, List.of(new BucketState(bucket, limit, Algorithm.GCRA, 0))).toCompletableFuture().join());
            controller.ready(aDrain).toCompletableFuture().join();
            var bNormal = controller.ready(bDrain).toCompletableFuture().join().context();
            assertFalse(store.seed(bDrain, List.of(new BucketState(bucket, limit, Algorithm.GCRA, 5))).toCompletableFuture().join());
            assertFalse(store.seed(bDrain, List.of()).toCompletableFuture().join(), "even an empty seed must validate its context");
            var result = store.tryAcquireAll(bNormal, request, false, pending).toCompletableFuture().join();
            assertNull(result.recoveryPending());
            assertFalse(result.acquired());
            assertEquals(0, result.remaining());
        }
    }
    @Test void plainSpiRequiresContextAndCannotBypassTheGatherGuard() {
        try (var connection = client().connect(); var store = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            var domain = new QuotaDomain("default", "root");
            var bucket = new BucketIdentity(domain, "root", Scope.GLOBAL, "shared");
            var limit = new Limit(10, 1, Duration.ofSeconds(1));
            store.registerPolicies(List.of(PolicyBinding.of(bucket, Algorithm.GCRA))).toCompletableFuture().join();
            assertThrows(PolicyConfigurationException.class, () -> store.tryAcquire(bucket, limit, Algorithm.GCRA, 1));
            assertThrows(PolicyConfigurationException.class, () -> store.seed(domain, List.of()));
            var controller = new RedisRecoveryController(connection, Duration.ofSeconds(2));
            var cohort = RecoveryCohort.single();
            controller.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            controller.provisionDomain(domain, cohort, "initial", "a".repeat(64), true, true).toCompletableFuture().join();
            var owner = controller.enroll("default", cohort, "single", "owner").toCompletableFuture().join();
            var gather = controller.attach(domain, owner).toCompletableFuture().join().context();
            store.bindRecoveryContext(gather);
            var pending = store.tryAcquire(bucket, limit, Algorithm.GCRA, 1);
            assertNotNull(pending.recoveryPending()); assertFalse(pending.acquired());
            assertNull(connection.sync().get(RedisKeyScheme.defaults().singleKey(bucket)));
            var drain = controller.join(gather).toCompletableFuture().join().context();
            var normal = controller.ready(drain).toCompletableFuture().join().context();
            store.bindRecoveryContext(normal);
            assertTrue(pending.recoveryPending().readiness().toCompletableFuture().isDone());
            assertTrue(store.tryAcquire(bucket, limit, Algorithm.GCRA, 1).acquired());
            assertThrows(java.util.concurrent.CompletionException.class, () -> store.seed(domain, List.of()).toCompletableFuture().join());
            connection.sync().del(RedisKeyScheme.defaults().controlKey(domain));
            assertInstanceOf(StateCompatibilityException.class, assertThrows(java.util.concurrent.CompletionException.class,
                    () -> store.tryAcquire(bucket, limit, Algorithm.GCRA, 1)).getCause());
        }
    }
    @Test void callerOwnedTransportsMustDeclareAndProvideRecoveryCapabilities() {
        try (var connection = client().connect(); var store = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            assertThrows(IllegalArgumentException.class, () -> store.recoveryPrimary("default", Duration.ofSeconds(1), true));
        }
        var safeClient = RedisClientFactory.createClient("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379), Duration.ofSeconds(1));
        try (var connection = safeClient.connect(); var store = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            assertThrows(IllegalArgumentException.class, () -> store.recoveryPrimary("default", Duration.ofSeconds(1), false));
            assertDoesNotThrow(() -> store.recoveryPrimary("default", Duration.ofSeconds(1), true).probe().toCompletableFuture().join());
        } finally { safeClient.shutdown(); }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(Algorithm.class)
    void cachedHealthyContextCannotSpendAcrossANewerRecoveryEpoch(Algorithm algorithm) {
        try (var connection = client().connect(); var store = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            var domain = new QuotaDomain("default", "root");
            var bucket = new BucketIdentity(domain, "root", Scope.GLOBAL, "shared");
            var limit = new Limit(10, 1, Duration.ofHours(1));
            store.registerPolicies(List.of(PolicyBinding.of(bucket, algorithm))).toCompletableFuture().join();
            var controller = new RedisRecoveryController(connection, Duration.ofSeconds(2));
            var cohort = RecoveryCohort.single();
            controller.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            controller.provisionDomain(domain, cohort, "initial", "a".repeat(64), true, true).toCompletableFuture().join();
            var session = controller.enroll("default", cohort, "single", "owner").toCompletableFuture().join();
            var gather = controller.attach(domain, session).toCompletableFuture().join().context();
            var drain = controller.join(gather).toCompletableFuture().join().context();
            var normal = controller.ready(drain).toCompletableFuture().join().context();
            store.bindRecoveryContext(normal);
            assertTrue(store.tryAcquire(bucket, limit, algorithm, 1).acquired());
            var next = controller.begin(normal, "a".repeat(64), 0).toCompletableFuture().join().context();
            assertTrue(next.epoch() > normal.epoch());
            String key = RedisKeyScheme.defaults().singleKey(bucket), before = connection.sync().get(key);
            var old = store.tryAcquire(bucket, limit, algorithm, 1);
            assertFalse(old.acquired()); assertNotNull(old.recoveryPending());
            assertEquals(before, connection.sync().get(key), "stale NORMAL must not debit or refill");
            assertFalse(controller.ready(drain).toCompletableFuture().join().applied());
            assertFalse(store.seed(normal, List.of(new BucketState(bucket, limit, algorithm, 10))).toCompletableFuture().join());
            assertEquals(before, connection.sync().get(key), "a delayed healthy seed cannot restore credit");
        }
    }
}
