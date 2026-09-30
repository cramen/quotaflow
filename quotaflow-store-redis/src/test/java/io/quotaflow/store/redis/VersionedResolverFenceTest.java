package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** Regression for the original unchanged-limitRef / old-10/10-after-new-2/1 seed counterexample. */
class VersionedResolverFenceTest extends RedisContainerSupport {
    @Test void providerOrderingPrecedesSeedNormalizationAndSurvivesStaticAndCohortTransitions() {
        try (var connection = client().connect(); var store = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            var domain = new QuotaDomain("default", "quota");
            var key = new BucketIdentity(domain, "quota", Scope.GLOBAL, "all");
            var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).limitRef("plan").build()));
            String policyFp = policies.recoveryFingerprint("quota");
            var cohort = new RecoveryCohort(List.of("a", "b"));
            store.registerPolicies(List.of(PolicyBinding.of(key, Algorithm.TOKEN_BUCKET))).toCompletableFuture().join();
            var admin = new RedisRecoveryController(connection, Duration.ofSeconds(2));
            admin.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            admin.provisionDomain(domain, cohort, "initial", policyFp, true, true).toCompletableFuture().join();
            var a = admin.enroll("default", cohort, "a", "owner-a").toCompletableFuture().join();
            var b = admin.enroll("default", cohort, "b", "owner-b").toCompletableFuture().join();
            var previous = new Limit(10, 10, Duration.ofSeconds(1)); var current = new Limit(2, 1, Duration.ofSeconds(1));
            var lookup = new LimitSnapshot.Key("plan", "global");
            var first = new LimitSnapshot(1, Map.of(lookup, previous)); var second = new LimitSnapshot(2, Map.of(lookup, current));
            var aInitial = admin.attach(domain, a).toCompletableFuture().join().context();
            var old = admin.begin(aInitial, target(policyFp, 0, first)).toCompletableFuture().join().context();
            var bObserved = admin.attach(domain, b).toCompletableFuture().join().context();
            var fresh = admin.begin(bObserved, target(policyFp, 0, second)).toCompletableFuture().join().context();
            assertTrue(store.seed(fresh, List.of(new BucketState(key, current, Algorithm.TOKEN_BUCKET, 1))).toCompletableFuture().join());
            String redisKey = RedisKeyScheme.defaults().singleKey(key); String before = connection.sync().get(redisKey);
            assertEquals(ParameterFingerprint.of(Algorithm.TOKEN_BUCKET, current), before.split(":")[2]);
            assertFalse(store.seed(old, List.of(new BucketState(key, previous, Algorithm.TOKEN_BUCKET, 4))).toCompletableFuture().join());
            assertEquals(before, connection.sync().get(redisKey));
            var latestA = admin.read(domain, a).toCompletableFuture().join().context();
            admin.join(latestA).toCompletableFuture().join();
            // Even copied current control counters and a valid joined slot cannot authorize an older resolver snapshot.
            var retagged = new RecoveryContext(domain, a, fresh.epoch(), fresh.dispatchGeneration(), fresh.configurationVersion(),
                    policyFp, fresh.phase(), first.revision(), first.fingerprint());
            assertFalse(store.seed(retagged, List.of(new BucketState(key, previous, Algorithm.TOKEN_BUCKET, 4))).toCompletableFuture().join());
            var request = new LevelRequest(key, previous, Algorithm.TOKEN_BUCKET, 1, "global", policyFp, 1, first.fingerprint());
            var pending = new RecoveryPending(domain, 0, new CompletableFuture<>());
            assertNotNull(store.tryAcquireAll(retagged, List.of(request), true, pending).toCompletableFuture().join().recoveryPending());
            assertEquals(before, connection.sync().get(redisKey));
            assertFalse(admin.begin(latestA, target(policyFp, 999, first)).toCompletableFuture().join().applied(), "a larger local reload counter cannot roll back the provider");
            var conflicting = new LimitSnapshot(2, Map.of(lookup, previous));
            assertInstanceOf(StateCompatibilityException.class, assertThrows(CompletionException.class,
                    () -> admin.begin(latestA, target(policyFp, 999, conflicting)).toCompletableFuture().join()).getCause());
            assertEquals(before, connection.sync().get(redisKey));
            String staticFp = "b".repeat(64);
            var staticView = admin.begin(latestA, new RecoveryConfiguration(staticFp, 1)).toCompletableFuture().join();
            assertEquals(0, staticView.context().resolverRevision()); assertEquals(2, staticView.resolverFloor());
            assertFalse(admin.begin(staticView.context(), target(policyFp, 2, first)).toCompletableFuture().join().applied());
            var readded = admin.begin(staticView.context(), target(policyFp, 2, second)).toCompletableFuture().join();
            assertTrue(readded.applied()); assertEquals(2, readded.context().resolverRevision());
            assertFalse(store.seed(fresh, List.of(new BucketState(key, current, Algorithm.TOKEN_BUCKET, 0))).toCompletableFuture().join(), "configuration ABA cannot revive old seed authority");
            assertEquals(before, connection.sync().get(redisKey));
            // No business acquisition was allowed; both synthetic owners are quiescent.
            admin.replaceCohort("default", "initial", cohort, List.of(domain), "maintain", true, true).toCompletableFuture().join();
            var replacement = admin.enroll("default", cohort, "a", "replacement").toCompletableFuture().join();
            var afterMaintenance = admin.attach(domain, replacement).toCompletableFuture().join();
            assertEquals(2, afterMaintenance.resolverFloor());
            assertFalse(admin.begin(afterMaintenance.context(), target(policyFp, 1000, first)).toCompletableFuture().join().applied());
        }
    }
    private static RecoveryConfiguration target(String fingerprint, long localRevision, LimitSnapshot snapshot) {
        return new RecoveryConfiguration(fingerprint, localRevision, snapshot.revision(), snapshot.fingerprint());
    }
}
