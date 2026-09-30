package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.store.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class RecoveryControllerTest extends RedisContainerSupport {
    private static final RecoveryCohort COHORT = new RecoveryCohort(List.of("a", "b"));
    private static final QuotaDomain DOMAIN = new QuotaDomain("default", "root");
    private static final String FP = "a".repeat(64);

    @Test void fixedCohortBarrierRequiresEverySessionAndRejectsStaleReadiness() {
        try (var connection = client().connect()) {
            newStore(connection).registerPolicies(List.of(new PolicyBinding(DOMAIN, "root", io.quotaflow.core.Scope.GLOBAL,
                    io.quotaflow.core.Algorithm.GCRA))).toCompletableFuture().join();
            var controller = new RedisRecoveryController(connection, Duration.ofSeconds(2));
            controller.provisionCohort("default", COHORT, "inc", true, true).toCompletableFuture().join();
            controller.provisionDomain(DOMAIN, COHORT, "inc", FP, true, true).toCompletableFuture().join();
            var a = controller.enroll("default", COHORT, "a", "session-a").toCompletableFuture().join();
            var b = controller.enroll("default", COHORT, "b", "session-b").toCompletableFuture().join();
            var aContext = controller.attach(DOMAIN, a).toCompletableFuture().join().context();
            var bContext = controller.attach(DOMAIN, b).toCompletableFuture().join().context();
            assertEquals(RecoveryPhase.GATHER, aContext.phase());
            var joined = controller.join(aContext).toCompletableFuture().join();
            assertEquals(1, joined.joinedMembers());
            assertEquals(1, controller.join(aContext).toCompletableFuture().join().joinedMembers());
            var drain = controller.join(bContext).toCompletableFuture().join();
            assertEquals(RecoveryPhase.DRAIN, drain.context().phase());
            assertFalse(controller.ready(aContext).toCompletableFuture().join().applied());
            var aDrain = controller.read(DOMAIN, a).toCompletableFuture().join().context();
            assertEquals(RecoveryPhase.DRAIN, controller.ready(aDrain).toCompletableFuture().join().context().phase());
            var normal = controller.ready(drain.context()).toCompletableFuture().join();
            assertEquals(RecoveryPhase.NORMAL, normal.context().phase());
            assertEquals(normal.context().epoch(), normal.retiredEpoch());
            assertEquals(2, controller.ready(drain.context()).toCompletableFuture().join().readyMembers());
            assertFalse(controller.abort(drain.context()).toCompletableFuture().join().applied());
            var next = controller.begin(DOMAIN, a, FP, 0).toCompletableFuture().join();
            assertEquals(RecoveryPhase.GATHER, next.context().phase());
            assertEquals(normal.context().epoch(), next.retiredEpoch(), "a missed NORMAL must still retire the old guard");
            assertFalse(controller.begin(aDrain, FP, 0).toCompletableFuture().join().applied(),
                    "a delayed begin cannot reopen a later recovery generation");
            assertThrows(CompletionException.class, () -> controller.enroll("default", COHORT, "a", "impostor").toCompletableFuture().join());
        }
    }

    @Test void abortInvalidatesOldCommandsAndConfigurationAbaCannotReuseReadiness() {
        try (var connection = client().connect()) {
            newStore(connection).registerPolicies(List.of(new PolicyBinding(DOMAIN, "root", io.quotaflow.core.Scope.GLOBAL,
                    io.quotaflow.core.Algorithm.GCRA))).toCompletableFuture().join();
            var controller = new RedisRecoveryController(connection, Duration.ofSeconds(2));
            controller.provisionCohort("default", COHORT, "inc", true, true).toCompletableFuture().join();
            controller.provisionDomain(DOMAIN, COHORT, "inc", FP, true, true).toCompletableFuture().join();
            var a = controller.enroll("default", COHORT, "a", "a").toCompletableFuture().join();
            var b = controller.enroll("default", COHORT, "b", "b").toCompletableFuture().join();
            var first = controller.attach(DOMAIN, a).toCompletableFuture().join().context();
            var second = controller.attach(DOMAIN, b).toCompletableFuture().join().context();
            controller.join(first).toCompletableFuture().join();
            var drain = controller.join(second).toCompletableFuture().join().context();
            var aborted = controller.abort(drain).toCompletableFuture().join();
            assertEquals(RecoveryPhase.GATHER, aborted.context().phase());
            assertTrue(aborted.context().dispatchGeneration() > drain.dispatchGeneration());
            assertFalse(controller.ready(drain).toCompletableFuture().join().applied());
            var changed = controller.begin(DOMAIN, a, "b".repeat(64), 1).toCompletableFuture().join();
            var returned = controller.begin(DOMAIN, a, FP, 2).toCompletableFuture().join();
            assertTrue(returned.context().configurationVersion() > changed.context().configurationVersion());
            assertFalse(controller.begin(DOMAIN, a, "b".repeat(64), 1).toCompletableFuture().join().applied());
            assertFalse(controller.join(first).toCompletableFuture().join().applied());
        }
    }

    @Test void missingControllerCannotBeSilentlyRecreated() {
        try (var connection = client().connect()) {
            newStore(connection).registerPolicies(List.of(new PolicyBinding(DOMAIN, "root", io.quotaflow.core.Scope.GLOBAL,
                    io.quotaflow.core.Algorithm.GCRA))).toCompletableFuture().join();
            var controller = new RedisRecoveryController(connection, Duration.ofSeconds(2));
            controller.provisionCohort("default", COHORT, "inc", true, true).toCompletableFuture().join();
            controller.provisionDomain(DOMAIN, COHORT, "inc", FP, true, true).toCompletableFuture().join();
            var session = controller.enroll("default", COHORT, "a", "a").toCompletableFuture().join();
            controller.attach(DOMAIN, session).toCompletableFuture().join();
            connection.sync().del(RedisKeyScheme.defaults().controlKey(DOMAIN));
            assertInstanceOf(StateCompatibilityException.class, assertThrows(CompletionException.class,
                    () -> controller.read(DOMAIN, session).toCompletableFuture().join()).getCause());
            assertThrows(CompletionException.class, () -> controller.provisionDomain(DOMAIN, COHORT, "inc", FP, true, true).toCompletableFuture().join());
        }
    }
    @Test void explicitQuiescentMaintenanceFencesEveryOldSessionAndPreservesCanonicalBuckets() {
        try (var connection = client().connect()) {
            var store = newStore(connection);
            store.registerPolicies(List.of(new PolicyBinding(DOMAIN, "root", io.quotaflow.core.Scope.GLOBAL,
                    io.quotaflow.core.Algorithm.GCRA))).toCompletableFuture().join();
            var controller = new RedisRecoveryController(connection, Duration.ofSeconds(2));
            controller.provisionCohort("default", COHORT, "inc", true, true).toCompletableFuture().join();
            controller.provisionDomain(DOMAIN, COHORT, "inc", FP, true, true).toCompletableFuture().join();
            var old = controller.enroll("default", COHORT, "a", "old").toCompletableFuture().join();
            var context = controller.attach(DOMAIN, old).toCompletableFuture().join().context();
            var bucket = new BucketIdentity(DOMAIN, "root", io.quotaflow.core.Scope.GLOBAL, "all");
            var limit = new io.quotaflow.core.Limit(10, 1, Duration.ofSeconds(1));
            store.seed(context, List.of(new BucketState(bucket, limit, io.quotaflow.core.Algorithm.GCRA, 0))).toCompletableFuture().join();
            var before = connection.sync().get(RedisKeyScheme.defaults().singleKey(bucket));
            assertThrows(IllegalArgumentException.class, () -> controller.replaceCohort("default", "inc", RecoveryCohort.single(),
                    List.of(DOMAIN), "replace", false, true));
            assertThrows(CompletionException.class, () -> controller.replaceCohort("default", "inc", RecoveryCohort.single(),
                    List.of(), "replace", true, true).toCompletableFuture().join());
            String incarnation = controller.replaceCohort("default", "inc", RecoveryCohort.single(), List.of(DOMAIN), "replace", true, true)
                    .toCompletableFuture().join();
            assertNotEquals("inc", incarnation);
            assertEquals(incarnation, controller.replaceCohort("default", "inc", RecoveryCohort.single(), List.of(DOMAIN), "replace", true, true)
                    .toCompletableFuture().join(), "retry is idempotent");
            assertThrows(CompletionException.class, () -> controller.replaceCohort("default", "inc", RecoveryCohort.single(), List.of(), "replace", true, true)
                    .toCompletableFuture().join(), "an idempotency token cannot authorize a different maintenance plan");
            assertThrows(CompletionException.class, () -> controller.read(DOMAIN, old).toCompletableFuture().join());
            assertThrows(CompletionException.class, () -> store.seed(context, List.of()).toCompletableFuture().join());
            assertEquals(before, connection.sync().get(RedisKeyScheme.defaults().singleKey(bucket)));
            var replacement = controller.enroll("default", RecoveryCohort.single(), "single", "new").toCompletableFuture().join();
            var gather = controller.attach(DOMAIN, replacement).toCompletableFuture().join().context();
            assertTrue(gather.epoch() > context.epoch());
            assertEquals(RecoveryPhase.GATHER, gather.phase());
            var drain = controller.join(gather).toCompletableFuture().join().context();
            assertEquals(RecoveryPhase.NORMAL, controller.ready(drain).toCompletableFuture().join().context().phase());
            var controlKey = RedisKeyScheme.defaults().controlKey(DOMAIN);
            int fields = connection.sync().hgetall(controlKey).size();
            String second = controller.replaceCohort("default", incarnation, RecoveryCohort.single(), List.of(DOMAIN), "replace-again", true, true)
                    .toCompletableFuture().join();
            assertNotEquals(incarnation, second);
            assertTrue(connection.sync().hgetall(controlKey).size() <= fields);
        }
    }
    @Test void repeatedEpochsOverwriteReadinessInsteadOfGrowingHistory() {
        try (var connection = client().connect()) {
            newStore(connection).registerPolicies(List.of(new PolicyBinding(DOMAIN, "root", io.quotaflow.core.Scope.GLOBAL,
                    io.quotaflow.core.Algorithm.GCRA))).toCompletableFuture().join();
            var controller = new RedisRecoveryController(connection, Duration.ofSeconds(2)); var cohort = RecoveryCohort.single();
            controller.provisionCohort("default", cohort, "inc", true, true).toCompletableFuture().join();
            controller.provisionDomain(DOMAIN, cohort, "inc", FP, true, true).toCompletableFuture().join();
            var owner = controller.enroll("default", cohort, "single", "owner").toCompletableFuture().join();
            var context = controller.attach(DOMAIN, owner).toCompletableFuture().join().context(); int fields = 0;
            for (int epoch = 0; epoch < 100; epoch++) {
                if (context.phase() == RecoveryPhase.NORMAL) context = controller.begin(context, FP, 0).toCompletableFuture().join().context();
                context = controller.join(context).toCompletableFuture().join().context();
                var normal = controller.ready(context).toCompletableFuture().join(); context = normal.context();
                assertEquals(context.epoch(), normal.retiredEpoch());
                int current = connection.sync().hgetall(RedisKeyScheme.defaults().controlKey(DOMAIN)).size();
                if (epoch == 1) fields = current;
                if (epoch > 1) assertEquals(fields, current);
            }
        }
    }
}
