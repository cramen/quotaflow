package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.testing.RecoveryPrimaryFixture;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class RecoveryEnvelopeTest {
    @ParameterizedTest @EnumSource(Algorithm.class)
    void generatedFleetHistoriesStayInsideTheContinuousGlobalEnvelope(Algorithm algorithm) throws Exception {
        var limit = new Limit(19, 7, Duration.ofSeconds(1));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).algorithm(algorithm).limit(limit).build()));
        var cohort = new RecoveryCohort(List.of("0", "1", "2", "3"));
        var time = new AtomicLong();
        var stores = new ArrayList<FallbackRateLimitStore>(); var primaries = new ArrayList<RecoveryPrimaryFixture>();
        var flows = new ArrayList<DefaultQuotaFlow>();
        try {
            for (String member : cohort.members()) {
                var primary = new RecoveryPrimaryFixture(policies); primaries.add(primary);
                var store = new FallbackRateLimitStore(primary, new RecoverySettings("default", "test", member, cohort, 20, 20,
                        Duration.ofMillis(20), Duration.ofSeconds(2)), List.of(), time::get);
                stores.add(store); flows.add(DefaultQuotaFlow.builder(policies, store).build());
            }
            FallbackRateLimitStoreTest.await(() -> stores.stream().allMatch(store -> store.state() == DegradationState.CLOSED));
            primaries.forEach(primary -> primary.available = false);
            for (var flow : flows) {
                assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
                assertFalse(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            }
            // Credit is measured in represented emission-interval units, without floating point.
            long maximum = limit.capacity() * limit.emissionIntervalNanos(), credit = maximum;
            var random = new Random(17429); long admitted = 0;
            for (int event = 0; event < 10_000; event++) {
                long elapsed = random.nextInt(20_000_000);
                time.addAndGet(elapsed); credit = Math.min(maximum, credit + elapsed);
                long weight = 1 + random.nextInt(6);
                var decision = flows.get(random.nextInt(flows.size())).tryAcquire("quota", RateLimitContext.empty(), weight);
                if (decision.isAllowed()) {
                    long cost = weight * limit.emissionIntervalNanos();
                    assertTrue(credit >= cost, "fleet exceeded the exact global token-bucket envelope at event " + event);
                    credit -= cost; admitted += weight;
                }
            }
            assertTrue(admitted > 100, "the conservative fleet must still earn useful refill credit");
        } finally { stores.forEach(FallbackRateLimitStore::close); }
    }
    @ParameterizedTest @EnumSource(Algorithm.class)
    void aLaterSelectivePartitionIsOutsideTheGlobalRecoveryEnvelopeGuarantee(Algorithm algorithm) throws Exception {
        var limit = new Limit(10, 10, Duration.ofSeconds(1));
        var policies = PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL).algorithm(algorithm).limit(limit).build()));
        var time = new AtomicLong(); var sharedHealthy = new LocalRateLimitStore(time::get);
        var aPrimary = new RecoveryPrimaryFixture(policies); var bPrimary = new RecoveryPrimaryFixture(policies);
        aPrimary.accounting = sharedHealthy::tryAcquireAll; bPrimary.accounting = sharedHealthy::tryAcquireAll;
        var cohort = new RecoveryCohort(List.of("a", "b"));
        try (var a = new FallbackRateLimitStore(aPrimary, new RecoverySettings("default", "test", "a", cohort, 10, 10,
                     Duration.ofMillis(20), Duration.ofSeconds(1)), List.of(), time::get);
             var b = new FallbackRateLimitStore(bPrimary, new RecoverySettings("default", "test", "b", cohort, 10, 10,
                     Duration.ofMillis(20), Duration.ofSeconds(1)), List.of(), time::get)) {
            var fa = DefaultQuotaFlow.builder(policies, a).build(); var fb = DefaultQuotaFlow.builder(policies, b).build();
            FallbackRateLimitStoreTest.await(() -> a.state() == DegradationState.CLOSED && b.state() == DegradationState.CLOSED);
            assertTrue(fa.tryAcquire("quota", RateLimitContext.empty(), 10).isAllowed());
            bPrimary.available = false;
            assertFalse(fb.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            assertFalse(fb.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            time.set(200_000_000L);
            assertTrue(fa.tryAcquire("quota", RateLimitContext.empty(), 2).isAllowed());
            assertTrue(fb.tryAcquire("quota", RateLimitContext.empty()).isAllowed());
            // The pooled primary earned 2 and the isolated owner earned 1: this is the documented
            // later-partition limit, not a successful recovery-barrier acceptance trace.
            assertEquals(DegradationState.CLOSED, a.state());
            assertEquals(DegradationState.OPEN, b.state());
            assertTrue(10 + 2 + 1 > limit.capacity() + time.get() / limit.emissionIntervalNanos());
        }
    }
}
