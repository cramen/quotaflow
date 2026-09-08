package io.quotaflow.fallback;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.Verdict;
import io.quotaflow.core.store.BatchRateLimitStore;
import io.quotaflow.core.store.BucketState;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.core.store.LocalRateLimitStore;
import io.quotaflow.core.store.StateSeeder;
import io.quotaflow.core.store.StoreResult;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The fallback store end to end with a controllable fake primary: passthrough
 * while healthy, local decisions with zero primary calls while open,
 * conservative scaling, probe-based recovery with state seeding, and listener
 * observability through a full outage-recovery cycle.
 */
class FallbackRateLimitStoreTest {

    private static final long SECOND = 1_000_000_000L;
    private static final long HOUR_NANOS = 3_600_000_000_000L;

    /** Controllable primary: delegates to a real local store while healthy, fails on demand. */
    private static class FakePrimary implements BatchRateLimitStore {
        private final LocalRateLimitStore delegate;
        volatile boolean failing;
        final AtomicInteger batchCalls = new AtomicInteger();
        final AtomicInteger singleCalls = new AtomicInteger();

        FakePrimary(LocalRateLimitStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
            batchCalls.incrementAndGet();
            if (failing) {
                return CompletableFuture.failedFuture(new RuntimeException("store is down"));
            }
            return delegate.tryAcquireAll(chain);
        }

        @Override
        public StoreResult tryAcquire(String storageKey, Limit limit, Algorithm algorithm, long weight) {
            singleCalls.incrementAndGet();
            if (failing) {
                throw new RuntimeException("store is down");
            }
            return delegate.tryAcquire(storageKey, limit, algorithm, weight);
        }

        @Override
        public CompletionStage<StoreResult> tryAcquireAsync(
                String storageKey, Limit limit, Algorithm algorithm, long weight) {
            singleCalls.incrementAndGet();
            if (failing) {
                return CompletableFuture.failedFuture(new RuntimeException("store is down"));
            }
            return delegate.tryAcquireAsync(storageKey, limit, algorithm, weight);
        }
    }

    private static final class RecordingSeeder implements StateSeeder {
        final List<String> leaves = new CopyOnWriteArrayList<>();
        final List<List<BucketState>> buckets = new CopyOnWriteArrayList<>();
        volatile boolean fail;

        @Override
        public CompletionStage<Void> seed(String chainLeafStorageKey, List<BucketState> entries) {
            leaves.add(chainLeafStorageKey);
            buckets.add(entries);
            return fail
                    ? CompletableFuture.failedFuture(new RuntimeException("seed write failed"))
                    : CompletableFuture.completedFuture(null);
        }

        int totalSeeded() {
            return buckets.stream().mapToInt(List::size).sum();
        }
    }

    private static final class RecordingListener implements DegradationListener {
        final List<String> transitions = new CopyOnWriteArrayList<>();
        final List<String> decisions = new CopyOnWriteArrayList<>();

        @Override
        public void onTransition(DegradationState from, DegradationState to, String reason) {
            transitions.add(from + "->" + to + ":" + reason);
        }

        @Override
        public void onFallbackDecision(String policyId, String keyGroup, Verdict verdict) {
            decisions.add(policyId + "/" + keyGroup + "/" + verdict);
        }
    }

    private final AtomicLong nanos = new AtomicLong();
    private final LocalRateLimitStore delegate = new LocalRateLimitStore(nanos::get);
    private final LocalRateLimitStore local = new LocalRateLimitStore(nanos::get);
    private final FakePrimary primary = new FakePrimary(delegate);
    private final RecordingSeeder seeder = new RecordingSeeder();
    private final RecordingListener listener = new RecordingListener();

    private FallbackRateLimitStore newStore(int expectedInstances, int maxSeedEntries) {
        FallbackConfig config = new FallbackConfig(3, Duration.ofSeconds(1), Duration.ofSeconds(4),
                expectedInstances, maxSeedEntries);
        return new FallbackRateLimitStore(primary, local, seeder, config, List.of(listener),
                nanos::get, FallbackRateLimitStoreTest::scopeAsKeyGroup);
    }

    private static String scopeAsKeyGroup(String storageKey) {
        int first = storageKey.indexOf(':');
        return storageKey.substring(first + 1, storageKey.indexOf(':', first + 1));
    }

    private static List<LevelRequest> chain(String key, Limit limit) {
        return List.of(new LevelRequest(key, limit, Algorithm.TOKEN_BUCKET, 1));
    }

    private static ChainResult join(FallbackRateLimitStore store, List<LevelRequest> chain) {
        return store.tryAcquireAll(chain).toCompletableFuture().join();
    }

    /** No refill during a test: one token per hour. */
    private static Limit noRefill(long capacity) {
        return new Limit(capacity, 1, Duration.ofHours(1));
    }

    /** Trips the breaker with three failing calls on a fast-refilling throwaway key. */
    private void trip(FallbackRateLimitStore store) {
        primary.failing = true;
        Limit fastRefill = new Limit(1, 1000, Duration.ofSeconds(1));
        for (int i = 0; i < 3; i++) {
            join(store, chain("trip:global:t", fastRefill));
        }
        assertEquals(DegradationState.OPEN, store.state());
    }

    @Test
    void closedPassesThroughUnchanged() {
        FallbackRateLimitStore store = newStore(1, 100);
        Limit limit = noRefill(10);
        List<LevelRequest> chain = chain("p:tenant:acme", limit);
        ChainResult viaWrapper = join(store, chain);
        assertTrue(viaWrapper.acquired());
        assertEquals(9, viaWrapper.remaining());
        assertEquals(1, primary.batchCalls.get());
        assertEquals(0, local.cellCount(), "the local store is untouched while healthy");
        assertTrue(listener.transitions.isEmpty());
        assertTrue(listener.decisions.isEmpty());
    }

    @Test
    void openServesLocallyWithZeroPrimaryCalls() {
        FallbackRateLimitStore store = newStore(1, 100);
        Limit limit = noRefill(10);
        trip(store);
        int callsAtOpen = primary.batchCalls.get();
        for (int i = 0; i < 5; i++) {
            assertTrue(join(store, chain("p:tenant:acme", limit)).acquired());
        }
        assertEquals(callsAtOpen, primary.batchCalls.get(), "OPEN mode makes zero primary calls");
        assertTrue(local.cellCount() >= 2, "local state exists for the served keys");
        assertEquals(5 + 3, listener.decisions.size(), "every locally served request is reported");
    }

    @Test
    void callerErrorsPropagateWithoutDegrading() {
        FallbackRateLimitStore store = newStore(1, 100);
        Limit limit = noRefill(10);
        assertThrows(IllegalArgumentException.class,
                () -> store.tryAcquire("p:tenant:acme", limit, Algorithm.TOKEN_BUCKET, 0));
        assertThrows(IllegalArgumentException.class, () -> store.tryAcquireAll(List.of()));
        assertEquals(DegradationState.CLOSED, store.state());
        assertTrue(join(store, chain("p:tenant:acme", limit)).acquired());
    }

    @Test
    void tokenBucketScalingDividesCapacityAndRefillWithFloors() {
        FallbackRateLimitStore store = newStore(4, 100);
        trip(store);
        // capacity 8 / 4 = 2, refill 8 / 4 = 2 per second -> 500 ms per token
        Limit limit = new Limit(8, 8, Duration.ofSeconds(1));
        List<LevelRequest> chain = chain("p:tenant:acme", limit);
        ChainResult first = join(store, chain);
        ChainResult second = join(store, chain);
        ChainResult rejected = join(store, chain);
        assertTrue(first.acquired());
        assertEquals(1, first.remaining());
        assertTrue(second.acquired());
        assertEquals(0, second.remaining());
        assertFalse(rejected.acquired());
        assertEquals(0, rejected.firedLevelIndex());
        assertEquals(500, rejected.retryAfterMillis());

        // floors: capacity 3 / 4 -> 1, refill 2 / 4 -> 1
        Limit tiny = new Limit(3, 2, Duration.ofSeconds(1));
        List<LevelRequest> tinyChain = chain("p:tenant:other", tiny);
        assertTrue(join(store, tinyChain).acquired());
        ChainResult tinyRejected = join(store, tinyChain);
        assertFalse(tinyRejected.acquired());
        assertEquals(1000, tinyRejected.retryAfterMillis());
    }

    @Test
    void scaledDecisionsMatchHealthyModeShape() {
        // healthy reference: the scaled limit evaluated by the same local batch path
        LocalRateLimitStore reference = new LocalRateLimitStore(nanos::get);
        Limit scaled = new Limit(2, 2, Duration.ofSeconds(1));
        FallbackRateLimitStore store = newStore(4, 100);
        trip(store);
        Limit limit = new Limit(8, 8, Duration.ofSeconds(1));
        for (int i = 0; i < 3; i++) {
            ChainResult degraded = join(store, chain("p:tenant:acme", limit));
            ChainResult healthy = reference.tryAcquireAll(
                    List.of(new LevelRequest("p:tenant:acme", scaled, Algorithm.TOKEN_BUCKET, 1)))
                    .toCompletableFuture().join();
            assertEquals(healthy, degraded,
                    "fired level, remaining and retry-after are computed identically");
        }
    }

    @Test
    void gcraScalingMultipliesTheEmissionInterval() {
        FallbackRateLimitStore store = newStore(2, 100);
        trip(store);
        // healthy interval 250 ms; degraded interval 500 ms; burst capacity unchanged
        Limit limit = new Limit(4, 4, Duration.ofSeconds(1));
        List<LevelRequest> chain = List.of(new LevelRequest("p:tenant:acme", limit, Algorithm.GCRA, 1));
        for (long expectedRemaining = 3; expectedRemaining >= 0; expectedRemaining--) {
            ChainResult result = join(store, chain);
            assertTrue(result.acquired());
            assertEquals(expectedRemaining, result.remaining());
        }
        ChainResult rejected = join(store, chain);
        assertFalse(rejected.acquired());
        assertEquals(500, rejected.retryAfterMillis());
    }

    @Test
    void recoveryReplaysScaledLocalStateTranslatedToFullLimitThenCloses() {
        FallbackRateLimitStore store = newStore(2, 100);
        Limit limit = noRefill(10);
        trip(store); // three locally served calls on the trip key
        List<LevelRequest> chain = chain("p:tenant:acme", limit);
        join(store, chain); // local scaled capacity 5, consumes 1 -> remaining 4
        ChainResult second = join(store, chain);
        assertEquals(3, second.remaining());

        primary.failing = false;
        nanos.addAndGet(SECOND);
        ChainResult probe = join(store, chain);
        assertTrue(probe.acquired(), "the probe rides a real request to the healed primary");
        assertEquals(9, probe.remaining(), "the probe consumed from the distributed bucket");

        assertEquals(DegradationState.CLOSED, store.state());
        assertEquals(1, seeder.leaves.size());
        assertEquals("p:tenant:acme", seeder.leaves.get(0));
        List<BucketState> seeded = seeder.buckets.get(0);
        assertEquals(1, seeded.size(), "the refilled trip key was swept from the snapshot");
        BucketState bucket = seeded.get(0);
        assertEquals("p:tenant:acme", bucket.storageKey());
        assertEquals(limit, bucket.limit(), "seeding uses the original unscaled limit");
        // local remaining 3 out of scaled capacity 5 -> 3 * 2 = 6 of the shared capacity unused
        assertEquals(6, bucket.remaining());

        ChainResult afterRecovery = join(store, chain);
        assertTrue(afterRecovery.acquired());
        assertEquals(8, afterRecovery.remaining(), "post-recovery decisions are distributed again");
        assertEquals(List.of(
                "CLOSED->OPEN:primary store call failed (RuntimeException)",
                "OPEN->HALF_OPEN:open duration elapsed; admitting one probe request",
                "HALF_OPEN->CLOSED:probe succeeded and 1 local buckets replayed"),
                listener.transitions);
    }

    @Test
    void seedingFailureExtendsOpenAndRecoveryRetriesLater() {
        FallbackRateLimitStore store = newStore(2, 100);
        Limit limit = noRefill(10);
        trip(store);
        join(store, chain("p:tenant:acme", limit));
        seeder.fail = true;
        primary.failing = false;
        nanos.addAndGet(SECOND);
        assertTrue(join(store, chain("p:tenant:acme", limit)).acquired(), "probe succeeds");
        assertEquals(DegradationState.OPEN, store.state(), "seeding failure extends the outage");
        int primaryCallsAfterFailedSeed = primary.batchCalls.get();
        join(store, chain("p:tenant:acme", limit));
        assertEquals(primaryCallsAfterFailedSeed, primary.batchCalls.get(),
                "still zero primary calls while reopened");

        seeder.fail = false;
        nanos.addAndGet(SECOND);
        join(store, chain("p:tenant:acme", limit));
        assertEquals(DegradationState.OPEN, store.state(), "backoff doubled the open duration");
        nanos.addAndGet(SECOND);
        join(store, chain("p:tenant:acme", limit));
        assertEquals(DegradationState.CLOSED, store.state());
        assertTrue(listener.transitions.contains(
                "HALF_OPEN->OPEN:seeding failed (RuntimeException)"));
    }

    @Test
    void probeFailureReopensAndTheProbeCallerStillGetsALocalDecision() {
        FallbackRateLimitStore store = newStore(2, 100);
        Limit limit = noRefill(10);
        trip(store);
        nanos.addAndGet(SECOND);
        ChainResult probe = join(store, chain("p:tenant:acme", limit));
        assertTrue(probe.acquired(), "a failed probe is served locally, no exception escapes");
        assertEquals(DegradationState.OPEN, store.state());
        assertTrue(listener.transitions.contains("HALF_OPEN->OPEN:probe failed (RuntimeException)"));
        assertTrue(seeder.leaves.isEmpty(), "no seeding happens without a successful probe");
    }

    @Test
    void singleLevelCallsDegradeAndRecover() {
        FallbackRateLimitStore store = newStore(1, 100);
        Limit limit = noRefill(5);
        primary.failing = true;
        for (int i = 0; i < 3; i++) {
            store.tryAcquire("p:user:alice", limit, Algorithm.TOKEN_BUCKET, 1);
        }
        assertEquals(DegradationState.OPEN, store.state());
        int callsAtOpen = primary.singleCalls.get();
        StoreResult localDecision = store.tryAcquire("p:user:alice", limit, Algorithm.TOKEN_BUCKET, 1);
        assertTrue(localDecision.acquired());
        assertEquals(callsAtOpen, primary.singleCalls.get());

        primary.failing = false;
        nanos.addAndGet(SECOND);
        StoreResult probe = store.tryAcquire("p:user:alice", limit, Algorithm.TOKEN_BUCKET, 1);
        assertTrue(probe.acquired());
        assertEquals(4, probe.remaining(), "the probe hit the distributed bucket");
        assertEquals(DegradationState.CLOSED, store.state());
        assertEquals(1, seeder.totalSeeded());
    }

    @Test
    void asyncSingleLevelFailuresDegradeWithoutEscaping() {
        FallbackRateLimitStore store = newStore(1, 100);
        Limit limit = noRefill(5);
        primary.failing = true;
        for (int i = 0; i < 4; i++) {
            StoreResult result = store
                    .tryAcquireAsync("p:user:bob", limit, Algorithm.TOKEN_BUCKET, 1)
                    .toCompletableFuture().join();
            assertTrue(result.acquired());
        }
        assertEquals(DegradationState.OPEN, store.state());
        assertEquals(4, listener.decisions.size());
    }

    @Test
    void callerErrorsFromAsyncPrimariesPropagate() {
        FallbackRateLimitStore brokenInput = new FallbackRateLimitStore(
                new FakePrimary(delegate) {
                    @Override
                    public CompletionStage<ChainResult> tryAcquireAll(List<LevelRequest> chain) {
                        return CompletableFuture.failedFuture(new IllegalArgumentException("bad chain"));
                    }
                },
                local, seeder,
                new FallbackConfig(3, Duration.ofSeconds(1), Duration.ofSeconds(4), 1, 100),
                List.of(listener), nanos::get, FallbackRateLimitStoreTest::scopeAsKeyGroup);
        CompletionStage<ChainResult> result = brokenInput.tryAcquireAll(chain("p:tenant:acme", noRefill(5)));
        assertThrows(CompletionException.class, () -> result.toCompletableFuture().join());
        assertEquals(DegradationState.CLOSED, brokenInput.state());
    }

    @Test
    void seedingIsCappedAndOverflowIsSkipped() {
        FallbackRateLimitStore store = newStore(1, 1);
        Limit limit = noRefill(10);
        trip(store);
        join(store, chain("p:tenant:first", limit));
        join(store, chain("p:tenant:second", limit));
        primary.failing = false;
        nanos.addAndGet(SECOND);
        join(store, chain("p:tenant:first", limit));
        assertEquals(DegradationState.CLOSED, store.state());
        assertEquals(1, seeder.totalSeeded(), "the hard cap bounds the replayed entries");
        assertTrue(seeder.leaves.get(0).startsWith("p:tenant:"));
    }

    @Test
    void listenersSeeKeyGroupOnlyThroughTheFullCycle() {
        FallbackRateLimitStore store = newStore(1, 100);
        Limit limit = noRefill(2);
        trip(store);
        join(store, chain("p:tenant:raw-secret-1", limit));
        join(store, chain("p:tenant:raw-secret-1", limit));
        join(store, chain("p:tenant:raw-secret-1", limit)); // rejected: capacity 2
        primary.failing = false;
        nanos.addAndGet(SECOND);
        join(store, chain("p:tenant:raw-secret-1", limit));
        assertEquals(DegradationState.CLOSED, store.state());

        assertEquals(List.of("p/tenant/ALLOWED", "p/tenant/ALLOWED", "p/tenant/REJECTED"),
                listener.decisions.subList(3, 6));
        for (String event : listener.decisions) {
            assertFalse(event.contains("raw-secret-1"), "raw keys never reach listeners");
        }
        for (String transition : listener.transitions) {
            assertFalse(transition.contains("raw-secret-1"), "raw keys never appear in reasons");
            assertFalse(transition.endsWith(":"), "every transition carries a non-empty reason");
        }
        assertEquals(3, listener.transitions.size(), "degrade, probe, recover");
    }

    @Test
    void stateSeederNoOpCompletes() {
        assertTrue(StateSeeder.noOp().seed("p:tenant:x", List.of()).toCompletableFuture().isDone());
    }

    @Test
    void configValidation() {
        assertThrows(IllegalArgumentException.class,
                () -> new FallbackConfig(0, Duration.ofSeconds(1), Duration.ofSeconds(4), 1, 100));
        assertThrows(IllegalArgumentException.class,
                () -> new FallbackConfig(1, Duration.ZERO, Duration.ofSeconds(4), 1, 100));
        assertThrows(IllegalArgumentException.class,
                () -> new FallbackConfig(1, Duration.ofSeconds(4), Duration.ofSeconds(1), 1, 100));
        assertThrows(IllegalArgumentException.class,
                () -> new FallbackConfig(1, Duration.ofSeconds(1), Duration.ofSeconds(4), 0, 100));
        assertThrows(IllegalArgumentException.class,
                () -> new FallbackConfig(1, Duration.ofSeconds(1), Duration.ofSeconds(4), 1, 0));
        assertThrows(NullPointerException.class,
                () -> new FallbackConfig(1, null, Duration.ofSeconds(4), 1, 100));
        assertThrows(NullPointerException.class,
                () -> new FallbackConfig(1, Duration.ofSeconds(1), null, 1, 100));
        assertEquals(3, FallbackConfig.defaults().failureThreshold());
    }

    @Test
    void constructorValidation() {
        FallbackConfig config = FallbackConfig.defaults();
        assertThrows(NullPointerException.class,
                () -> new FallbackRateLimitStore(null, seeder, config, List.of()));
        assertThrows(NullPointerException.class,
                () -> new FallbackRateLimitStore(primary, null, config, List.of()));
        assertThrows(NullPointerException.class,
                () -> new FallbackRateLimitStore(primary, seeder, null, List.of()));
        assertThrows(NullPointerException.class,
                () -> new FallbackRateLimitStore(primary, seeder, config, null));
        FallbackRateLimitStore store = new FallbackRateLimitStore(primary, seeder, config, List.of());
        assertEquals(DegradationState.CLOSED, store.state());
    }
}
