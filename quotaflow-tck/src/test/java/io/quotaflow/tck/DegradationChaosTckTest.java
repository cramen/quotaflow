package io.quotaflow.tck;

import static io.quotaflow.testing.TestIdentities.key;
import io.quotaflow.core.store.BucketIdentity;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.RedisClient;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import io.quotaflow.core.Verdict;
import io.quotaflow.core.store.ChainResult;
import io.quotaflow.core.store.LevelRequest;
import io.quotaflow.fallback.DegradationListener;
import io.quotaflow.fallback.DegradationState;
import io.quotaflow.fallback.FallbackConfig;
import io.quotaflow.fallback.FallbackRateLimitStore;
import io.quotaflow.store.redis.RedisRateLimitStore;
import io.quotaflow.store.redis.RedisStoreConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Degradation and recovery chaos: a dedicated Redis container is paused
 * under load (every store call stalls into a timeout — the "store
 * unavailable or times out" scenario) and later unpaused. Two independent
 * fallback-wrapped instances must degrade within a bounded time, keep serving
 * decisions locally with zero exceptions escaping to callers, keep the summed
 * degraded flow within the global limit, and recover without a pass-through
 * spike.
 *
 * <p>The container is dedicated (not the shared one) because the tests freeze
 * it. The outage is a pause rather than a stop/start because restarting a
 * container re-maps the host port, leaving a client bound to the old endpoint
 * permanently unreachable; a pause additionally preserves the stored state,
 * so recovery exercises the conservative seed merge against real data. Waits
 * are poll-based with generous deadlines; the only fixed sleeps are the
 * scenario's outage and measurement windows themselves.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DegradationChaosTckTest {

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);

    private static final int INSTANCES = 2;
    private static final int THREADS_PER_INSTANCE = 2;

    private static final RedisStoreConfig STORE_CONFIG =
            new RedisStoreConfig(Duration.ofMillis(200), Duration.ofSeconds(2));
    private static final FallbackConfig FALLBACK_CONFIG = new FallbackConfig(
            3, Duration.ofMillis(500), Duration.ofSeconds(2), INSTANCES, 10_000);

    private static final List<RedisClient> CLIENTS = new ArrayList<>();

    @BeforeAll
    static void startContainer() {
        Assumptions.assumeTrue(
                DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
        REDIS.start();
        RedisClient provisioning = RedisClient.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        try (var connection = provisioning.connect()) {
            new io.quotaflow.store.redis.RedisNamespaceAdmin(connection).provisionFresh("default", true);
        } finally { provisioning.shutdown(); }
    }

    @AfterAll
    static void shutdown() {
        CLIENTS.forEach(RedisClient::shutdown);
    }

    private static void pauseRedis() {
        DockerClientFactory.instance().client()
                .pauseContainerCmd(REDIS.getContainerId()).exec();
    }

    private static void unpauseRedis() {
        DockerClientFactory.instance().client()
                .unpauseContainerCmd(REDIS.getContainerId()).exec();
    }

    /** Counts fallback decisions across instances; this is the degraded flow. */
    private static final class CountingListener implements DegradationListener {
        private final AtomicLong degradedAllowed;

        CountingListener(AtomicLong degradedAllowed) {
            this.degradedAllowed = degradedAllowed;
        }

        @Override
        public void onTransition(DegradationState from, DegradationState to, String reason) {
        }

        @Override
        public void onFallbackDecision(String policyId, String keyGroup, Verdict verdict) {
            if (verdict == Verdict.ALLOWED) {
                degradedAllowed.incrementAndGet();
            }
        }
    }

    /** Continuous load over {@link #INSTANCES} fallback-wrapped stores. */
    private static final class LoadHarness implements AutoCloseable {
        private final List<FallbackRateLimitStore> stores = new ArrayList<>();
        private final List<RedisRateLimitStore> primaries = new ArrayList<>();
        private final AtomicLong allowedTotal = new AtomicLong();
        private final AtomicLong window = new AtomicLong();
        private final AtomicLong degradedAllowed = new AtomicLong();
        private final CopyOnWriteArrayList<Throwable> escaped = new CopyOnWriteArrayList<>();
        private final ExecutorService pool =
                Executors.newFixedThreadPool(INSTANCES * THREADS_PER_INSTANCE);
        private final List<LevelRequest> chain;
        private volatile boolean running;
        private volatile boolean throttled;

        LoadHarness(String storageKey, Limit limit) {
            String namespace = "chaos-" + UUID.randomUUID();
            var domain = new io.quotaflow.core.store.QuotaDomain(namespace, "quota");
            var identity = new BucketIdentity(domain, "quota", io.quotaflow.core.Scope.GLOBAL, storageKey);
            this.chain = List.of(new LevelRequest(identity, limit, Algorithm.TOKEN_BUCKET, 1));
            var policies = io.quotaflow.core.PolicySet.compile(List.of(io.quotaflow.core.RateLimitPolicy.builder("quota")
                    .scope(io.quotaflow.core.Scope.GLOBAL).limit(limit).build()));
            var cohort = new io.quotaflow.core.store.RecoveryCohort(List.of("a", "b"));
            var bindings = List.of(io.quotaflow.core.store.PolicyBinding.of(identity, Algorithm.TOKEN_BUCKET));
            String uri = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
            for (int i = 0; i < INSTANCES; i++) {
                RedisClient client = io.quotaflow.store.redis.RedisClientFactory.createClient(uri, Duration.ofSeconds(2), STORE_CONFIG.commandTimeout());
                CLIENTS.add(client);
                RedisRateLimitStore primary = RedisRateLimitStore.create(client, STORE_CONFIG);
                primaries.add(primary);
                if (i == 0) try (var connection = client.connect()) {
                    new io.quotaflow.store.redis.RedisNamespaceAdmin(connection).provisionFresh(namespace, true);
                    primary.registerPolicies(bindings).toCompletableFuture().join();
                    var admin = new io.quotaflow.store.redis.RedisRecoveryController(connection, Duration.ofSeconds(2));
                    admin.provisionCohort(namespace, cohort, "initial", true, true).toCompletableFuture().join();
                    admin.provisionDomain(domain, cohort, "initial", policies.recoveryFingerprint("quota"), true, true).toCompletableFuture().join();
                }
                var delegate = primary.recoveryPrimary(namespace, Duration.ofMillis(200), true);
                var adapter = (io.quotaflow.core.store.RecoveryPrimary) java.lang.reflect.Proxy.newProxyInstance(
                        io.quotaflow.core.store.RecoveryPrimary.class.getClassLoader(), new Class<?>[]{io.quotaflow.core.store.RecoveryPrimary.class},
                        (proxy, method, args) -> {
                            if (throttled) return java.util.concurrent.CompletableFuture.failedFuture(new io.quotaflow.core.store.PrimaryDispatchException(
                                    io.quotaflow.core.store.PrimaryDispatchException.Outcome.NOT_DISPATCHED));
                            try { return method.invoke(delegate, args); }
                            catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                        });
                var settings = new io.quotaflow.fallback.RecoverySettings(namespace, "chaos", cohort.members().get(i), cohort,
                        10_000, 4096, Duration.ofMillis(100), Duration.ofSeconds(2));
                var store = new FallbackRateLimitStore(adapter, settings, List.of(new CountingListener(degradedAllowed)));
                store.registerPolicies(bindings).toCompletableFuture().join();
                store.configureRecovery(policies, namespace, null).toCompletableFuture().join();
                stores.add(store);
            }
        }

        void start() {
            running = true;
            for (FallbackRateLimitStore store : stores) {
                for (int t = 0; t < THREADS_PER_INSTANCE; t++) {
                    pool.submit(() -> {
                        while (running) {
                            if (throttled) {
                                try {
                                    Thread.sleep(10);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                                continue;
                            }
                            try {
                                ChainResult result =
                                        store.tryAcquireAll(chain).toCompletableFuture().join();
                                if (result.acquired()) {
                                    allowedTotal.incrementAndGet();
                                    window.incrementAndGet();
                                }
                            } catch (Throwable e) {
                                if (running) {
                                    escaped.add(e);
                                }
                            }
                        }
                    });
                }
            }
        }

        /**
         * Suspends traffic and control dispatch while the fixture edits quota state; {@link #resume()} restarts the flow.
         */
        void throttle() {
            throttled = true;
        }

        void resume() {
            throttled = false;
        }

        /** Polls until every instance reports {@code expected} or the deadline passes. */
        boolean awaitState(DegradationState expected, Duration deadline) throws InterruptedException {
            long deadlineNanos = System.nanoTime() + deadline.toNanos();
            while (System.nanoTime() < deadlineNanos) {
                if (stores.stream().allMatch(store -> store.state() == expected)) {
                    return true;
                }
                Thread.sleep(20);
            }
            return stores.stream().allMatch(store -> store.state() == expected);
        }

        /** Measures the allowed flow over a window. */
        long measureWindow(Duration windowDuration) throws InterruptedException {
            window.set(0);
            Thread.sleep(windowDuration.toMillis());
            return window.get();
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);
            pool.shutdownNow();
            stores.forEach(FallbackRateLimitStore::close);
            primaries.forEach(RedisRateLimitStore::close);
        }
    }

    @Test
    @Order(1)
    void outageDegradesWithinBoundsAndKeepsSummedFlowUnderTheGlobalLimit() throws Exception {
        // capacity 100, effectively no refill: exact accounting over the whole outage
        Limit limit = new Limit(100, 1, Duration.ofHours(1));
        String key = "chaos:global:" + UUID.randomUUID();
        try (LoadHarness load = new LoadHarness(key, limit)) {
            assertTrue(load.awaitState(DegradationState.CLOSED, Duration.ofSeconds(10)));
            load.start();
            long healthyDeadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (load.allowedTotal.get() == 0 && System.nanoTime() < healthyDeadline) {
                Thread.sleep(20);
            }
            assertTrue(load.allowedTotal.get() > 0, "healthy flow before the outage");

            pauseRedis();
            try {
                long stoppedAt = System.nanoTime();
                boolean degraded = load.awaitState(DegradationState.OPEN, Duration.ofSeconds(15));
                long elapsedMillis = (System.nanoTime() - stoppedAt) / 1_000_000;
                assertTrue(degraded, "both instances must degrade within a bounded time; last observed"
                        + " after " + elapsedMillis + " ms (store timeout " + STORE_CONFIG.commandTimeout()
                        + ", threshold " + FALLBACK_CONFIG.failureThreshold() + ")");

                // stay under load through several probe cycles
                Thread.sleep(3_000);

                assertTrue(load.escaped.isEmpty(),
                        "zero exceptions escape to callers during the outage: " + load.escaped);
                long degradedFlow = load.degradedAllowed.get();
                org.junit.jupiter.api.Assertions.assertEquals(0, degradedFlow, "cold shares cannot create another burst before the first refill");
                assertTrue(degradedFlow <= 100,
                        "summed degraded flow " + degradedFlow + " must not exceed the global limit 100"
                                + " (two instances at half the limit each)");
            } finally {
                unpauseRedis();
            }
        }
    }

    @Test
    @Order(2)
    void recoverySeedsStateAndDoesNotSpikeBeyondSteadyState() throws Exception {
        // steady state 100 tokens/second with a burst capacity of 100
        Limit limit = new Limit(100, 100, Duration.ofSeconds(1));
        String key = "chaos:global:" + UUID.randomUUID();
        try (LoadHarness load = new LoadHarness(key, limit)) {
            load.start();
            // warmup: drain the initially full bucket and settle at the refill rate
            Thread.sleep(2_000);
            long baseline = load.measureWindow(Duration.ofMillis(500));
            assertTrue(baseline >= 20 && baseline <= 80,
                    "steady state should be near 50 per 500 ms, measured " + baseline);

            pauseRedis();
            try {
                assertTrue(load.awaitState(DegradationState.OPEN, Duration.ofSeconds(15)),
                        "both instances degrade");
                // outage under load: the local buckets are drained when the store returns
                Thread.sleep(3_000);

                // Simulate lost quota buckets while retaining the authoritative
                // namespace identity registry. Lost registry metadata is a
                // separate configuration failure, not a valid recovery probe.
                // Without seeding, fresh buckets would violate the burst bound.
                load.throttle();
                Thread.sleep(500); // in-flight calls settle (store timeout is 200 ms)
            } finally {
                unpauseRedis();
            }
            RedisClient admin = RedisClient.create(
                    "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
            try (var connection = admin.connect()) {
                for (String bucket : connection.sync().keys("qf:v2:{*}:*")) {
                    if (!bucket.endsWith(":control")) connection.sync().del(bucket);
                }
            } finally {
                admin.shutdown();
            }
            load.resume();
            assertTrue(load.awaitState(DegradationState.CLOSED, Duration.ofSeconds(60)),
                    "both instances probe, seed and close within a bounded time");

            long postRecovery = load.measureWindow(Duration.ofMillis(500));
            assertTrue(postRecovery > 0, "flow resumed after recovery");
            assertTrue(postRecovery <= baseline * 1.2,
                    "post-recovery flow " + postRecovery + " must stay within 1.2x of the steady"
                            + " state " + baseline + " (seeded buckets prevent the spike)");
            assertTrue(load.escaped.isEmpty(),
                    "zero exceptions escape to callers across the outage: " + load.escaped);
        }
    }
}
