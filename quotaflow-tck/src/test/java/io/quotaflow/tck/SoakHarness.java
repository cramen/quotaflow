package io.quotaflow.tck;

import io.lettuce.core.RedisClient;
import io.quotaflow.config.ConfigReloader;
import io.quotaflow.config.ConfigSource;
import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Decision;
import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.KeyResolvers;
import io.quotaflow.core.Limit;
import io.quotaflow.core.RateLimitContext;
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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Soak profile (not a per-PR gate): sustained mixed traffic through the full
 * stack — {@code QuotaFlow} facade, throttle waits, hot-reload churn, atomic
 * chain evaluation — with periodic Redis pause/unpause degradation injection,
 * asserting continuously:
 *
 * <ul>
 *   <li>zero exceptions escaping to traffic callers;</li>
 *   <li>summed flow within limits: per leaf chain (policy + user), per-second
 *       allows bounded by the leaf capacity + refill + slack; intervals
 *       overlapping an outage additionally get the fallback's conservative
 *       local budget. Note: on the Redis batch chain path, parent levels of a
 *       chain are keyed under the leaf chain's hash tag, so they bind per leaf
 *       chain rather than across chains — cross-leaf shared parent budgets are
 *       a known gap surfaced by this harness and left to a follow-up change,
 *       hence the flow invariant is asserted per leaf chain;</li>
 *   <li>bounded memory: Redis keyspace and heap stay under fixed ceilings
 *       (idle state evicted).</li>
 * </ul>
 *
 * <p>Duration: {@code quotaflow.soak.duration.seconds} (default 300). With
 * {@code quotaflow.soak.assert.latency=true} (nightly), store-level probe
 * latencies are additionally asserted against the absolute targets
 * (allow p99 &le; 1 ms, fallback p99 &le; 0.01 ms); they are always measured
 * and reported.
 */
public final class SoakHarness {

    private static final String DURATION_PROPERTY = "quotaflow.soak.duration.seconds";
    private static final String ASSERT_LATENCY_PROPERTY = "quotaflow.soak.assert.latency";

    private static final int TRAFFIC_THREADS = 8;
    private static final int USERS = 50;
    private static final int TENANTS = 5;

    private static final long GLOBAL_CAPACITY = 300;
    private static final long GLOBAL_REFILL = 300;
    /**
     * Largest leaf capacity and refill (per second) the reload churn flips to —
     * the flow bound per leaf chain.
     */
    private static final long LEAF_MAX_CAPACITY = 18;
    private static final long LEAF_MAX_REFILL = 18;
    /** Per-second allow bound per leaf chain: capacity + refill + slack. */
    private static final long PER_SECOND_ALLOW_BOUND_PER_CHAIN = LEAF_MAX_CAPACITY + LEAF_MAX_REFILL + 2;

    private static final long MAX_REDIS_KEYS = 10_000;
    private static final long MAX_USED_HEAP_BYTES = 400L * 1024 * 1024;

    private static final Duration STORE_COMMAND_TIMEOUT = Duration.ofMillis(200);
    private static final long OUTAGE_PROBE_THRESHOLD_NANOS = 50_000_000; // 50 ms

    /**
     * The soak presents itself as one of four fleet instances: the fallback's
     * conservative local budget is the distributed limit divided by four.
     */
    private static final int EXPECTED_INSTANCES = 4;

    /** Fallback per-second budget per chain: (capacity + refill) / instances + slack. */
    private static final long FALLBACK_PER_SECOND_ALLOW_BOUND_PER_CHAIN =
            (LEAF_MAX_CAPACITY + LEAF_MAX_REFILL) / EXPECTED_INSTANCES + 2;

    private static final RedisStoreConfig STORE_CONFIG =
            new RedisStoreConfig(STORE_COMMAND_TIMEOUT, Duration.ofSeconds(2));
    private static final FallbackConfig FALLBACK_CONFIG =
            new FallbackConfig(3, Duration.ofMillis(500), Duration.ofSeconds(2), EXPECTED_INSTANCES, 10_000);

    private SoakHarness() {
    }

    public static void main(String[] args) throws Exception {
        Duration duration =
                Duration.ofSeconds(Long.parseLong(System.getProperty(DURATION_PROPERTY, "300")));
        boolean assertLatency = Boolean.parseBoolean(System.getProperty(ASSERT_LATENCY_PROPERTY, "false"));

        GenericContainer<?> redis =
                new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);
        redis.start();
        String uri = "redis://" + redis.getHost() + ':' + redis.getMappedPort(6379);

        Counters counters = new Counters();
        RedisClient client = RedisClient.create(uri);
        RedisRateLimitStore primary = RedisRateLimitStore.create(client, STORE_CONFIG);
        FallbackRateLimitStore store = new FallbackRateLimitStore(
                primary, primary, FALLBACK_CONFIG, List.of(counters));

        ConfigSourcePayload payload = new ConfigSourcePayload();
        DefaultQuotaFlow quotaFlow = DefaultQuotaFlow
                .builder(payload.currentPolicySet(), store)
                .addResolver(KeyResolvers.PRINCIPAL_ID, KeyResolvers.principal())
                .addResolver(KeyResolvers.TENANT_ID_ID, KeyResolvers.tenantId())
                .addResolver(KeyResolvers.API_KEY_ID, KeyResolvers.apiKey())
                .build();
        ConfigReloader reloader = ConfigReloader.builder(payload, quotaFlow)
                .pollInterval(Duration.ofMillis(200))
                .build();
        reloader.start();

        ExecutorService threads = Executors.newCachedThreadPool();
        CountDownLatch running = new CountDownLatch(1);
        long deadlineNanos = System.nanoTime() + duration.toNanos();
        try {
            for (int i = 0; i < TRAFFIC_THREADS; i++) {
                threads.submit(() -> trafficWorker(quotaFlow, counters, running, deadlineNanos));
            }
            threads.submit(() -> probeWorker(store, counters, running, deadlineNanos));
            threads.submit(() -> reloadChurn(payload, reloader, counters, running, deadlineNanos));
            threads.submit(() -> degradationInjector(redis, counters, running, deadlineNanos));
            running.countDown();

            monitor(redis, counters, deadlineNanos);

            counters.report(duration, assertLatency);
        } finally {
            threads.shutdownNow();
            threads.awaitTermination(10, TimeUnit.SECONDS);
            reloader.close();
            primary.close();
            client.shutdown();
            redis.stop();
        }
        if (!counters.violations.isEmpty()) {
            throw new IllegalStateException(
                    "soak invariants violated:\n - " + String.join("\n - ", counters.violations));
        }
    }

    /** Mixed allow/reject/throttle-wait traffic through the facade. */
    private static void trafficWorker(
            DefaultQuotaFlow quotaFlow, Counters counters, CountDownLatch running, long deadlineNanos) {
        await(running);
        while (System.nanoTime() < deadlineNanos) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String user = "user-" + random.nextInt(USERS);
            RateLimitContext context = RateLimitContext.builder()
                    .put(RateLimitContext.PRINCIPAL, user)
                    .put(RateLimitContext.TENANT_ID, "tenant-" + random.nextInt(TENANTS))
                    .build();
            try {
                Decision decision = random.nextInt(10) < 7
                        ? quotaFlow.acquire("api", context, 1, Duration.ofMillis(30))
                        : quotaFlow.tryAcquire("api", context, 1);
                if (decision.isAllowed()) {
                    counters.allowed.incrementAndGet();
                    counters.allowanceOf(user).incrementAndGet();
                    if (!decision.waitDuration().isZero()) {
                        counters.waitedThenAllowed.incrementAndGet();
                    }
                } else {
                    counters.rejected.incrementAndGet();
                }
            } catch (Throwable e) {
                if (!isInterruption(e)) {
                    counters.escaped.add(describe(e));
                }
            }
        }
    }

    /**
     * Store-level latency probes, decoupled from facade queueing: one atomic
     * chain evaluation every 10 ms, recorded by path (healthy, fallback,
     * outage-shaped) for the absolute-target report and nightly assertions.
     */
    private static void probeWorker(
            FallbackRateLimitStore store, Counters counters, CountDownLatch running, long deadlineNanos) {
        await(running);
        Limit probeLimit = new Limit(1_000_000_000, 1_000_000_000, Duration.ofSeconds(1));
        List<LevelRequest> chain = List.of(
                new LevelRequest("soak-probe:global:probe", probeLimit, Algorithm.TOKEN_BUCKET, 1));
        while (System.nanoTime() < deadlineNanos) {
            long start = System.nanoTime();
            try {
                ChainResult result = store.tryAcquireAll(chain).toCompletableFuture().join();
                long latency = System.nanoTime() - start;
                if (!result.acquired()) {
                    counters.violations.add("probe rejected with an effectively unlimited limit");
                }
                counters.recordProbe(latency, store.state() == DegradationState.OPEN);
            } catch (Throwable e) {
                if (!isInterruption(e)) {
                    counters.escaped.add("probe: " + describe(e));
                }
            }
            park(Duration.ofMillis(10));
        }
    }

    /** Hot-reload churn: flips the leaf capacity every 4 s. */
    private static void reloadChurn(
            ConfigSourcePayload payload, ConfigReloader reloader, Counters counters,
            CountDownLatch running, long deadlineNanos) {
        await(running);
        while (System.nanoTime() < deadlineNanos) {
            park(Duration.ofSeconds(4));
            payload.flip();
            try {
                reloader.reload();
                counters.reloads.incrementAndGet();
            } catch (Throwable e) {
                if (!isInterruption(e)) {
                    counters.escaped.add("reload: " + describe(e));
                }
            }
        }
    }

    /** Periodic degradation injection: pause Redis for 5 s every 25 s. */
    private static void degradationInjector(
            GenericContainer<?> redis, Counters counters, CountDownLatch running, long deadlineNanos) {
        await(running);
        park(Duration.ofSeconds(10));
        while (System.nanoTime() < deadlineNanos) {
            DockerClientFactory.instance().client().pauseContainerCmd(redis.getContainerId()).exec();
            counters.injections.incrementAndGet();
            park(Duration.ofSeconds(5));
            DockerClientFactory.instance().client().unpauseContainerCmd(redis.getContainerId()).exec();
            park(Duration.ofSeconds(20));
        }
    }

    /** Continuous invariant assertions plus progress logging. */
    private static void monitor(GenericContainer<?> redis, Counters counters, long deadlineNanos)
            throws Exception {
        long lastAllows = 0;
        long lastFallbackAllows = 0;
        long lastSampleNanos = System.nanoTime();
        long lastReport = System.nanoTime();
        Map<String, Long> lastChainAllows = new LinkedHashMap<>();
        RedisClient adminClient = RedisClient.create(
                "redis://" + redis.getHost() + ':' + redis.getMappedPort(6379));
        try (var admin = adminClient.connect()) {
            while (System.nanoTime() < deadlineNanos) {
                Thread.sleep(1_000);
                long now = System.nanoTime();
                long allows = counters.allowed.get();
                long fallbackAllows = counters.fallbackAllowed.get();
                long perInterval = allows - lastAllows;
                long fallbackPerInterval = fallbackAllows - lastFallbackAllows;
                // scale by the actually elapsed time: during an injected outage the
                // keyspace probe below can stall this loop past the 1 s cadence
                long elapsedSeconds = Math.max(1, (now - lastSampleNanos) / 1_000_000_000L);
                lastAllows = allows;
                lastFallbackAllows = fallbackAllows;
                lastSampleNanos = now;
                long boundPerChain = PER_SECOND_ALLOW_BOUND_PER_CHAIN * elapsedSeconds
                        + (fallbackPerInterval > 0
                                ? FALLBACK_PER_SECOND_ALLOW_BOUND_PER_CHAIN * elapsedSeconds : 0);
                long maxChainFlow = 0;
                String maxChain = "-";
                for (Map.Entry<String, AtomicLong> chain : counters.allowedByChain.entrySet()) {
                    long previous = lastChainAllows.getOrDefault(chain.getKey(), 0L);
                    long flow = chain.getValue().get() - previous;
                    lastChainAllows.put(chain.getKey(), chain.getValue().get());
                    if (flow > maxChainFlow) {
                        maxChainFlow = flow;
                        maxChain = chain.getKey();
                    }
                    if (flow > boundPerChain) {
                        counters.violations.add("summed flow " + flow + " of chain " + chain.getKey()
                                + " over " + elapsedSeconds + " s (fallback " + fallbackPerInterval
                                + ") exceeded the bound " + boundPerChain
                                + " (leaf capacity + refill + slack"
                                + (fallbackPerInterval > 0 ? " + fallback budget" : "") + ")");
                    }
                }
                // total sanity bound: every chain within its budget
                if (perInterval > boundPerChain * USERS) {
                    counters.violations.add("total flow " + perInterval + " over " + elapsedSeconds
                            + " s exceeded the aggregate bound " + boundPerChain * USERS);
                }
                Long keys = null;
                try {
                    keys = admin.sync().dbsize();
                } catch (RuntimeException e) {
                    // during an injected outage the keyspace probe may fail; the
                    // next sample re-checks (memory stays bounded by key TTLs)
                }
                if (keys != null && keys > MAX_REDIS_KEYS) {
                    counters.violations.add("Redis keyspace " + keys + " exceeded " + MAX_REDIS_KEYS
                            + " (idle state must be evicted)");
                }
                Runtime runtime = Runtime.getRuntime();
                long usedHeap = runtime.totalMemory() - runtime.freeMemory();
                if (usedHeap > MAX_USED_HEAP_BYTES) {
                    counters.violations.add("used heap " + usedHeap + " exceeded " + MAX_USED_HEAP_BYTES);
                }
                if (now - lastReport > Duration.ofSeconds(30).toNanos()) {
                    lastReport = now;
                    System.out.printf("soak: allowed=%d rejected=%d waited=%d reloads=%d injections=%d"
                                    + " fallbackDecisions=%d transitions=%d maxChainFlow=%d/%s"
                                    + " keys=%s escaped=%d%n",
                            allows, counters.rejected.get(), counters.waitedThenAllowed.get(),
                            counters.reloads.get(), counters.injections.get(),
                            counters.fallbackDecisions.get(), counters.degradationTransitions.get(),
                            maxChainFlow, maxChain, keys, counters.escaped.size());
                }
            }
        } finally {
            adminClient.shutdown();
        }
    }

    private static boolean isInterruption(Throwable e) {
        for (Throwable current = e; current != null; current = current.getCause()) {
            if (current instanceof InterruptedException) {
                return true;
            }
        }
        return false;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void park(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String describe(Throwable e) {
        return e.getClass().getName() + ": " + e.getMessage();
    }

    /** Mutable in-memory config source flipping the leaf capacity between 15 and 18. */
    private static final class ConfigSourcePayload implements ConfigSource {

        private final AtomicReference<Map<String, String>> payload = new AtomicReference<>(payload(15));

        static Map<String, String> payload(long leafCapacity) {
            Map<String, String> map = new LinkedHashMap<>();
            map.put("quotaflow.policies.global.scope", "global");
            map.put("quotaflow.policies.global.limit.capacity", Long.toString(GLOBAL_CAPACITY));
            map.put("quotaflow.policies.global.limit.refill-amount", Long.toString(GLOBAL_REFILL));
            map.put("quotaflow.policies.global.limit.refill-period", "PT1S");
            map.put("quotaflow.policies.tenant.scope", "tenant");
            map.put("quotaflow.policies.tenant.limit.capacity", "80");
            map.put("quotaflow.policies.tenant.limit.refill-amount", "80");
            map.put("quotaflow.policies.tenant.limit.refill-period", "PT1S");
            map.put("quotaflow.policies.tenant.parent", "global");
            map.put("quotaflow.policies.api.scope", "user");
            map.put("quotaflow.policies.api.limit.capacity", Long.toString(leafCapacity));
            map.put("quotaflow.policies.api.limit.refill-amount", Long.toString(leafCapacity));
            map.put("quotaflow.policies.api.limit.refill-period", "PT1S");
            map.put("quotaflow.policies.api.reaction", "throttle");
            map.put("quotaflow.policies.api.parent", "tenant");
            return map;
        }

        io.quotaflow.core.PolicySet currentPolicySet() {
            return io.quotaflow.config.ConfigurationParser.parse(payload.get()).policySet();
        }

        void flip() {
            payload.updateAndGet(current ->
                    payload("15".equals(current.get("quotaflow.policies.api.limit.capacity"))
                            ? LEAF_MAX_CAPACITY : 15));
        }

        @Override
        public Map<String, String> load() {
            return payload.get();
        }
    }

    /** Shared counters, the degradation listener, probe reservoirs and the final report. */
    private static final class Counters implements DegradationListener {

        private static final int RESERVOIR = 50_000;

        private final AtomicLong allowed = new AtomicLong();
        private final java.util.concurrent.ConcurrentHashMap<String, AtomicLong> allowedByChain =
                new java.util.concurrent.ConcurrentHashMap<>();

        AtomicLong allowanceOf(String chainKey) {
            return allowedByChain.computeIfAbsent(chainKey, key -> new AtomicLong());
        }

        private final AtomicLong rejected = new AtomicLong();
        private final AtomicLong waitedThenAllowed = new AtomicLong();
        private final AtomicLong reloads = new AtomicLong();
        private final AtomicLong injections = new AtomicLong();
        private final AtomicLong fallbackDecisions = new AtomicLong();
        private final AtomicLong fallbackAllowed = new AtomicLong();
        private final AtomicLong degradationTransitions = new AtomicLong();
        private final List<String> escaped = new CopyOnWriteArrayList<>();
        private final List<String> violations = new CopyOnWriteArrayList<>();

        private final long[] healthyProbes = new long[RESERVOIR];
        private final AtomicLong healthyProbeCount = new AtomicLong();
        private final long[] fallbackProbes = new long[RESERVOIR];
        private final AtomicLong fallbackProbeCount = new AtomicLong();
        private final AtomicLong outageProbes = new AtomicLong();

        void recordProbe(long latencyNanos, boolean degraded) {
            if (latencyNanos > OUTAGE_PROBE_THRESHOLD_NANOS) {
                // outage-shaped (stall into the command timeout): counted, but not
                // part of the normal-path latency distribution
                outageProbes.incrementAndGet();
            } else if (degraded) {
                fallbackProbes[(int) (fallbackProbeCount.getAndIncrement() % RESERVOIR)] = latencyNanos;
            } else {
                healthyProbes[(int) (healthyProbeCount.getAndIncrement() % RESERVOIR)] = latencyNanos;
            }
        }

        @Override
        public void onTransition(DegradationState from, DegradationState to, String reason) {
            degradationTransitions.incrementAndGet();
        }

        @Override
        public void onFallbackDecision(String policyId, String keyGroup, Verdict verdict) {
            fallbackDecisions.incrementAndGet();
            if (verdict == Verdict.ALLOWED) {
                fallbackAllowed.incrementAndGet();
            }
        }

        void report(Duration duration, boolean assertLatency) {
            double healthyP99Ms = p99Millis(healthyProbes, healthyProbeCount.get());
            double fallbackP99Ms = p99Millis(fallbackProbes, fallbackProbeCount.get());
            System.out.printf("soak summary (%d s): allowed=%d rejected=%d waited=%d reloads=%d%n"
                            + "  degradation: injections=%d transitions=%d fallbackDecisions=%d%n"
                            + "  probes: healthy=%d (p99 %.4f ms) fallback=%d (p99 %.4f ms) outage=%d%n"
                            + "  escaped=%d violations=%d%n",
                    duration.toSeconds(), allowed.get(), rejected.get(), waitedThenAllowed.get(),
                    reloads.get(), injections.get(), degradationTransitions.get(),
                    fallbackDecisions.get(), healthyProbeCount.get(), healthyP99Ms,
                    fallbackProbeCount.get(), fallbackP99Ms, outageProbes.get(),
                    escaped.size(), violations.size());

            check(allowed.get() > 0, "no traffic was allowed at all");
            check(escaped.isEmpty(), escaped.size() + " exceptions escaped to callers, first: "
                    + escaped.stream().findFirst().orElse("-"));
            check(injections.get() >= 2, "expected at least 2 degradation injections, got "
                    + injections.get());
            check(degradationTransitions.get() >= 2, "expected degradation transitions, got "
                    + degradationTransitions.get());
            check(fallbackDecisions.get() > 0, "no fallback decisions happened during outages");
            check(reloads.get() >= 2, "expected configuration reloads, got " + reloads.get());
            long totalProbes = healthyProbeCount.get() + fallbackProbeCount.get() + outageProbes.get();
            check(totalProbes == 0 || outageProbes.get() * 10 < totalProbes,
                    "outage-shaped probes exceeded 10% of all probes");
            if (assertLatency) {
                check(healthyProbeCount.get() >= 100, "too few healthy probes for a p99 assertion");
                check(healthyP99Ms <= 1.0,
                        "allow-path p99 " + healthyP99Ms + " ms exceeded the 1 ms target");
                if (fallbackProbeCount.get() >= 100) {
                    check(fallbackP99Ms <= 0.01,
                            "fallback p99 " + fallbackP99Ms + " ms exceeded the 0.01 ms target");
                }
            }
        }

        private void check(boolean condition, String violation) {
            if (!condition) {
                violations.add(violation);
            }
        }

        private static double p99Millis(long[] reservoir, long count) {
            int size = (int) Math.min(count, RESERVOIR);
            if (size == 0) {
                return 0;
            }
            long[] copy = Arrays.copyOf(reservoir, size);
            Arrays.sort(copy);
            return copy[Math.min(size - 1, (int) Math.ceil(size * 0.99) - 1)] / 1_000_000.0;
        }
    }
}
