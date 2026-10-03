package io.quotaflow.tck;

import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.fallback.*;
import io.quotaflow.store.redis.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** Full fleet soak; short diagnostic runs never count as one-hour certification. */
public final class SoakHarness {
    private static final int OWNERS = 4, WORKERS = 8, MAX_TRACKED = 1024;
    private static final long CAPACITY = 40, REFILL = 40;
    private static final Duration COMMAND_TIMEOUT = Duration.ofMillis(200);
    private static final RedisStoreConfig STORE_CONFIG = new RedisStoreConfig(COMMAND_TIMEOUT, Duration.ofSeconds(2));
    private static String root(Algorithm algorithm) { return algorithm.name().toLowerCase(Locale.ROOT); }
    private static PolicySet policies(int childCapacity) {
        var result = new ArrayList<RateLimitPolicy>();
        for (Algorithm algorithm : Algorithm.values()) {
            String root = root(algorithm);
            result.add(RateLimitPolicy.builder(root).scope(Scope.GLOBAL).algorithm(algorithm)
                    .limit(new Limit(CAPACITY, REFILL, Duration.ofSeconds(1))).build());
            for (String leaf : List.of("a", "b")) result.add(RateLimitPolicy.builder(root + "-" + leaf)
                    .scope(Scope.USER).parentId(root).algorithm(algorithm).reaction(Reaction.THROTTLE)
                    .limit(new Limit(childCapacity, childCapacity, Duration.ofSeconds(1))).build());
        }
        return PolicySet.compile(result);
    }
    public static void main(String[] args) throws Exception {
        long seconds = Long.getLong("quotaflow.soak.duration.seconds", 3600);
        if (seconds < 60) throw new IllegalArgumentException("Diagnostic soak must run for at least 60 seconds");
        var counters = new Counters();
        var stores = new ArrayList<FallbackRateLimitStore>();
        var primaries = new ArrayList<RedisRateLimitStore>();
        var flows = new ArrayList<DefaultQuotaFlow>();
        var workers = Executors.newFixedThreadPool(WORKERS + 2);
        var futures = new ArrayList<Future<?>>();
        var redis = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);
        redis.start();
        var client = RedisClientFactory.createClient("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379),
                Duration.ofSeconds(2), COMMAND_TIMEOUT);
        var cohort = new RecoveryCohort(List.of("0", "1", "2", "3"));
        PolicySet initial = policies(80);
        try {
            try (var connection = client.connect(); var store = new RedisRateLimitStore(connection, STORE_CONFIG)) {
                new RedisNamespaceAdmin(connection).provisionFresh("default", true);
                var bindings = initial.policies().stream().map(p -> new PolicyBinding(
                        new QuotaDomain("default", initial.rootPolicyId(p.id())), p.id(), p.scope(), p.algorithm())).toList();
                store.registerPolicies(bindings).toCompletableFuture().join();
                var admin = new RedisRecoveryController(connection, Duration.ofSeconds(2));
                admin.provisionCohort("default", cohort, "soak", true, true).toCompletableFuture().join();
                for (Algorithm algorithm : Algorithm.values()) admin.provisionDomain(new QuotaDomain("default", root(algorithm)),
                        cohort, "soak", initial.recoveryFingerprint(root(algorithm)), true, true).toCompletableFuture().join();
            }
            for (String member : cohort.members()) {
                var primary = RedisRateLimitStore.create(client, STORE_CONFIG); primaries.add(primary);
                var owner = new FallbackRateLimitStore(primary.recoveryPrimary("default", COMMAND_TIMEOUT, true),
                        new RecoverySettings("default", "soak", member, cohort, MAX_TRACKED, 256,
                                Duration.ofMillis(50), Duration.ofSeconds(2)), List.of(counters));
                stores.add(owner);
                flows.add(DefaultQuotaFlow.builder(initial, owner).maxWaitersPerPolicy(64).build());
            }
            await(() -> stores.stream().allMatch(s -> s.state() == DegradationState.CLOSED), Duration.ofSeconds(15));
            long started = System.nanoTime(), deadline = started + TimeUnit.SECONDS.toNanos(seconds);
            for (int worker = 0; worker < WORKERS; worker++) {
                final int owner = worker % OWNERS;
                futures.add(workers.submit(() -> traffic(flows.get(owner), counters, deadline)));
            }
            futures.add(workers.submit(() -> {
                int revision = 0;
                while (pause(4000, deadline)) {
                    PolicySet next = policies(++revision % 2 == 0 ? 80 : 100);
                    flows.forEach(flow -> flow.replacePolicySet(next));
                    counters.reloads.incrementAndGet();
                }
            }));
            futures.add(workers.submit(() -> {
                while (pause(10_000, deadline)) {
                    var docker = DockerClientFactory.instance().client();
                    docker.pauseContainerCmd(redis.getContainerId()).exec();
                    try {
                        counters.outages.incrementAndGet();
                        await(() -> stores.stream().allMatch(s -> s.state() == DegradationState.OPEN), Duration.ofSeconds(5));
                        pause(3000, deadline);
                    } finally { docker.unpauseContainerCmd(redis.getContainerId()).exec(); }
                    long recoveryStart = System.nanoTime();
                    await(() -> stores.stream().allMatch(s -> s.state() == DegradationState.CLOSED), Duration.ofSeconds(15));
                    counters.recoveryNanos.accumulateAndGet(System.nanoTime() - recoveryStart, Math::max);
                    counters.recoveries.incrementAndGet();
                }
            }));
            long[] previous = new long[Algorithm.values().length];
            long sampleAt = started, lastReport = started, lastKeyspaceSuccess = started;
            try (var monitor = client.connect()) {
                monitor.setTimeout(Duration.ofSeconds(2));
                while (System.nanoTime() < deadline) {
                    Thread.sleep(1000);
                    for (Future<?> future : futures) if (future.isDone()) future.get();
                    long now = System.nanoTime();
                    double elapsed = (now - sampleAt) / 1_000_000_000d;
                    // All children and every owner share this one root allowance. No extra
                    // fallback capacity is added during outage or handoff windows.
                    long bound = (long) Math.ceil(1.2 * (CAPACITY + REFILL * elapsed));
                    for (Algorithm algorithm : Algorithm.values()) {
                        long count = counters.allowed.get(algorithm.ordinal());
                        check(count - previous[algorithm.ordinal()] <= bound, "shared parent recovery envelope exceeded: " + algorithm);
                        previous[algorithm.ordinal()] = count;
                    }
                    sampleAt = now;
                    for (var store : stores) check(store.trackedBuckets() <= MAX_TRACKED, "tracking bound exceeded");
                    for (var flow : flows) for (var policy : flow.policySet().policies())
                        check(flow.waitQueueDepth(policy.id()) <= 64, "wait queue bound exceeded");
                    try {
                        check(monitor.sync().dbsize() < 10_000, "Redis state bound exceeded");
                        lastKeyspaceSuccess = System.nanoTime();
                    }
                    catch (io.lettuce.core.RedisException unavailable) {
                        counters.monitorFailures.incrementAndGet();
                        // This is an independent administrative connection, not a failed
                        // limiter command. Each injected outage separately requires all
                        // owners to report OPEN; administrative sampling must recover too.
                        check(System.nanoTime() - lastKeyspaceSuccess < TimeUnit.SECONDS.toNanos(30), "Redis state sampling remained unavailable");
                    }
                    if (now - lastReport >= TimeUnit.SECONDS.toNanos(30)) {
                        lastReport = now;
                        System.gc();
                        check(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() < 400L * 1024 * 1024,
                                "retained heap bound exceeded");
                        System.out.println("soak progress " + counters.summary(seconds));
                    }
                }
            }
            workers.shutdown();
            check(workers.awaitTermination(25, TimeUnit.SECONDS), "soak workers did not terminate");
            for (Future<?> future : futures) future.get();
            for (var flow : flows) {
                flow.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
                check(flow.observationFailures() == 0, "incomplete observations");
                for (var policy : flow.policySet().policies()) check(flow.waitQueueDepth(policy.id()) == 0, "leaked waiter");
            }
            for (var store : stores) store.flushObservations().toCompletableFuture().get(2, TimeUnit.SECONDS);
            check(counters.outages.get() >= 2 && counters.recoveries.get() == counters.outages.get(), "missing outage/recovery cycles");
            check(counters.reloads.get() >= 2 && counters.cancelled.get() > 0 && counters.rejected.get() > 0, "missing mixed traffic");
            check(counters.fallback.get() > 0 && counters.transitions.get() >= 2 * OWNERS, "missing degradation observations");
            for (Algorithm algorithm : Algorithm.values()) check(counters.allowed.get(algorithm.ordinal()) > 0, "algorithm admitted no traffic");
            var output = Path.of("build/reports/soak/summary.json"); Files.createDirectories(output.getParent());
            var report = counters.summary(seconds);
            report.put("elapsedNanos", System.nanoTime() - started);
            report.put("jdk", Runtime.version().toString()); report.put("pid", ProcessHandle.current().pid());
            report.put("serverImage", redis.getDockerImageName());
            report.put("status", seconds >= 3600 ? "PASS" : "DIAGNOSTIC");
            Files.writeString(output, new com.fasterxml.jackson.databind.ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(report));
            System.out.println("SOAK VERIFIED " + report);
        } finally {
            workers.shutdownNow(); workers.awaitTermination(5, TimeUnit.SECONDS);
            flows.forEach(DefaultQuotaFlow::close); stores.forEach(FallbackRateLimitStore::close);
            primaries.forEach(RedisRateLimitStore::close); client.shutdown(); redis.stop();
        }
    }
    private static void traffic(DefaultQuotaFlow flow, Counters counters, long deadline) {
        var random = new Random(7261 + Thread.currentThread().getId());
        while (System.nanoTime() < deadline && !Thread.currentThread().isInterrupted()) {
            Algorithm algorithm = Algorithm.values()[random.nextInt(Algorithm.values().length)];
            String policy = root(algorithm) + (random.nextBoolean() ? "-a" : "-b");
            var context = RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "user-" + random.nextInt(50)).build();
            long started = System.nanoTime();
            if (random.nextInt(10) == 0) {
                var future = flow.acquireAsync(policy, context, 1, Duration.ofMillis(100)).toCompletableFuture();
                if (future.cancel(true)) counters.cancelled.incrementAndGet();
                else record(future.join(), algorithm, counters);
            } else {
                Decision result = random.nextBoolean() ? flow.tryAcquire(policy, context)
                        : flow.acquire(policy, context, 1, Duration.ofMillis(50));
                record(result, algorithm, counters);
            }
            counters.latency(System.nanoTime() - started);
        }
    }
    private static void record(Decision result, Algorithm algorithm, Counters counters) {
        if (result.isAllowed()) counters.allowed.incrementAndGet(algorithm.ordinal());
        else counters.rejected.incrementAndGet();
    }
    private static boolean pause(long millis, long deadline) {
        long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (remaining <= 0) return false;
        try { Thread.sleep(Math.min(millis, remaining)); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return false; }
        return System.nanoTime() < deadline;
    }
    private static void await(BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        do {
            if (condition.getAsBoolean()) return;
        } while (pause(10, deadline));
        if (condition.getAsBoolean()) return;
        throw new IllegalStateException("recovery checkpoint timed out");
    }
    private static void check(boolean condition, String message) { if (!condition) throw new IllegalStateException(message); }
    private static final class Counters implements DegradationListener {
        final AtomicLongArray allowed = new AtomicLongArray(Algorithm.values().length);
        final AtomicLong rejected = new AtomicLong(), cancelled = new AtomicLong(), reloads = new AtomicLong();
        final AtomicLong outages = new AtomicLong(), recoveries = new AtomicLong(), recoveryNanos = new AtomicLong();
        final AtomicLong fallback = new AtomicLong(), transitions = new AtomicLong(), monitorFailures = new AtomicLong();
        final long[] samples = new long[100_000]; long latencyCount;
        synchronized void latency(long nanos) { samples[(int) (latencyCount++ % samples.length)] = nanos; }
        public void onTransition(DegradationState from, DegradationState to, String reason) { transitions.incrementAndGet(); }
        public void onFallbackDecision(String policy, String group, Verdict verdict) { fallback.incrementAndGet(); }
        synchronized Map<String, Object> summary(long seconds) {
            var result = new LinkedHashMap<String, Object>();
            result.put("durationSeconds", seconds); result.put("owners", OWNERS);
            for (Algorithm algorithm : Algorithm.values()) result.put(algorithm.name() + "Allowed", allowed.get(algorithm.ordinal()));
            result.put("rejected", rejected.get()); result.put("cancelled", cancelled.get()); result.put("reloads", reloads.get());
            result.put("outages", outages.get()); result.put("recoveries", recoveries.get());
            result.put("maximumRecoveryMillis", recoveryNanos.get() / 1_000_000d);
            result.put("monitorFailures", monitorFailures.get()); result.put("fallbackDecisions", fallback.get()); result.put("transitions", transitions.get());
            long[] sorted = Arrays.copyOf(samples, (int) Math.min(latencyCount, samples.length)); Arrays.sort(sorted);
            result.put("latencySamples", latencyCount);
            result.put("retainedLatencySamples", sorted.length);
            result.put("latencyWindow", "last 100000 completions; bounded rolling reservoir");
            for (int percentile : List.of(50, 95, 99)) result.put("p" + percentile + "Millis", sorted.length == 0 ? 0
                    : sorted[Math.min(sorted.length - 1, (int) Math.ceil(sorted.length * percentile / 100d) - 1)] / 1_000_000d);
            result.put("unexpectedExceptions", 0);
            return result;
        }
    }
}
