package io.quotaflow.tck;

import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.fallback.*;
import io.quotaflow.store.redis.RedisStoreConfig;
import io.quotaflow.testing.RecoveryPrimaryFixture;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import jdk.jfr.*;
import static org.junit.jupiter.api.Assertions.*;

abstract class VirtualThreadScenario extends TckContainers {
    void verifyScenario(Algorithm algorithm, String mode) throws Exception {
        assertTrue(Runtime.version().feature() >= 21);
        assertTrue(FlightRecorder.getFlightRecorder().getEventTypes().stream()
                .anyMatch(type -> type.getName().equals("jdk.VirtualThreadPinned")), "pinning event must be available");
        var limit = mode.equals("throttle") ? new Limit(32, 10_000, Duration.ofSeconds(1))
                : new Limit(1_000_000_000, 1_000_000, Duration.ofSeconds(1));
        var policy = RateLimitPolicy.builder("virtual").scope(Scope.GLOBAL).algorithm(algorithm).limit(limit)
                .reaction(mode.equals("throttle") ? Reaction.THROTTLE : Reaction.REJECT).build();
        var policies = PolicySet.compile(List.of(policy));
        var resources = new ArrayList<AutoCloseable>();
        RateLimitStore store;
        RecoveryPrimaryFixture primary = null;
        FallbackRateLimitStore fallback = null;
        if (mode.equals("healthy")) {
            var connection = newClient(redisUri()).connect(); resources.add(connection);
            var redis = new io.quotaflow.testing.RecoveryStoreFixture(connection, new RedisStoreConfig(Duration.ofSeconds(5), Duration.ofSeconds(30)));
            resources.add(redis); store = redis;
        } else if (mode.equals("degraded")) {
            primary = new RecoveryPrimaryFixture(policies);
            fallback = new FallbackRateLimitStore(primary, new RecoverySettings("default", "virtual", "single",
                    RecoveryCohort.single(), 100, 100, Duration.ofMillis(10), Duration.ofSeconds(1)), List.of());
            resources.add(fallback); store = fallback;
        } else store = new LocalRateLimitStore();
        Path folder = Path.of("build/reports/virtual-threads", "jdk" + Runtime.version().feature());
        Files.createDirectories(folder);
        String prefix = algorithm + "-" + mode;
        var entries = new AtomicInteger();
        try (var cold = new Recording()) {
        cold.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO).withStackTrace();
        cold.start();
        try (var flow = DefaultQuotaFlow.builder(policies, store).maxWaitersPerPolicy(512)
                .operationTimeout(Duration.ofSeconds(5)).addWaitListener((p,g) -> entries.incrementAndGet()).build()) {
            if (fallback != null) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (fallback.state() != DegradationState.CLOSED && System.nanoTime() < deadline) Thread.sleep(5);
                assertEquals(DegradationState.CLOSED, fallback.state());
                primary.available = false;
                flow.tryAcquire("virtual", RateLimitContext.empty());
                assertEquals(DegradationState.OPEN, fallback.state());
                Thread.sleep(10); // The real conservative share starts cold and must earn credit.
            }
            if (mode.equals("throttle")) flow.tryAcquire("virtual", RateLimitContext.empty(), 32);
            Path coldFile = folder.resolve(prefix + "-cold.jfr");
            Phase coldPhase;
            try { coldPhase = exercise(flow, mode, 512); }
            finally { cold.stop(); cold.dump(coldFile); }
            flow.flushObservations().toCompletableFuture().get(10, TimeUnit.SECONDS);
            var coldPins = PinningEvidence.read(coldFile);
            writeReport(folder.resolve(prefix + "-cold.json"), "cold", coldPhase, entries.get(), coldPins, cold.getStartTime(), cold.getStopTime());
            assertEquals(0, coldPins.library(), "cold library blocking: " + coldPins.events());
            assertEquals(0, coldPins.unattributed(), "unattributed cold pinning: " + coldPins.events());
            assertEquals(0, flow.waitQueueDepth("virtual"));
            assertEquals(0, flow.observationFailures());
            entries.set(0);

            Path warmFile = folder.resolve(prefix + "-warm.jfr");
            Phase warmPhase;
            java.time.Instant warmStart, warmStop;
            try (var warm = new Recording()) {
                warm.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO).withStackTrace();
                warm.start();
                try { warmPhase = exercise(flow, mode, 100_000); }
                finally { warm.stop(); warm.dump(warmFile); }
                warmStart = warm.getStartTime(); warmStop = warm.getStopTime();
            }
            flow.flushObservations().toCompletableFuture().get(10, TimeUnit.SECONDS);
            var warmPins = PinningEvidence.read(warmFile);
            writeReport(folder.resolve(prefix + "-warm.json"), "warm", warmPhase, entries.get(), warmPins, warmStart, warmStop);
            assertTrue(warmPins.events().isEmpty(), "pinning after warm-up: " + warmPins.events());
            assertEquals(0, flow.waitQueueDepth("virtual"));
            assertEquals(0, flow.observationFailures());
            if (mode.equals("throttle")) assertTrue(entries.get() > 0, "throttle path must actually enqueue");
        }
        } finally {
            Collections.reverse(resources);
            for (var resource : resources) resource.close();
        }
    }

    record Phase(long started, long finished, int callers, int allowed, int rejected, int maximumQueue) { }

    private static Phase exercise(DefaultQuotaFlow flow, String mode, int callers) throws Exception {
        long started = System.nanoTime();
        var allowed = new AtomicInteger(); var rejected = new AtomicInteger(); var maximumQueue = new AtomicInteger();
        var window = new Semaphore(256);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var results = new ArrayList<Future<?>>(callers);
            for (int caller = 0; caller < callers; caller++) results.add(executor.submit(() -> {
                window.acquire();
                try {
                    assertTrue(Thread.currentThread().isVirtual());
                    var result = mode.equals("throttle")
                            ? flow.acquire("virtual", RateLimitContext.empty(), 1, Duration.ofSeconds(5))
                            : flow.tryAcquire("virtual", RateLimitContext.empty());
                    if (result.isAllowed()) allowed.incrementAndGet(); else rejected.incrementAndGet();
                    maximumQueue.accumulateAndGet(flow.waitQueueDepth("virtual"), Math::max);
                    return null;
                } finally { window.release(); }
            }));
            try {
                long deadline = started + TimeUnit.MINUTES.toNanos(2);
                for (var result : results) result.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } finally {
                results.forEach(result -> result.cancel(true));
                executor.shutdownNow();
            }
        }
        assertEquals(callers, allowed.get() + rejected.get());
        assertTrue(allowed.get() > 0); assertTrue(maximumQueue.get() <= 256);
        return new Phase(started, System.nanoTime(), callers, allowed.get(), rejected.get(), maximumQueue.get());
    }

    private static void writeReport(Path path, String phase, Phase outcome, int entries, PinningEvidence.Summary pins, java.time.Instant recordingStart, java.time.Instant recordingStop) throws Exception {
        Files.writeString(path.resolveSibling(path.getFileName().toString().replace(".json", "-events.txt")),
                String.join("\n", pins.events()));
        Files.writeString(path, "{\"phase\":\"" + phase + "\",\"pid\":" + ProcessHandle.current().pid()
                + ",\"recordingStarted\":\"" + recordingStart + "\",\"recordingStopped\":\"" + recordingStop + "\""
                + ",\"jdk\":" + Runtime.version().feature() + ",\"startedNanos\":" + outcome.started()
                + ",\"finishedNanos\":" + outcome.finished() + ",\"callers\":" + outcome.callers()
                + ",\"allowed\":" + outcome.allowed() + ",\"rejected\":" + outcome.rejected()
                + ",\"maximumQueue\":" + outcome.maximumQueue() + ",\"waitEntries\":" + entries
                + ",\"totalPins\":" + pins.total() + ",\"jvmInitializationPins\":" + pins.initialization()
                + ",\"libraryPins\":" + pins.library() + ",\"unattributedPins\":" + pins.unattributed() + "}\n");
    }
}
