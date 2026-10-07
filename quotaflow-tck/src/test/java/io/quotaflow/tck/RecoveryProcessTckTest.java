package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.store.redis.*;
import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class RecoveryProcessTckTest extends TckContainers {
    @ParameterizedTest @EnumSource(Algorithm.class)
    void independentOwnersRejectDuplicatesAndRequireExplicitMaintenanceAfterCrash(Algorithm algorithm) throws Exception {
        var processes = new ArrayList<Owner>();
        var client = RedisClientFactory.createClient(redisUri(), Duration.ofSeconds(2));
        var domain = new QuotaDomain("default", "quota");
        var cohort = new RecoveryCohort(List.of("a", "b"));
        try (var connection = client.connect(); var primary = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            primary.registerPolicies(List.of(new PolicyBinding(domain, "quota", Scope.GLOBAL, algorithm))).toCompletableFuture().join();
            var admin = new RedisRecoveryController(connection, Duration.ofSeconds(5));
            admin.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            admin.provisionDomain(domain, cohort, "initial", RecoveryProcessOwner.policies(algorithm).recoveryFingerprint("quota"), true, true).toCompletableFuture().join();
            Owner a = start(processes, algorithm, "a", "a,b"), b = start(processes, algorithm, "b", "a,b");
            a.await("STATE", "CLOSED"); b.await("STATE", "CLOSED");
            assertEquals("ALLOW", a.command("ACQUIRE"));
            String manifest = RedisKeyScheme.defaults().manifestKey("default");
            var oldSession = new RecoverySession("initial", cohort.digest(), "a", 0, 1, connection.sync().hget(manifest, "rc:owner:1"));
            var oldContext = admin.read(domain, oldSession).toCompletableFuture().join().context();
            Owner duplicate = start(processes, algorithm, "a", "a,b"); duplicate.await("ACQUIRE", "INCOMPATIBLE");
            duplicate.crash(); b.crash();
            assertEquals("OK", a.command("DISCONNECT")); a.await("STATE", "OPEN");
            assertEquals("REJECT", a.command("ACQUIRE"), "a new local observation starts empty");
            assertEquals("OK", a.command("RECONNECT"));
            Thread.sleep(300);
            assertEquals("OPEN", a.command("STATE"), "a crashed peer cannot be timed out as ready");
            Owner restarted = start(processes, algorithm, "b", "a,b"); restarted.await("ACQUIRE", "INCOMPATIBLE");
            restarted.crash(); a.crash();
            // All old local processes are terminated; one full horizon drains the old distributed debt.
            Thread.sleep(1100);
            String incarnation = admin.replaceCohort("default", "initial", RecoveryCohort.single(), List.of(domain),
                    "planned-replacement", true, true).toCompletableFuture().join();
            assertNotEquals("initial", incarnation);
            assertThrows(CompletionException.class, () -> primary.seed(oldContext, List.of()).toCompletableFuture().join());
            Owner replacement = start(processes, algorithm, "single", "single"); replacement.await("STATE", "CLOSED");
            assertEquals("ALLOW", replacement.command("ACQUIRE"));
            connection.sync().del(RedisKeyScheme.defaults().controlKey(domain));
            replacement.await("ACQUIRE", "INCOMPATIBLE");
            assertFalse(connection.sync().exists(RedisKeyScheme.defaults().controlKey(domain)) > 0, "runtime cannot recreate lost fencing metadata");
        } finally { processes.forEach(Owner::crash); client.shutdown(); }
    }

    @ParameterizedTest @EnumSource(Algorithm.class)
    void independentProcessesPreserveFiveVersusZeroThroughTheWholeBarrier(Algorithm algorithm) throws Exception {
        var processes = new ArrayList<Owner>();
        var client = RedisClientFactory.createClient(redisUri(), Duration.ofSeconds(2));
        var domain = new QuotaDomain("default", "quota"); var cohort = new RecoveryCohort(List.of("a", "b"));
        try (var connection = client.connect(); var primary = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            String clock = ControlledRedisClock.install(primary, domain); connection.sync().set(clock, "0");
            primary.registerPolicies(List.of(new PolicyBinding(domain, "quota", Scope.GLOBAL, algorithm))).toCompletableFuture().join();
            var admin = new RedisRecoveryController(connection, Duration.ofSeconds(2));
            admin.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            admin.provisionDomain(domain, cohort, "initial", RecoveryProcessOwner.policies(algorithm, true).recoveryFingerprint("quota"), true, true)
                    .toCompletableFuture().join();
            Owner a = new Owner(redisUri(), "a", "a,b", algorithm, true), b = new Owner(redisUri(), "b", "a,b", algorithm, true);
            processes.add(a); processes.add(b);
            assertEquals("STARTED", a.response()); assertEquals("STARTED", b.response());
            a.await("STATE", "CLOSED"); b.await("STATE", "CLOSED");
            assertEquals("ALLOW", a.command("ACQUIRE 10"));
            assertEquals("OK", a.command("DISCONNECT")); assertEquals("OK", b.command("DISCONNECT"));
            for (Owner owner : List.of(a, b)) {
                assertEquals("REJECT", owner.command("ACQUIRE"));
                owner.awaitColdGuard();
            }
            // Keep both observations live before the handoff. Otherwise A's
            // untouched entry legitimately expires at its 1000 s horizon plus
            // the 1 s attempt grace period, destroying the intended A=5 setup.
            for (Owner owner : List.of(a, b)) assertEquals("OK", owner.command("TIME 500"));
            for (Owner owner : List.of(a, b)) assertEquals("REJECT", owner.command("ACQUIRE 5"));
            for (Owner owner : List.of(a, b)) assertEquals("OK", owner.command("TIME 1000"));
            assertEquals("ALLOW", b.command("ACQUIRE 5"));
            var recoveryMillis = new java.util.concurrent.atomic.AtomicLong(1_000_000);
            assertEquals("OK", a.command("RECONNECT"));
            String control = RedisKeyScheme.defaults().controlKey(domain);
            awaitRecovery(() -> "GATHER".equals(connection.sync().hget(control, "phase"))
                    && "1".equals(connection.sync().hget(control, "joined")), recoveryMillis, a, b);
            var bucket = new BucketIdentity(domain, "quota", Scope.GLOBAL, "global");
            assertEquals(5, Long.parseLong(connection.sync().get(RedisKeyScheme.defaults().singleKey(bucket)).split(":")[3]),
                    "the joined owner seeds its own five credits, never the fleet multiple");
            for (int token = 0; token < 5; token++) a.await("ACQUIRE", "ALLOW");
            assertEquals("REJECT", a.command("ACQUIRE"));
            assertEquals("OPEN", a.command("STATE"), "one process cannot release the cohort guard");
            assertEquals("OK", b.command("RECONNECT"));
            awaitRecovery(() -> "CLOSED".equals(a.command("STATE")) && "CLOSED".equals(b.command("STATE")), recoveryMillis, a, b);
            assertEquals("REJECT", a.command("ACQUIRE 10")); assertEquals("REJECT", b.command("ACQUIRE 10"));
            long refilledAt = recoveryMillis.get() + 100_000;
            assertEquals("OK", a.command("TIME_MILLIS " + refilledAt));
            assertEquals("OK", b.command("TIME_MILLIS " + refilledAt));
            assertEquals("ALLOW", a.command("ACQUIRE")); assertEquals("REJECT", b.command("ACQUIRE"));
        } finally { processes.forEach(Owner::crash); client.shutdown(); }
    }
    private static void awaitRecovery(Callable<Boolean> condition,
                                      java.util.concurrent.atomic.AtomicLong millis, Owner... owners) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.call()) {
            assertTrue(System.nanoTime() < deadline, "recovery barrier did not complete");
            // Retry backoff uses the controlled clock too. Advancing by less than
            // one global emission interval cannot conceal an extra admitted token.
            long next = millis.addAndGet(50);
            assertTrue(next < 1_090_000, "recovery exceeded the sub-token clock budget");
            for (Owner owner : owners) assertEquals("OK", owner.command("TIME_MILLIS " + next));
            Thread.sleep(20);
        }
    }

    private Owner start(List<Owner> processes, Algorithm algorithm, String id, String members) throws Exception {
        var owner = new Owner(redisUri(), id, members, algorithm); processes.add(owner);
        assertEquals("STARTED", owner.response()); return owner;
    }
    private static final class Owner {
        private final Process process;
        private final BufferedWriter input;
        private final BlockingQueue<String> output = new LinkedBlockingQueue<>();
        Owner(String uri, String id, String members, Algorithm algorithm) throws IOException { this(uri, id, members, algorithm, false); }
        Owner(String uri, String id, String members, Algorithm algorithm, boolean controlled) throws IOException {
            process = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xmx128m", "-cp",
                    System.getProperty("quotaflow.tck.classpath"), RecoveryProcessOwner.class.getName(), uri, id, members, algorithm.name(), Boolean.toString(controlled))
                    .redirectErrorStream(true).start();
            input = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
            Thread reader = new Thread(() -> {
                try (var lines = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = lines.readLine()) != null) if (line.startsWith("QF_RESULT ")) output.add(line.substring(10));
                } catch (IOException stopped) { /* Process shutdown closes its output pipe. */ }
            }, "quotaflow-process-test-output"); reader.setDaemon(true); reader.start();
        }
        String response() throws InterruptedException {
            String result = output.poll(15, TimeUnit.SECONDS);
            assertNotNull(result, "owner did not respond; alive=" + process.isAlive()); return result;
        }
        String command(String command) throws Exception { input.write(command); input.newLine(); input.flush(); return response(); }
        void await(String command, String expected) throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos(); String actual;
            do { actual = command(command); if (actual.equals(expected)) return; Thread.sleep(20); } while (System.nanoTime() < deadline);
            assertEquals(expected, actual);
        }
        void awaitColdGuard() throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (true) {
                String result = command("ADMISSION");
                assertNotEquals("ALLOW", result, "a new local observation must remain empty");
                if (result.equals("REJECT")) return;
                assertEquals("PENDING", result);
                assertTrue(System.nanoTime() < deadline, "local guard did not become ready");
                Thread.sleep(20);
            }
        }
        void crash() {
            process.destroyForcibly();
            try { assertTrue(process.waitFor(5, TimeUnit.SECONDS), "old owner must be terminated before maintenance"); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
        }
    }
}
