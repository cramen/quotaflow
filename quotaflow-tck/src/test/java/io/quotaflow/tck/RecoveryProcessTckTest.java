package io.quotaflow.tck;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.store.redis.*;
import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class RecoveryProcessTckTest extends TckContainers {
    @Test void independentOwnersRejectDuplicatesAndRequireExplicitMaintenanceAfterCrash() throws Exception {
        var processes = new ArrayList<Owner>();
        var client = RedisClientFactory.createClient(redisUri(), Duration.ofSeconds(2));
        var domain = new QuotaDomain("default", "quota");
        var cohort = new RecoveryCohort(List.of("a", "b"));
        try (var connection = client.connect(); var primary = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            primary.registerPolicies(List.of(new PolicyBinding(domain, "quota", Scope.GLOBAL, Algorithm.TOKEN_BUCKET))).toCompletableFuture().join();
            var admin = new RedisRecoveryController(connection, Duration.ofSeconds(5));
            admin.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            admin.provisionDomain(domain, cohort, "initial", RecoveryProcessOwner.policies().recoveryFingerprint("quota"), true, true).toCompletableFuture().join();
            Owner a = start(processes, "a", "a,b"), b = start(processes, "b", "a,b");
            a.await("STATE", "CLOSED"); b.await("STATE", "CLOSED");
            assertEquals("ALLOW", a.command("ACQUIRE"));
            String manifest = RedisKeyScheme.defaults().manifestKey("default");
            var oldSession = new RecoverySession("initial", cohort.digest(), "a", 0, 1, connection.sync().hget(manifest, "rc:owner:1"));
            var oldContext = admin.read(domain, oldSession).toCompletableFuture().join().context();
            Owner duplicate = start(processes, "a", "a,b"); duplicate.await("ACQUIRE", "INCOMPATIBLE");
            duplicate.crash(); b.crash();
            assertEquals("OK", a.command("DISCONNECT")); a.await("STATE", "OPEN");
            assertEquals("REJECT", a.command("ACQUIRE"), "a new local observation starts empty");
            assertEquals("OK", a.command("RECONNECT"));
            Thread.sleep(300);
            assertEquals("OPEN", a.command("STATE"), "a crashed peer cannot be timed out as ready");
            Owner restarted = start(processes, "b", "a,b"); restarted.await("ACQUIRE", "INCOMPATIBLE");
            restarted.crash(); a.crash();
            // All old local processes are terminated; one full horizon drains the old distributed debt.
            Thread.sleep(1100);
            String incarnation = admin.replaceCohort("default", "initial", RecoveryCohort.single(), List.of(domain),
                    "planned-replacement", true, true).toCompletableFuture().join();
            assertNotEquals("initial", incarnation);
            assertThrows(CompletionException.class, () -> primary.seed(oldContext, List.of()).toCompletableFuture().join());
            Owner replacement = start(processes, "single", "single"); replacement.await("STATE", "CLOSED");
            assertEquals("ALLOW", replacement.command("ACQUIRE"));
            connection.sync().del(RedisKeyScheme.defaults().controlKey(domain));
            replacement.await("ACQUIRE", "INCOMPATIBLE");
            assertFalse(connection.sync().exists(RedisKeyScheme.defaults().controlKey(domain)) > 0, "runtime cannot recreate lost fencing metadata");
        } finally { processes.forEach(Owner::crash); client.shutdown(); }
    }
    private Owner start(List<Owner> processes, String id, String members) throws Exception {
        var owner = new Owner(redisUri(), id, members); processes.add(owner);
        assertEquals("STARTED", owner.response()); return owner;
    }
    private static final class Owner {
        private final Process process;
        private final BufferedWriter input;
        private final BlockingQueue<String> output = new LinkedBlockingQueue<>();
        Owner(String uri, String id, String members) throws IOException {
            process = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xmx128m", "-cp",
                    System.getProperty("quotaflow.tck.classpath"), RecoveryProcessOwner.class.getName(), uri, id, members)
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
        void crash() {
            process.destroyForcibly();
            try { assertTrue(process.waitFor(5, TimeUnit.SECONDS), "old owner must be terminated before maintenance"); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
        }
    }
}
