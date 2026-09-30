package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.*;
import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import java.io.IOException;
import java.net.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class RedisNoReplayTest extends RedisContainerSupport {
    @Test void executedCommandWithLostReplyIsNotReplayedAfterSocketReconnect() throws Exception {
        var domain = new QuotaDomain("default", "root");
        var limit = new Limit(10, 1, Duration.ofHours(1));
        var cohort = RecoveryCohort.single();
        var binding = new PolicyBinding(domain, "root", Scope.GLOBAL, Algorithm.TOKEN_BUCKET);
        try (var direct = client().connect(); var directStore = new RedisRateLimitStore(direct, RedisStoreConfig.defaults());
             var proxy = new ReplyDroppingProxy(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            directStore.registerPolicies(List.of(binding)).toCompletableFuture().join();
            var controller = new RedisRecoveryController(direct, Duration.ofSeconds(2));
            controller.provisionCohort("default", cohort, "initial", true, true).toCompletableFuture().join();
            controller.provisionDomain(domain, cohort, "initial", "a".repeat(64), true, true).toCompletableFuture().join();
            var owner = controller.enroll("default", cohort, "single", "known-owner").toCompletableFuture().join();
            var gather = controller.attach(domain, owner).toCompletableFuture().join().context();
            var drain = controller.join(gather).toCompletableFuture().join().context();
            var normal = controller.ready(drain).toCompletableFuture().join().context();
            var pending = new RecoveryPending(domain, normal.dispatchGeneration(), new CompletableFuture<>());
            // Warm the exact script before dropping a response, so the lost response is not NOSCRIPT.
            directStore.tryAcquireAll(normal, List.of(new LevelRequest(new BucketIdentity(domain, "root", Scope.GLOBAL, "warm"),
                    limit, Algorithm.TOKEN_BUCKET, 1)), false, pending).toCompletableFuture().join();
            var client = RedisClientFactory.createClient("redis://127.0.0.1:" + proxy.port(), Duration.ofSeconds(1), Duration.ofMillis(200));
            try (var connection = client.connect(); var store = new RedisRateLimitStore(connection,
                    new RedisStoreConfig(Duration.ofMillis(200), Duration.ofSeconds(2)))) {
                store.registerPolicies(List.of(binding)).toCompletableFuture().join(); store.bindRecoveryContext(normal);
                var key = new BucketIdentity(domain, "root", Scope.GLOBAL, "lost-reply");
                connection.sync().ping(); proxy.arm();
                var result = store.tryAcquireAsync(key, limit, Algorithm.TOKEN_BUCKET, 1).toCompletableFuture();
                assertTrue(proxy.dropped.await(2, TimeUnit.SECONDS), "the server executed the command before the connection was cut");
                assertThrows(ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                boolean connected = false;
                while (!connected && System.nanoTime() < deadline) {
                    try { connected = "PONG".equals(connection.async().ping().toCompletableFuture().get(300, TimeUnit.MILLISECONDS)); }
                    catch (Exception unavailable) { Thread.sleep(20); }
                }
                assertTrue(connected, "transport should reconnect");
                String encoded = direct.sync().get(RedisKeyScheme.defaults().singleKey(key));
                assertEquals(9, Long.parseLong(encoded.split(":")[3]), "one server debit despite the lost business outcome");
                var adapter = new RedisRecoveryPrimary("default", connection, store, Duration.ofSeconds(1), true);
                for (int i = 0; i < 5; i++) adapter.probe().toCompletableFuture().join();
                assertEquals(encoded, direct.sync().get(RedisKeyScheme.defaults().singleKey(key)), "probes consume no quota");
                assertTrue(store.tryAcquire(key, limit, Algorithm.TOKEN_BUCKET, 1).acquired());
                assertEquals(8, Long.parseLong(direct.sync().get(RedisKeyScheme.defaults().singleKey(key)).split(":")[3]));
            } finally { client.shutdown(); }
        }
    }

    /** Drops one complete execution outcome by cutting the socket on its first response byte. */
    private static final class ReplyDroppingProxy implements AutoCloseable {
        private final ServerSocket listener;
        private final String host; private final int target;
        private final AtomicBoolean armed = new AtomicBoolean(), closed = new AtomicBoolean();
        private final CountDownLatch dropped = new CountDownLatch(1);
        private final java.util.Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        ReplyDroppingProxy(String host, int target) throws IOException {
            this.host = host; this.target = target;
            listener = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
            daemon(() -> {
                while (!closed.get()) try {
                    Socket front = listener.accept(), back = new Socket(host, target);
                    sockets.add(front); sockets.add(back);
                    daemon(() -> pump(front, back, false)); daemon(() -> pump(back, front, true));
                } catch (IOException stopped) { if (!closed.get()) throw new RuntimeException(stopped); }
            });
        }
        int port() { return listener.getLocalPort(); }
        void arm() { armed.set(true); }
        private void pump(Socket from, Socket to, boolean reply) {
            byte[] bytes = new byte[8192];
            try {
                int read;
                while ((read = from.getInputStream().read(bytes)) >= 0) {
                    if (reply && armed.compareAndSet(true, false)) { dropped.countDown(); return; }
                    to.getOutputStream().write(bytes, 0, read); to.getOutputStream().flush();
                }
            } catch (IOException closedSocket) { /* The deliberate cut also terminates the reverse pump. */ }
            finally { dispose(from); dispose(to); }
        }
        private void dispose(Socket socket) {
            sockets.remove(socket); try { socket.close(); } catch (IOException ignored) { }
        }
        private static void daemon(Runnable action) {
            Thread thread = new Thread(action, "quotaflow-test-proxy"); thread.setDaemon(true); thread.start();
        }
        @Override public void close() throws IOException {
            closed.set(true); listener.close(); sockets.forEach(this::dispose);
        }
    }
}
