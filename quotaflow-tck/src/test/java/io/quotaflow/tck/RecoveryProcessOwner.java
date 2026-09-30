package io.quotaflow.tck;

import io.quotaflow.core.*;
import io.quotaflow.core.store.*;
import io.quotaflow.fallback.*;
import io.quotaflow.store.redis.*;
import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Separate JVM owner used by crash/restart conformance; commands never alter ownership metadata. */
public final class RecoveryProcessOwner {
    static PolicySet policies() {
        return PolicySet.compile(List.of(RateLimitPolicy.builder("quota").scope(Scope.GLOBAL)
                .limit(new Limit(100, 100, Duration.ofSeconds(1))).build()));
    }
    public static void main(String[] args) throws Exception {
        var cohort = new RecoveryCohort(Arrays.asList(args[2].split(",")));
        var connected = new AtomicBoolean(true);
        var client = RedisClientFactory.createClient(args[0], Duration.ofSeconds(2), Duration.ofMillis(200));
        try (var connection = client.connect(); var primary = new RedisRateLimitStore(connection, RedisStoreConfig.defaults())) {
            var delegate = new RedisRecoveryPrimary("default", connection, primary, Duration.ofMillis(200), true);
            var gated = (RecoveryPrimary) java.lang.reflect.Proxy.newProxyInstance(RecoveryPrimary.class.getClassLoader(),
                    new Class<?>[]{RecoveryPrimary.class}, (proxy, method, values) -> {
                        if (!connected.get()) return CompletableFuture.failedFuture(new PrimaryDispatchException(PrimaryDispatchException.Outcome.NOT_DISPATCHED));
                        try { return method.invoke(delegate, values); }
                        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var settings = new RecoverySettings("default", "process-test", args[1], cohort, 100, 100,
                    Duration.ofMillis(20), Duration.ofSeconds(1));
            try (var store = new FallbackRateLimitStore(gated, settings, List.of())) {
                var flow = DefaultQuotaFlow.builder(policies(), store).build();
                answer("STARTED");
                var commands = new BufferedReader(new InputStreamReader(System.in));
                String command;
                while ((command = commands.readLine()) != null) {
                    switch (command) {
                        case "STATE" -> answer(store.state().name());
                        case "DISCONNECT" -> { connected.set(false); answer("OK"); }
                        case "RECONNECT" -> { connected.set(true); answer("OK"); }
                        case "ACQUIRE" -> {
                            try { answer(flow.tryAcquire("quota", RateLimitContext.empty()).isAllowed() ? "ALLOW" : "REJECT"); }
                            catch (RuntimeException failure) {
                                Throwable cause = failure;
                                while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
                                if (cause instanceof PolicyConfigurationException) answer("INCOMPATIBLE"); else throw failure;
                            }
                        }
                        case "EXIT" -> { answer("STOPPED"); return; }
                        default -> throw new IllegalArgumentException("unknown test command");
                    }
                }
            }
        } finally { client.shutdown(); }
    }
    private static void answer(String value) { System.out.println("QF_RESULT " + value); System.out.flush(); }
}
