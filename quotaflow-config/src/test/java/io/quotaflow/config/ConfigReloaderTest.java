package io.quotaflow.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.DefaultQuotaFlow;
import io.quotaflow.core.Decision;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.RateLimitContext;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConfigReloaderTest {

    private static Map<String, String> payload(long capacity) {
        // slow refill: once drained, the bucket stays drained under this limit
        return payload(capacity, 1, "PT1H");
    }

    private static Map<String, String> payload(long capacity, long refillAmount, String refillPeriod) {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("quotaflow.policies.u.scope", "user");
        map.put("quotaflow.policies.u.limit.capacity", Long.toString(capacity));
        map.put("quotaflow.policies.u.limit.refill-amount", Long.toString(refillAmount));
        map.put("quotaflow.policies.u.limit.refill-period", refillPeriod);
        return map;
    }

    private static final RateLimitContext ALICE =
            RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "alice").build();

    private final MapConfigSource source = new MapConfigSource(payload(2));
    private final DefaultQuotaFlow quotaFlow = DefaultQuotaFlow
            .builder(ConfigurationParser.parse(payload(2)).policySet(), new LocalRateLimitStore())
            .build();

    @Test void endpointDefaultsAreEquivalentAndCredentialDiagnosticsAreRedacted() {
        assertEquals(StartupAccounting.fingerprintEndpoint("redis://localhost:6379"), StartupAccounting.fingerprintEndpoint("REDIS://LOCALHOST/0"));
        var candidate = payload(2);
        candidate.put("quotaflow.redis.url", "redis://user:secret-password@localhost:6379");
        var accounting = ConfigurationParser.parse(candidate).accounting();
        assertFalse(accounting.toString().contains("secret-password"));
        var reloader = ConfigReloader.builder(source, quotaFlow).build();
        source.update(candidate);
        var rejected = reloader.reload(); assertFalse(rejected.applied());
        assertFalse(rejected.error().contains("secret-password"));
        candidate.put("quotaflow.redis.url", "redis://bad secret-password");
        var failure = assertThrows(IllegalArgumentException.class, () -> ConfigurationParser.parse(candidate));
        assertFalse(failure.getMessage().contains("secret-password"));
        candidate.put("quotaflow.redis.url", "redis://localhost");
        candidate.put("quotaflow.recovery.mode", "COORDINATED");
        source.update(candidate); assertTrue(reloader.reload().applied());
    }

    @Test void accountingChangeRejectsTheEntireCandidateBeforePolicyPublicationOrHooks() {
        AtomicInteger hooks = new AtomicInteger();
        var reloader = ConfigReloader.builder(source, quotaFlow).onApplied(hooks::incrementAndGet).build();
        var changed = payload(20);
        changed.put("quotaflow.defaults.expected-instances", "2");
        changed.put("quotaflow.recovery.members", "a,b");
        changed.put("quotaflow.recovery.instance-id", "a");
        source.update(changed);
        assertFalse(reloader.reload().applied());
        assertEquals(0, hooks.get());
        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
        assertFalse(quotaFlow.tryAcquire("u", ALICE).isAllowed());
        var equivalent = payload(2);
        equivalent.put("quotaflow.defaults.expected-instances", "1");
        equivalent.put("quotaflow.namespace", "default");
        equivalent.put("quotaflow.recovery.members", "single");
        equivalent.put("quotaflow.recovery.instance-id", "single");
        equivalent.put("quotaflow.recovery.deployment-id", "default");
        source.update(equivalent);
        assertTrue(reloader.reload().applied());
        assertEquals(1, hooks.get());
    }

    @Test void capturesTheServingCohortAndRejectsIdentityOrNamespaceChanges() {
        var initial = payload(2);
        initial.put("quotaflow.defaults.expected-instances", "2");
        initial.put("quotaflow.recovery.members", "a,b");
        initial.put("quotaflow.recovery.instance-id", "a");
        var accounting = ConfigurationParser.parse(initial).accounting();
        var reloader = ConfigReloader.builder(source, quotaFlow).startupAccounting(accounting).build();
        source.update(initial);
        assertTrue(reloader.reload().applied());
        var reordered = new LinkedHashMap<>(initial);
        reordered.put("quotaflow.recovery.members", "b, a");
        source.update(reordered);
        assertTrue(reloader.reload().applied());
        for (var replacement : Map.of("quotaflow.recovery.instance-id", "b", "quotaflow.recovery.members", "a,c",
                "quotaflow.namespace", "other", "quotaflow.recovery.deployment-id", "other",
                "quotaflow.redis.url", "redis://other:6379", "quotaflow.recovery.mode", "uncoordinated").entrySet()) {
            var candidate = new LinkedHashMap<>(initial);
            candidate.put(replacement.getKey(), replacement.getValue());
            source.update(candidate);
            assertFalse(reloader.reload().applied(), replacement.getKey());
        }
    }

    @Test
    void reloadAppliesNewLimitsWithoutRestart() throws Exception {
        ConfigReloader reloader = ConfigReloader.builder(source, quotaFlow)
                .pollInterval(Duration.ZERO).build();
        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
        assertFalse(quotaFlow.tryAcquire("u", ALICE).isAllowed(), "initial capacity 2 exhausted");

        // the new limit refills quickly, so its adoption becomes observable
        source.update(payload(5, 50, "PT1S"));
        ReloadResult result = reloader.reload();
        assertTrue(result.applied());
        assertEquals(1, result.policyCount());
        awaitTrue(() -> quotaFlow.tryAcquire("u", ALICE).isAllowed(),
                Duration.ofSeconds(2), "new limit governs new decisions");
    }

    @Test
    void brokenReloadKeepsOldSetServingAndReportsTheOffendingKey() {
        AtomicInteger hooks = new AtomicInteger();
        ConfigReloader reloader = ConfigReloader.builder(source, quotaFlow)
                .pollInterval(Duration.ZERO)
                .onApplied(hooks::incrementAndGet)
                .build();
        Map<String, String> broken = payload(5);
        broken.put("quotaflow.policies.u.limt.capacity", "5");
        source.update(broken);

        ReloadResult result = reloader.reload();
        assertFalse(result.applied());
        assertTrue(result.error().contains("quotaflow.policies.u.limt.capacity"));
        assertEquals(0, hooks.get(), "hooks run only after a successful swap");

        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
        assertFalse(quotaFlow.tryAcquire("u", ALICE).isAllowed(),
                "the old set (capacity 2) still governs decisions");
    }

    @Test
    void invalidLimitValuesKeepTheOldSetServing() {
        ConfigReloader reloader = ConfigReloader.builder(source, quotaFlow)
                .pollInterval(Duration.ZERO).build();
        Map<String, String> broken = payload(5);
        broken.put("quotaflow.policies.u.limit.capacity", "-3");
        source.update(broken);
        assertFalse(reloader.reload().applied());
        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
    }

    @Test
    void failingSourceKeepsOldSetServing() {
        ConfigReloader reloader = ConfigReloader
                .builder(() -> {
                    throw new ConfigSourceException("boom", new RuntimeException());
                }, quotaFlow)
                .pollInterval(Duration.ZERO).build();
        ReloadResult result = reloader.reload();
        assertFalse(result.applied());
        assertTrue(result.error().contains("boom"));
        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
    }

    @Test
    void hookRunsAfterSuccessfulReload() {
        AtomicInteger hooks = new AtomicInteger();
        ConfigReloader reloader = ConfigReloader.builder(source, quotaFlow)
                .pollInterval(Duration.ZERO)
                .onApplied(hooks::incrementAndGet)
                .build();
        source.update(payload(3));
        reloader.reload();
        assertEquals(1, hooks.get());
    }

    @Test
    void startAppliesTheSourceImmediately() {
        source.update(payload(9));
        PolicySet startup = ConfigurationParser.parse(payload(2)).policySet();
        DefaultQuotaFlow flow =
                DefaultQuotaFlow.builder(startup, new LocalRateLimitStore()).build();
        ConfigReloader reloader = ConfigReloader.builder(source, flow)
                .pollInterval(Duration.ZERO).build();
        reloader.start();
        reloader.close();
        for (int i = 0; i < 9; i++) {
            assertTrue(flow.tryAcquire("u", ALICE).isAllowed(), "source payload applied at start");
        }
    }

    @Test
    void watcherAppliesFileChangesAndSurvivesBrokenEdits() throws Exception {
        java.nio.file.Path file = java.nio.file.Files.createTempFile("quotaflow", ".properties");
        write(file, payload(2));
        DefaultQuotaFlow flow = DefaultQuotaFlow
                .builder(ConfigurationParser.parse(payload(2)).policySet(), new LocalRateLimitStore())
                .build();
        ConfigReloader reloader = ConfigReloader
                .builder(new PropertiesFileConfigSource(file), flow)
                .pollInterval(Duration.ofMillis(20))
                .build();
        reloader.start();
        try {
            drain(flow, 2);

            Map<String, String> broken = payload(5);
            broken.put("quotaflow.policies.u.limit.capacity", "not-a-number");
            write(file, broken);
            Thread.sleep(200);
            assertFalse(flow.tryAcquire("u", ALICE).isAllowed(),
                    "broken edit never took effect; old capacity 2 still exhausted");

            write(file, payload(4, 50, "PT1S"));
            awaitTrue(() -> {
                Decision d = flow.tryAcquire("u", ALICE);
                return d.isAllowed();
            }, Duration.ofSeconds(2), "watcher applied the fixed edit");
        } finally {
            reloader.close();
            java.nio.file.Files.deleteIfExists(file);
        }
    }

    @Test
    void watcherSkipsUnchangedPayloads() throws Exception {
        source.update(payload(2));
        ConfigReloader reloader = ConfigReloader.builder(source, quotaFlow)
                .pollInterval(Duration.ofMillis(10))
                .build();
        reloader.start();
        Thread.sleep(100);
        reloader.close();
        assertTrue(quotaFlow.tryAcquire("u", ALICE).isAllowed());
    }

    @Test
    void builderRejectsNulls() {
        assertThrows(NullPointerException.class,
                () -> ConfigReloader.builder(null, quotaFlow));
        assertThrows(NullPointerException.class,
                () -> ConfigReloader.builder(source, null));
        assertThrows(NullPointerException.class,
                () -> ConfigReloader.builder(source, quotaFlow).pollInterval(null));
        assertThrows(NullPointerException.class,
                () -> ConfigReloader.builder(source, quotaFlow).onApplied(null));
    }

    private static void drain(DefaultQuotaFlow flow, int tokens) {
        for (int i = 0; i < tokens; i++) {
            assertTrue(flow.tryAcquire("u", ALICE).isAllowed());
        }
    }

    private static void write(java.nio.file.Path file, Map<String, String> payload)
            throws java.io.IOException {
        StringBuilder content = new StringBuilder();
        for (Map.Entry<String, String> entry : payload.entrySet()) {
            content.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
        }
        java.nio.file.Files.writeString(file, content.toString());
    }

    private static void awaitTrue(Check condition, Duration timeout, String description)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.check()) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timed out waiting for: " + description);
    }

    @FunctionalInterface
    private interface Check {
        boolean check();
    }
}
