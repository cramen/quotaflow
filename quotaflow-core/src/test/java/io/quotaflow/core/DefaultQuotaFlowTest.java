package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class DefaultQuotaFlowTest {

    private final AtomicLong nanos = new AtomicLong();
    private final LocalRateLimitStore store = new LocalRateLimitStore(nanos::get);

    private static RateLimitPolicy userPolicy(String id, long capacity) {
        return RateLimitPolicy.builder(id)
                .limit(new Limit(capacity, 1, Duration.ofMillis(1)))
                .scope(Scope.USER)
                .build();
    }

    private static RateLimitContext principal(String name) {
        return RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, name).build();
    }

    @Test
    void rejectionIsReturnedAsDataNotThrown() {
        QuotaFlow quotaFlow = DefaultQuotaFlow.builder(
                        PolicySet.compile(List.of(userPolicy("u", 1))), store)
                .build();
        assertTrue(quotaFlow.tryAcquire("u", principal("alice")).isAllowed());
        Decision rejected = quotaFlow.tryAcquire("u", principal("alice"));
        assertFalse(rejected.isAllowed());
        assertEquals(Verdict.REJECTED, rejected.verdict());
    }

    @Test
    void listenersFireForAllowAndRejectWithKeyGroupOnly() {
        List<Decision> decisions = new CopyOnWriteArrayList<>();
        List<String> keyGroups = new CopyOnWriteArrayList<>();
        QuotaFlow quotaFlow = DefaultQuotaFlow.builder(
                        PolicySet.compile(List.of(userPolicy("u", 1))), store)
                .addListener((decision, keyGroup) -> {
                    decisions.add(decision);
                    keyGroups.add(keyGroup);
                })
                .build();
        quotaFlow.tryAcquire("u", principal("alice"));
        quotaFlow.tryAcquire("u", principal("alice"));
        assertEquals(List.of(Verdict.ALLOWED, Verdict.REJECTED),
                decisions.stream().map(Decision::verdict).toList());
        assertEquals(List.of("principal", "principal"), keyGroups);
        // the raw key value is never exposed to listeners
        assertFalse(keyGroups.contains("alice"));
    }

    @Test
    void listenerReceivesUnresolvableGroupForMissingKey() {
        List<String> keyGroups = new CopyOnWriteArrayList<>();
        QuotaFlow quotaFlow = DefaultQuotaFlow.builder(
                        PolicySet.compile(List.of(userPolicy("u", 1))), store)
                .addListener((decision, keyGroup) -> keyGroups.add(keyGroup))
                .build();
        Decision decision = quotaFlow.tryAcquire("u", RateLimitContext.empty());
        assertFalse(decision.isAllowed());
        assertEquals(List.of("unresolvable"), keyGroups);
    }

    @Test
    void asyncAcquireFiresListeners() {
        List<Decision> decisions = new CopyOnWriteArrayList<>();
        QuotaFlow quotaFlow = DefaultQuotaFlow.builder(
                        PolicySet.compile(List.of(userPolicy("u", 5))), store)
                .addListener((decision, keyGroup) -> decisions.add(decision))
                .build();
        Decision decision = quotaFlow.tryAcquireAsync("u", principal("alice"), 2)
                .toCompletableFuture().join();
        assertTrue(decision.isAllowed());
        assertEquals(1, decisions.size());
        assertEquals(3, decisions.get(0).remaining());
    }

    @Test
    void namedResolversAndCustomDefaultResolverCompose() {
        PolicySet set = PolicySet.compile(List.of(
                RateLimitPolicy.builder("u")
                        .limit(new Limit(1, 1, Duration.ofMillis(1)))
                        .scope(Scope.USER)
                        .keyResolverId(KeyResolvers.PRINCIPAL_ID)
                        .build(),
                RateLimitPolicy.builder("g")
                        .limit(new Limit(1, 1, Duration.ofMillis(1)))
                        .scope(Scope.GLOBAL)
                        .build()));
        List<String> keyGroups = new CopyOnWriteArrayList<>();
        QuotaFlow quotaFlow = DefaultQuotaFlow.builder(set, store)
                .defaultResolver(KeyResolvers.scopeBased())
                .addResolver(KeyResolvers.PRINCIPAL_ID, KeyResolvers.principal())
                .addListener((decision, keyGroup) -> keyGroups.add(keyGroup))
                .build();
        assertTrue(quotaFlow.tryAcquire("u", principal("alice")).isAllowed());
        assertEquals(List.of("principal"), keyGroups);
        assertTrue(quotaFlow.tryAcquire("g", RateLimitContext.empty()).isAllowed());
    }

    @Test
    void builderRejectsNulls() {
        PolicySet set = PolicySet.compile(List.of(userPolicy("u", 1)));
        assertThrows(NullPointerException.class, () -> DefaultQuotaFlow.builder(null, store));
        assertThrows(NullPointerException.class, () -> DefaultQuotaFlow.builder(set, null));
        DefaultQuotaFlow.Builder builder = DefaultQuotaFlow.builder(set, store);
        assertThrows(NullPointerException.class, () -> builder.defaultResolver(null));
        assertThrows(NullPointerException.class, () -> builder.addResolver(null, KeyResolvers.principal()));
        assertThrows(NullPointerException.class, () -> builder.addResolver("x", null));
        assertThrows(NullPointerException.class, () -> builder.addListener(null));
    }

    @Test
    void replacePolicySetRejectsNull() {
        DefaultQuotaFlow quotaFlow = DefaultQuotaFlow.builder(
                        PolicySet.compile(List.of(userPolicy("u", 1))), store)
                .build();
        assertThrows(NullPointerException.class, () -> quotaFlow.replacePolicySet(null));
    }

    @Test
    void replacedPolicySetServesSubsequentDecisions() {
        DefaultQuotaFlow quotaFlow = DefaultQuotaFlow.builder(
                        PolicySet.compile(List.of(userPolicy("u", 1))), store)
                .build();
        assertTrue(quotaFlow.tryAcquire("u", principal("alice")).isAllowed());
        assertFalse(quotaFlow.tryAcquire("u", principal("alice")).isAllowed());
        quotaFlow.replacePolicySet(PolicySet.compile(List.of(userPolicy("u", 3))));
        // the new set is active for subsequent decisions: a fresh principal gets the larger capacity
        Decision decision = quotaFlow.tryAcquire("u", principal("bob"));
        assertTrue(decision.isAllowed());
        assertEquals(2, decision.remaining());
    }
}
