package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.*;

import io.quotaflow.core.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class LocalPolicyBindingsTest {
    private PolicyBinding binding(String policy, String root, Scope scope) {
        return new PolicyBinding(new QuotaDomain("test", root), policy, scope);
    }

    @Test
    void preservesBindingsAndRejectsEntireInvalidOrOverBudgetCandidate() {
        LocalPolicyBindings registry = new LocalPolicyBindings(2);
        PolicyBinding original = binding("one", "root", Scope.USER);
        registry.register(List.of(original));
        registry.register(List.of(original));
        assertEquals(1, registry.size("test"));
        assertThrows(PolicyConfigurationException.class, () -> registry.register(List.of(
                binding("two", "root", Scope.USER), binding("one", "other", Scope.USER))));
        assertEquals(1, registry.size("test"));
        assertThrows(PolicyConfigurationException.class, () -> registry.register(List.of(
                binding("two", "root", Scope.USER), binding("three", "root", Scope.USER))));
        assertEquals(1, registry.size("test"));
        assertThrows(PolicyConfigurationException.class, () -> registry.register(List.of(
                binding("two", "root", Scope.USER), binding("two", "root", Scope.TENANT))));
        registry.register(List.of()); // Removal from an active set never deletes history.
        assertThrows(PolicyConfigurationException.class,
                () -> registry.register(List.of(binding("one", "root", Scope.TENANT))));
        registry.register(List.of(binding("two", "root", Scope.USER)));
        assertEquals(2, registry.size("test"));
        assertEquals(0, registry.size("other"));
        registry.register(List.of(new PolicyBinding(new QuotaDomain("other", "root"), "one", Scope.GLOBAL)));
        assertEquals(1, registry.size("other"));
    }

    @Test
    void concurrentCandidatesNeverLeavePartialRegistrations() throws Exception {
        LocalPolicyBindings registry = new LocalPolicyBindings(3);
        var executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        try {
            var tasks = java.util.stream.IntStream.range(0, 2).mapToObj(i -> executor.submit(() -> {
                start.await();
                try {
                    registry.register(List.of(binding("a" + i, "root", Scope.USER), binding("b" + i, "root", Scope.USER)));
                    accepted.incrementAndGet();
                } catch (PolicyConfigurationException expected) { }
                return null;
            })).toList();
            start.countDown();
            for (var task : tasks) task.get();
            assertEquals(1, accepted.get());
            assertEquals(2, registry.size("test"));
        } finally { executor.shutdownNow(); }
    }

    @Test
    void incompatibleReplacementAndRemoveReaddKeepServingPolicyAndDebt() {
        var old = RateLimitPolicy.builder("p").scope(Scope.USER)
                .limit(new Limit(3, 1, Duration.ofHours(1))).build();
        var store = new LocalRateLimitStore();
        var flow = DefaultQuotaFlow.builder(PolicySet.compile(List.of(old)), store).build();
        var context = RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "alice").build();
        assertTrue(flow.tryAcquire("p", context).isAllowed());
        var changed = RateLimitPolicy.builder("p").scope(Scope.TENANT).limit(old.limit().orElseThrow()).build();
        assertThrows(PolicyConfigurationException.class, () -> flow.replacePolicySet(PolicySet.compile(List.of(changed))));
        assertEquals(Scope.USER, flow.tryAcquire("p", context).scope());
        var different = RateLimitPolicy.builder("other").scope(Scope.GLOBAL).limit(old.limit().orElseThrow()).build();
        flow.replacePolicySet(PolicySet.compile(List.of(different)));
        assertThrows(PolicyConfigurationException.class, () -> flow.replacePolicySet(PolicySet.compile(List.of(changed))));
        flow.replacePolicySet(PolicySet.compile(List.of(old)));
        assertEquals(0, flow.tryAcquire("p", context).remaining());
    }

    @Test
    void invalidBindingsAndBudgetsFailEarly() {
        assertThrows(IllegalArgumentException.class, () -> new LocalPolicyBindings(0));
        assertThrows(NullPointerException.class, () -> new LocalPolicyBindings().register((List<PolicyBinding>) null));
        assertThrows(NullPointerException.class, () -> new PolicyBinding(null, "p", Scope.GLOBAL));
        assertThrows(NullPointerException.class, () -> new PolicyBinding(new QuotaDomain("n", "p"), "p", null));
        assertThrows(IllegalArgumentException.class, () -> new PolicyBinding(new QuotaDomain("n", "p"), "", Scope.GLOBAL));
        assertThrows(IllegalArgumentException.class, () -> DefaultQuotaFlow.builder(
                PolicySet.compile(List.of(RateLimitPolicy.builder("p").scope(Scope.GLOBAL)
                        .limit(new Limit(1, 1, Duration.ofSeconds(1))).build())), new LocalRateLimitStore()).namespace(""));
    }
}
