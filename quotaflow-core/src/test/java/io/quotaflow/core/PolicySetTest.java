package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class PolicySetTest {

    private static RateLimitPolicy policy(String id, Scope scope, String parentId) {
        return RateLimitPolicy.builder(id)
                .limit(new Limit(10, 1, Duration.ofSeconds(1)))
                .scope(scope)
                .parentId(parentId)
                .build();
    }

    private static RateLimitPolicy policy(String id, Scope scope) {
        return policy(id, scope, null);
    }

    @Test
    void compilesValidChainAndResolvesRootToLeaf() {
        PolicySet set = PolicySet.compile(List.of(
                policy("g", Scope.GLOBAL),
                policy("t", Scope.TENANT, "g"),
                policy("u", Scope.USER, "t"),
                policy("k", Scope.KEY, "u")));
        assertEquals(4, set.size());
        List<RateLimitPolicy> chain = set.chainFromLeaf("k");
        assertEquals(List.of("g", "t", "u", "k"), chain.stream().map(RateLimitPolicy::id).toList());
        assertEquals("t", set.policy("t").id());
        assertTrue(set.policies().stream().anyMatch(p -> p.id().equals("u")));
    }

    @Test
    void chainOfRootOnlyContainsRoot() {
        PolicySet set = PolicySet.compile(List.of(policy("g", Scope.GLOBAL)));
        assertEquals(List.of("g"), set.chainFromLeaf("g").stream().map(RateLimitPolicy::id).toList());
    }

    @Test
    void rejectsEmptySet() {
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> PolicySet.compile(List.of()));
        assertTrue(e.getMessage().contains("at least one"));
    }

    @Test
    void rejectsNullCollection() {
        assertThrows(NullPointerException.class, () -> PolicySet.compile(null));
    }

    @Test
    void rejectsDuplicateIds() {
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> PolicySet.compile(List.of(policy("a", Scope.GLOBAL), policy("a", Scope.GLOBAL))));
        assertTrue(e.getMessage().contains("a"));
    }

    @Test
    void rejectsUnknownParentNamingPolicyAndParent() {
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> PolicySet.compile(List.of(policy("child", Scope.TENANT, "ghost"))));
        assertTrue(e.getMessage().contains("child"));
        assertTrue(e.getMessage().contains("ghost"));
    }

    @Test
    void rejectsTwoNodeCycleNamingBothPolicies() {
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> PolicySet.compile(List.of(
                        policy("a", Scope.TENANT, "b"),
                        policy("b", Scope.GLOBAL, "a"))));
        assertTrue(e.getMessage().contains("a"));
        assertTrue(e.getMessage().contains("b"));
        assertTrue(e.getMessage().toLowerCase().contains("cycle"));
    }

    @Test
    void rejectsThreeNodeCycle() {
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> PolicySet.compile(List.of(
                        policy("a", Scope.USER, "b"),
                        policy("b", Scope.TENANT, "c"),
                        policy("c", Scope.GLOBAL, "a"))));
        assertTrue(e.getMessage().contains("a"));
    }

    @Test
    void rejectsChildScopeBroaderThanParent() {
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> PolicySet.compile(List.of(
                        policy("u", Scope.USER),
                        policy("t", Scope.TENANT, "u"))));
        assertTrue(e.getMessage().contains("narrower"));
        assertTrue(e.getMessage().contains("t"));
        assertTrue(e.getMessage().contains("u"));
    }

    @Test
    void rejectsChildScopeEqualToParent() {
        assertThrows(PolicyConfigurationException.class,
                () -> PolicySet.compile(List.of(
                        policy("p", Scope.TENANT),
                        policy("c", Scope.TENANT, "p"))));
    }

    @Test
    void unknownPolicyIdIsCallerMisuse() {
        PolicySet set = PolicySet.compile(List.of(policy("g", Scope.GLOBAL)));
        PolicyConfigurationException e = assertThrows(PolicyConfigurationException.class,
                () -> set.policy("nope"));
        assertTrue(e.getMessage().contains("nope"));
        assertThrows(PolicyConfigurationException.class, () -> set.chainFromLeaf("nope"));
    }

    @Test
    void scopeBreadthOrdering() {
        assertTrue(Scope.GLOBAL.isBroaderThan(Scope.KEY));
        assertTrue(Scope.TENANT.isBroaderThan(Scope.USER));
        assertTrue(!Scope.USER.isBroaderThan(Scope.TENANT));
        assertTrue(!Scope.KEY.isBroaderThan(Scope.KEY));
        assertEquals("tenant", Scope.TENANT.wireName());
    }
}
