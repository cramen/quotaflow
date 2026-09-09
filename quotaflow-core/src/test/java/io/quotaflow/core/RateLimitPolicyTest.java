package io.quotaflow.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class RateLimitPolicyTest {

    private static final Limit LIMIT = new Limit(5, 1, Duration.ofSeconds(1));

    @Test
    void builderAppliesDefaults() {
        RateLimitPolicy policy = RateLimitPolicy.builder("p")
                .limit(LIMIT)
                .scope(Scope.USER)
                .build();
        assertEquals("p", policy.id());
        assertEquals(LIMIT, policy.limit().orElseThrow());
        assertTrue(policy.limitRef().isEmpty());
        assertEquals(Algorithm.TOKEN_BUCKET, policy.algorithm());
        assertEquals(Reaction.REJECT, policy.reaction());
        assertEquals(0, policy.priority());
        assertTrue(policy.parentId().isEmpty());
        assertTrue(policy.keyResolverId().isEmpty());
        assertTrue(policy.defaultKey().isEmpty());
    }

    @Test
    void builderCarriesAllDimensions() {
        RateLimitPolicy policy = RateLimitPolicy.builder("child")
                .limit(LIMIT)
                .algorithm(Algorithm.GCRA)
                .scope(Scope.KEY)
                .reaction(Reaction.THROTTLE)
                .priority(7)
                .parentId("parent")
                .keyResolverId("custom")
                .defaultKey("anon")
                .build();
        assertEquals(Algorithm.GCRA, policy.algorithm());
        assertEquals(Scope.KEY, policy.scope());
        assertEquals(Reaction.THROTTLE, policy.reaction());
        assertEquals(7, policy.priority());
        assertEquals("parent", policy.parentId().orElseThrow());
        assertEquals("custom", policy.keyResolverId().orElseThrow());
        assertEquals("anon", policy.defaultKey().orElseThrow());
    }

    @Test
    void rejectsBlankId() {
        assertThrows(IllegalArgumentException.class,
                () -> RateLimitPolicy.builder(" ").limit(LIMIT).scope(Scope.USER).build());
        assertThrows(IllegalArgumentException.class,
                () -> RateLimitPolicy.builder(null).limit(LIMIT).scope(Scope.USER).build());
    }

    @Test
    void rejectsMissingComponents() {
        assertThrows(NullPointerException.class,
                () -> RateLimitPolicy.builder("p").limit(LIMIT).build());
        assertThrows(NullPointerException.class,
                () -> RateLimitPolicy.builder("p").limit(LIMIT).scope(Scope.USER).algorithm(null).build());
        assertThrows(NullPointerException.class,
                () -> RateLimitPolicy.builder("p").limit(LIMIT).scope(Scope.USER).reaction(null).build());
    }

    @Test
    void limitRefPolicyCarriesNoStaticLimit() {
        RateLimitPolicy policy = RateLimitPolicy.builder("p")
                .limitRef("tariff")
                .scope(Scope.USER)
                .build();
        assertTrue(policy.limit().isEmpty());
        assertEquals("tariff", policy.limitRef().orElseThrow());
    }

    @Test
    void declaresExactlyOneLimitSource() {
        PolicyConfigurationException both = assertThrows(PolicyConfigurationException.class,
                () -> RateLimitPolicy.builder("p")
                        .limit(LIMIT).limitRef("tariff").scope(Scope.USER).build());
        assertTrue(both.getMessage().contains("'p'"));
        PolicyConfigurationException neither = assertThrows(PolicyConfigurationException.class,
                () -> RateLimitPolicy.builder("q").scope(Scope.USER).build());
        assertTrue(neither.getMessage().contains("'q'"));
    }

    @Test
    void rejectsBlankLimitRef() {
        assertThrows(IllegalArgumentException.class,
                () -> RateLimitPolicy.builder("p").limitRef(" ").scope(Scope.USER).build());
    }

    @Test
    void rejectsSelfParent() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RateLimitPolicy.builder("p")
                        .limit(LIMIT).scope(Scope.USER).parentId("p").build());
        assertTrue(e.getMessage().contains("p"));
    }
}
