package io.quotaflow.store.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class RedisKeySchemeTest {

    private final RedisKeyScheme scheme = RedisKeyScheme.defaults();

    @Test
    void singleKeyUsesOwnIdentityAsHashTag() {
        assertEquals("{u:alice}:u:user:alice", scheme.singleKey("u:user:alice"));
    }

    @Test
    void chainKeysShareTheLeafHashTag() {
        List<String> keys = scheme.chainKeys(List.of(
                "g:global:global", "t:tenant:acme", "u:user:alice"));
        assertEquals(List.of(
                "{u:alice}:g:global:global",
                "{u:alice}:t:tenant:acme",
                "{u:alice}:u:user:alice"), keys);
    }

    @Test
    void differentLeavesGetDifferentTagsSoChainsSpreadAcrossSlots() {
        List<String> alice = scheme.chainKeys(List.of("t:tenant:acme", "u:user:alice"));
        List<String> bob = scheme.chainKeys(List.of("t:tenant:acme", "u:user:bob"));
        assertNotEquals(alice.get(0), bob.get(0));
        assertTrue(alice.get(0).startsWith("{u:alice}:"));
        assertTrue(bob.get(0).startsWith("{u:bob}:"));
    }

    @Test
    void sameChainAlwaysMapsToTheSameKeys() {
        List<String> chain = List.of("t:tenant:acme", "u:user:alice");
        assertEquals(scheme.chainKeys(chain), scheme.chainKeys(chain));
    }

    @Test
    void overlongRawKeysAreHashedDeterministically() {
        RedisKeyScheme tight = new RedisKeyScheme(16);
        String longRaw = "tenant-" + "x".repeat(200);
        String key = tight.singleKey("t:tenant:" + longRaw);
        assertTrue(key.startsWith("{t:sha256:"));
        assertTrue(key.contains("}:t:tenant:sha256:"));
        assertEquals(key, tight.singleKey("t:tenant:" + longRaw));
        assertNotEquals(key, tight.singleKey("t:tenant:" + longRaw + "-other"));
        // 7 ("{t:sha256:") + 64 (hex) for the tag content plus separators — bounded size
        assertTrue(key.length() < longRaw.length());
    }

    @Test
    void shortRawKeysAreKeptVerbatim() {
        RedisKeyScheme tight = new RedisKeyScheme(16);
        assertEquals("{u:short}:u:user:short", tight.singleKey("u:user:short"));
    }

    @Test
    void malformedStorageKeyRejectedWithoutEchoingIt() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> scheme.singleKey("sensitive-raw-key-without-separators"));
        assertFalse(e.getMessage().contains("sensitive"));
    }

    @Test
    void emptyChainRejected() {
        assertThrows(IllegalArgumentException.class, () -> scheme.chainKeys(List.of()));
    }

    @Test
    void invalidBoundRejected() {
        assertThrows(IllegalArgumentException.class, () -> new RedisKeyScheme(0));
    }

    @Test
    void rawKeyWithColonsStaysInTheTail() {
        assertEquals("{u:a:b:c}:u:user:a:b:c", scheme.singleKey("u:user:a:b:c"));
    }
}
