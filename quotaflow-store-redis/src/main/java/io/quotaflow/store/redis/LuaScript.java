package io.quotaflow.store.redis;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** A Lua script loaded from the classpath together with its EVALSHA digest. */
final class LuaScript {

    private final String source;
    private final String sha1;

    private LuaScript(String source, String sha1) {
        this.source = source;
        this.sha1 = sha1;
    }

    static LuaScript load(String classpathResource) {
        byte[] bytes;
        try (InputStream in = LuaScript.class.getResourceAsStream(classpathResource)) {
            if (in == null) {
                throw new IllegalStateException(
                        "missing Lua script on the classpath: " + classpathResource);
            }
            bytes = in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read Lua script " + classpathResource, e);
        }
        return new LuaScript(new String(bytes, StandardCharsets.UTF_8), sha1Hex(bytes));
    }

    String source() {
        return source;
    }

    String sha1() {
        return sha1;
    }

    private static String sha1Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-1").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 is required by the Java platform", e);
        }
    }
}
