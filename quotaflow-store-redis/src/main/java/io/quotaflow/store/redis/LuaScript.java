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
        String source = new String(bytes, StandardCharsets.UTF_8);
        if (classpathResource.equals("/lua/token_bucket.lua") || classpathResource.equals("/lua/gcra.lua")
                || classpathResource.equals("/lua/chain.lua") || classpathResource.equals("/lua/seed.lua")) {
            source = load("/lua/numeric_state.lua").source() + "\n" + source;
        }
        return new LuaScript(source, sha1Hex(source.getBytes(StandardCharsets.UTF_8)));
    }

    static LuaScript coordinated(String resource, String operation) {
        if (!operation.equals("acquire") && !operation.equals("seed")) throw new IllegalArgumentException("unknown operation");
        String source = load("/lua/recovery_guard.lua").source() + "\nif not recovery_authorize('" + operation
                + "') then return {-20} end\n"
                + (operation.equals("seed") ? "if #KEYS == 0 then return {1} end\n" : "") + load(resource).source();
        return new LuaScript(source, sha1Hex(source.getBytes(StandardCharsets.UTF_8)));
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
