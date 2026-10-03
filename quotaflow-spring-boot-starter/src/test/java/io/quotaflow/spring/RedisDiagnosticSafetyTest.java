package io.quotaflow.spring;

import ch.qos.logback.classic.*;
import ch.qos.logback.classic.spi.*;
import ch.qos.logback.core.read.ListAppender;
import java.io.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class RedisDiagnosticSafetyTest {
    @Test void failedConnectionNeverExposesUserinfoQueryOrMalformedInput() {
        var logger=(Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);var previous=logger.getLevel();
        var protocol=(Logger)LoggerFactory.getLogger("io.lettuce.core.protocol");var previousProtocol=protocol.getLevel();protocol.setLevel(Level.INFO);
        var events=new ListAppender<ILoggingEvent>();events.start();logger.addAppender(events);logger.setLevel(Level.ALL);
        try {
            for(String url:new String[]{"redis://fixture_user:fixture_pass@localhost:1?password=fixture_query", "rediss://fixture_user:fixture_pass@localhost:1", "redis-sentinel://fixture_user:fixture_pass@localhost:1?sentinelMasterId=primary", "redis://fixture_user:fixture%5Fpass@localhost:1", "redis://fixture_user:fixture_pass@bad host"}) {
                events.list.clear();var properties=new QuotaFlowProperties.Redis();properties.setUrl(url);
                properties.setConnectTimeout(Duration.ofMillis(100));properties.setCommandTimeout(Duration.ofMillis(100));
                Throwable failure=assertThrows(Exception.class,()->RedisStoreFactory.recoveryConnection(properties,"default"));
                var rendered=new StringWriter();failure.printStackTrace(new PrintWriter(rendered));
                for(var event:events.list) {
                    rendered.append(event.getFormattedMessage());
                    if(event.getThrowableProxy()!=null) rendered.append(ThrowableProxyUtil.asString(event.getThrowableProxy()));
                }
                String text=rendered.toString();
                for(String secret:new String[]{"fixture_user","fixture_pass","fixture%5Fpass","fixture_query"}) assertFalse(text.contains(secret),"diagnostics exposed "+secret);
            }
        } finally {logger.setLevel(previous);protocol.setLevel(previousProtocol);logger.detachAppender(events);events.stop();}
    }
    @Test void realAuthenticationWorksWithoutCredentialOrQueryLogging() {
        String password="fixture_wire_pass", username="fixture_wire_user", clientName="fixture_wire_query";
        try(var redis=new org.testcontainers.containers.GenericContainer<>(org.testcontainers.utility.DockerImageName.parse("redis:6.2.24-alpine"))
                .withCommand("redis-server","--requirepass",password).withExposedPorts(6379)) {
            redis.start();
            String endpoint=redis.getHost()+":"+redis.getMappedPort(6379);
            var admin=io.quotaflow.store.redis.RedisClientFactory.createClient("redis://default:"+password+"@"+endpoint,Duration.ofSeconds(1));
            try(var connection=admin.connect()) { connection.sync().aclSetuser(username,new io.lettuce.core.AclSetuserArgs().on().addPassword(password).allKeys().allCommands()); }
            finally {admin.shutdown();}
            var logger=(Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);var previous=logger.getLevel();
            var protocol=(Logger)LoggerFactory.getLogger("io.lettuce.core.protocol");var previousProtocol=protocol.getLevel();protocol.setLevel(Level.INFO);
        var events=new ListAppender<ILoggingEvent>();events.start();logger.addAppender(events);logger.setLevel(Level.ALL);
            try {
                var client=io.quotaflow.store.redis.RedisClientFactory.createClient("redis://"+username+":"+password+"@"+endpoint+"/3?clientName="+clientName,Duration.ofSeconds(1));
                try(var connection=client.connect()) {
                    assertEquals("PONG",connection.sync().ping());
                    assertEquals(clientName,connection.sync().clientGetname());
                    assertEquals("OK",connection.sync().set("verification-database", "three"));
                    connection.sync().select(0);
                    assertNull(connection.sync().get("verification-database"));
                    connection.sync().select(3);
                    assertEquals("three",connection.sync().get("verification-database"));
                }
                finally {client.shutdown();}
                var text=new StringBuilder();
                for(var event:events.list) {
                    text.append(event.getFormattedMessage());
                    if(event.getThrowableProxy()!=null)text.append(ThrowableProxyUtil.asString(event.getThrowableProxy()));
                }
                assertFalse(text.toString().contains(password),"wire diagnostics exposed password");
                assertFalse(text.toString().contains(username),"wire diagnostics exposed username");
                assertFalse(text.toString().contains(clientName),"wire diagnostics exposed query value");
            } finally {logger.setLevel(previous);protocol.setLevel(previousProtocol);logger.detachAppender(events);events.stop();}
        }
    }

    @Test void nestedDriverFailuresAreNotRetainedInSafeExceptions() {
        for(Throwable cause:new Throwable[]{new IllegalArgumentException("fixture_nested"),new LinkageError("fixture_nested"),new javax.net.ssl.SSLException("fixture_nested")}) {
            cause.addSuppressed(new IllegalStateException("fixture_suppressed"));
            var safe=io.quotaflow.store.redis.SafeRedisDiagnostics.failure("rediss://fixture_user:fixture_pass@localhost:1?password=fixture_query",cause);
            var rendered=new StringWriter();safe.printStackTrace(new PrintWriter(rendered));
            assertFalse(rendered.toString().contains("fixture_"));assertNull(safe.getCause());assertEquals(0,safe.getSuppressed().length);
        }
    }

}
