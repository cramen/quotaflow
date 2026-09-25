package io.quotaflow.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.quotaflow.config.ConfigReloader;
import io.quotaflow.core.QuotaFlow;
import io.quotaflow.core.RateLimitContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * Zero-config smoke: one policy in application properties, no declared beans —
 * the starter assembles the whole stack (facade, fallback wrapper, reload
 * pipeline, annotation machinery, 429 handler) and limits immediately.
 */
@SpringBootTest(
        classes = ZeroConfigSmokeTest.TestApplication.class,
        properties = {
            "quotaflow.policies.user-api.scope=user",
            "quotaflow.policies.user-api.limit.capacity=2",
            "quotaflow.policies.user-api.limit.refill-amount=2",
            "quotaflow.policies.user-api.limit.refill-period=PT1M",
            "quotaflow.redis.url=redis://localhost:6390",
            "quotaflow.redis.connect-timeout=100ms"
        })
class ZeroConfigSmokeTest {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private QuotaFlow quotaFlow;

    @Test
    void fullStackActiveWithoutDeclaredBeans() {
        assertThat(context.getBeanNamesForType(QuotaFlow.class)).hasSize(1);
        assertThat(context.getBeanNamesForType(ConfigReloader.class)).hasSize(1);
        assertThat(context.getBeanNamesForType(RateLimitInterceptor.class)).hasSize(1);
        assertThat(context.getBeanNamesForType(RateLimitedAdvisor.class)).hasSize(1);
        assertThat(context.getBeanNamesForType(RateLimitExceptionHandler.class)).hasSize(1);

        RateLimitContext alice =
                RateLimitContext.builder().put(RateLimitContext.PRINCIPAL, "alice").build();
        assertThat(quotaFlow.tryAcquire("user-api", alice).isAllowed()).isTrue();
        assertThat(quotaFlow.tryAcquire("user-api", alice).isAllowed()).isTrue();
        assertThat(quotaFlow.tryAcquire("user-api", alice).isAllowed()).isFalse();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {
    }
}
