package io.quotaflow.spring;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@SpringBootTest(
        classes = ServletRateLimitIntegrationTest.TestApplication.class,
        properties = {
            "quotaflow.policies.user-api.scope=user",
            "quotaflow.policies.user-api.limit.capacity=2",
            "quotaflow.policies.user-api.limit.refill-amount=2",
            "quotaflow.policies.user-api.limit.refill-period=PT1M",
            "quotaflow.policies.throttled-api.scope=user",
            "quotaflow.policies.throttled-api.reaction=throttle",
            "quotaflow.policies.throttled-api.limit.capacity=1",
            "quotaflow.policies.throttled-api.limit.refill-amount=1",
            "quotaflow.policies.throttled-api.limit.refill-period=PT0.15S",
            "quotaflow.policies.slow-api.scope=user",
            "quotaflow.policies.slow-api.reaction=throttle",
            "quotaflow.policies.slow-api.limit.capacity=1",
            "quotaflow.policies.slow-api.limit.refill-amount=1",
            "quotaflow.policies.slow-api.limit.refill-period=PT30S",
            "quotaflow.redis.url=redis://localhost:6390",
            "quotaflow.redis.connect-timeout=100ms"
        })
@AutoConfigureMockMvc
class ServletRateLimitIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void allowsWithinQuota() throws Exception {
        mockMvc.perform(get("/api/alice"))
                .andExpect(status().isOk())
                .andExpect(content().string("hello alice"));
    }

    @Test
    void rejectsWith429SemanticsWhenQuotaExhausted() throws Exception {
        mockMvc.perform(get("/api/frank")).andExpect(status().isOk());
        mockMvc.perform(get("/api/frank")).andExpect(status().isOk());

        mockMvc.perform(get("/api/frank"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().exists("Retry-After"))
                .andExpect(header().string("Retry-After", org.hamcrest.Matchers.matchesPattern("[1-9][0-9]*")))
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.type").value("urn:quotaflow:rate-limit-exceeded"))
                .andExpect(jsonPath("$.title").value("Rate limit exceeded"))
                .andExpect(jsonPath("$.status").value(429))
                .andExpect(jsonPath("$.detail").value(containsString("user-api")))
                .andExpect(jsonPath("$.detail").value(containsString("user")))
                // the raw limit key must never leak into the response
                .andExpect(jsonPath("$.detail").value(not(containsString("frank"))));
    }

    @Test
    void quotasAreTrackedPerKey() throws Exception {
        mockMvc.perform(get("/api/grace")).andExpect(status().isOk());
        mockMvc.perform(get("/api/grace")).andExpect(status().isOk());
        mockMvc.perform(get("/api/grace")).andExpect(status().isTooManyRequests());

        mockMvc.perform(get("/api/heidi")).andExpect(status().isOk());
    }

    @Test
    void throttledCallWaitsAndSucceeds() throws Exception {
        mockMvc.perform(get("/throttled/ivan")).andExpect(status().isOk());
        // immediate second call: throttle reaction waits for the refill instead of rejecting
        mockMvc.perform(get("/throttled/ivan"))
                .andExpect(status().isOk())
                .andExpect(content().string("hello ivan"));
    }

    @Test
    void waitTimeoutRejectionHasDistinctProblemType() throws Exception {
        mockMvc.perform(get("/slow/judy")).andExpect(status().isOk());

        mockMvc.perform(get("/slow/judy"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().exists("Retry-After"))
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.type").value("urn:quotaflow:rate-limit-wait-timeout"))
                .andExpect(jsonPath("$.title").value("Rate limit wait timeout"))
                .andExpect(jsonPath("$.detail").value(containsString("slow-api")))
                .andExpect(jsonPath("$.detail").value(not(containsString("judy"))));
    }

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    static class TestApplication {

        @RestController
        static class LimitedController {

            @GetMapping("/api/{user}")
            @RateLimited(policy = "user-api", key = "#user")
            public String limited(@PathVariable String user) {
                return "hello " + user;
            }

            @GetMapping("/throttled/{user}")
            @RateLimited(policy = "throttled-api", key = "#user", waitTimeout = "2s")
            public String throttled(@PathVariable String user) {
                return "hello " + user;
            }

            @GetMapping("/slow/{user}")
            @RateLimited(policy = "slow-api", key = "#user", waitTimeout = "50ms")
            public String slow(@PathVariable String user) {
                return "hello " + user;
            }
        }
    }
}
