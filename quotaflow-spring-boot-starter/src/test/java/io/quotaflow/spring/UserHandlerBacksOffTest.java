package io.quotaflow.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@SpringBootTest(
        classes = UserHandlerBacksOffTest.TestApplication.class,
        properties = {
            "quotaflow.policies.user-api.scope=user",
            "quotaflow.policies.user-api.limit.capacity=1",
            "quotaflow.policies.user-api.limit.refill-amount=1",
            "quotaflow.policies.user-api.limit.refill-period=PT1M",
            "quotaflow.redis.url=redis://localhost:6390",
            "quotaflow.redis.connect-timeout=100ms"
        })
@AutoConfigureMockMvc
class UserHandlerBacksOffTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private org.springframework.context.ApplicationContext context;

    @Test
    void userDeclaredHandlerReplacesTheDefault() throws Exception {
        assertThat(context.getBeanNamesForType(RateLimitExceptionHandler.class)).hasSize(1);

        mockMvc.perform(get("/api/alice")).andExpect(status().isOk());
        mockMvc.perform(get("/api/alice"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().string("X-Custom-Handler", "true"))
                .andExpect(jsonPath("$.detail").value("custom mapping"));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {

        @Bean
        RateLimitExceptionHandler customRateLimitExceptionHandler() {
            return new RateLimitExceptionHandler() {
                @Override
                @ExceptionHandler(RateLimitExceededException.class)
                public ResponseEntity<ProblemDetail> handleRateLimitExceeded(
                        RateLimitExceededException exception) {
                    ProblemDetail body =
                            ProblemDetail.forStatusAndDetail(HttpStatus.TOO_MANY_REQUESTS, "custom mapping");
                    return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                            .header("X-Custom-Handler", "true")
                            .body(body);
                }
            };
        }

        @RestController
        static class LimitedController {

            @GetMapping("/api/{user}")
            @RateLimited(policy = "user-api", key = "#user")
            public String limited(@PathVariable String user) {
                return "hello " + user;
            }
        }
    }
}
