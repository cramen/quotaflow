package io.quotaflow.spring;

import java.nio.charset.StandardCharsets;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

/**
 * Default reactive mapping of {@link RateLimitExceededException} to HTTP 429,
 * with the same {@code Retry-After} and problem+json semantics as the servlet
 * default ({@link RateLimitExceptionHandler}). The body is written
 * non-blocking; any other exception is passed down the handler chain
 * untouched.
 *
 * <p>Ordered at {@code -2} so it precedes Spring Boot's default error handler
 * ({@code -1}) and the response-status handler, exactly where custom
 * {@link WebExceptionHandler}s belong. Replace the mapping by declaring an
 * own bean of this type; the auto-configured default backs off. An own
 * {@link WebExceptionHandler} of a different type ordered before this one
 * takes precedence as well.
 */
@Order(-2)
public class RateLimitWebExceptionHandler implements WebExceptionHandler {

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable exception) {
        if (!(exception instanceof RateLimitExceededException rejected)) {
            return Mono.error(exception);
        }
        RateLimitProblem problem = RateLimitProblem.of(rejected.decision());
        var response = exchange.getResponse();
        response.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        response.getHeaders().setContentType(MediaType.APPLICATION_PROBLEM_JSON);
        if (problem.retryAfterSeconds().isPresent()) {
            response.getHeaders().set(
                    HttpHeaders.RETRY_AFTER, Long.toString(problem.retryAfterSeconds().orElseThrow()));
        }
        byte[] body = problem.toJson().getBytes(StandardCharsets.UTF_8);
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }
}
