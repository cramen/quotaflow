package io.quotaflow.spring;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Default servlet mapping of {@link RateLimitExceededException} to HTTP 429:
 * a {@code Retry-After} header (ceiling seconds of the decision's
 * retry-after) and an RFC 7807 problem+json body naming the policy and fired
 * level — never raw limit key material. Throttle wait timeouts and queue
 * overflows carry distinct problem types so clients can tell "slow down" from
 * "queue full, fail fast".
 *
 * <p>Replace the mapping by declaring an own bean of this type (subclass and
 * override, or provide a different {@code @ControllerAdvice} and register it
 * as this type); the auto-configured default backs off.
 */
@RestControllerAdvice
public class RateLimitExceptionHandler {

    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<ProblemDetail> handleRateLimitExceeded(RateLimitExceededException exception) {
        RateLimitProblem problem = RateLimitProblem.of(exception.decision());
        ProblemDetail body =
                ProblemDetail.forStatusAndDetail(HttpStatus.TOO_MANY_REQUESTS, problem.detail());
        body.setType(problem.type());
        body.setTitle(problem.title());
        ResponseEntity.BodyBuilder response = ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .contentType(MediaType.APPLICATION_PROBLEM_JSON);
        if (problem.retryAfterSeconds().isPresent()) {
            response.header(HttpHeaders.RETRY_AFTER, Long.toString(problem.retryAfterSeconds().orElseThrow()));
        }
        return response.body(body);
    }
}
