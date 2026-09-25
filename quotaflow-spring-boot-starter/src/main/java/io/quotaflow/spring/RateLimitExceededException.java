package io.quotaflow.spring;

import io.quotaflow.core.Decision;
import io.quotaflow.core.ThrottleRejection;
import java.util.Objects;

/**
 * A rejected rate limit decision raised at the Spring integration boundary.
 * Inside the engine decisions stay data; this exception exists so the web
 * layer can map rejections to HTTP semantics. The message names the policy and
 * the fired level only — never raw limit key material.
 */
public class RateLimitExceededException extends RuntimeException {

    private final transient Decision decision;

    public RateLimitExceededException(Decision decision) {
        super(message(Objects.requireNonNull(decision, "decision")));
        this.decision = decision;
    }

    /** The rejection decision, carrying the fired level, remaining quota and retry-after. */
    public Decision decision() {
        return decision;
    }

    private static String message(Decision decision) {
        StringBuilder message = new StringBuilder("rate limit rejected: policy '")
                .append(decision.policyId())
                .append("', fired level ")
                .append(decision.scope().wireName());
        if (decision.throttleRejection().isPresent()) {
            ThrottleRejection reason = decision.throttleRejection().orElseThrow();
            message.append(reason == ThrottleRejection.WAIT_TIMEOUT
                    ? " (wait timeout expired)"
                    : " (wait queue full)");
        }
        return message.toString();
    }
}
