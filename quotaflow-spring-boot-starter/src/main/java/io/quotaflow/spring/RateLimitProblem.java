package io.quotaflow.spring;

import io.quotaflow.core.Decision;
import io.quotaflow.core.ThrottleRejection;
import java.net.URI;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Maps a rejected {@link Decision} onto the RFC 7807 problem shape and the
 * {@code Retry-After} header shared by the servlet and reactive default
 * handlers. The problem {@code type} distinguishes plain quota rejections from
 * throttle wait timeouts and queue overflows; the detail names the policy and
 * the fired level — never raw limit key material.
 */
record RateLimitProblem(URI type, String title, int status, String detail, OptionalLong retryAfterSeconds) {

    static final URI TYPE_REJECTED = URI.create("urn:quotaflow:rate-limit-exceeded");
    static final URI TYPE_WAIT_TIMEOUT = URI.create("urn:quotaflow:rate-limit-wait-timeout");
    static final URI TYPE_QUEUE_OVERFLOW = URI.create("urn:quotaflow:rate-limit-queue-overflow");

    static RateLimitProblem of(Decision decision) {
        URI type = TYPE_REJECTED;
        String title = "Rate limit exceeded";
        Optional<ThrottleRejection> reason = decision.throttleRejection();
        if (reason.isPresent() && reason.orElseThrow() == ThrottleRejection.WAIT_TIMEOUT) {
            type = TYPE_WAIT_TIMEOUT;
            title = "Rate limit wait timeout";
        } else if (reason.isPresent() && reason.orElseThrow() == ThrottleRejection.QUEUE_OVERFLOW) {
            type = TYPE_QUEUE_OVERFLOW;
            title = "Rate limit queue overflow";
        }
        StringBuilder detail = new StringBuilder("Rate limit rejected for policy '")
                .append(decision.policyId())
                .append("' (fired level: ")
                .append(decision.scope().wireName())
                .append(')');
        if (reason.isPresent() && reason.orElseThrow() == ThrottleRejection.WAIT_TIMEOUT) {
            detail.append("; the wait timed out after ")
                    .append(decision.waitDuration().toMillis())
                    .append(" ms");
        } else if (reason.isPresent()) {
            detail.append("; the wait queue is full");
        }
        detail.append('.');
        return new RateLimitProblem(type, title, 429, detail.toString(), retryAfterSeconds(decision));
    }

    /** Ceiling of the decision's retry-after in seconds, at least one second when scheduled. */
    private static OptionalLong retryAfterSeconds(Decision decision) {
        if (decision.retryAfter().isEmpty()) {
            return OptionalLong.empty();
        }
        long nanos = decision.retryAfter().orElseThrow().toNanos();
        return OptionalLong.of(Math.max(1, (nanos + 999_999_999L) / 1_000_000_000L));
    }

    /** Minimal RFC 7807 JSON serialization with full string escaping. */
    String toJson() {
        StringBuilder json = new StringBuilder(128);
        json.append('{');
        appendField(json, "type", type.toString());
        json.append(',');
        appendField(json, "title", title);
        json.append(',');
        json.append("\"status\":").append(status);
        json.append(',');
        appendField(json, "detail", detail);
        json.append('}');
        return json.toString();
    }

    private static void appendField(StringBuilder json, String name, String value) {
        json.append('"').append(name).append("\":\"");
        appendEscaped(json, value);
        json.append('"');
    }

    private static void appendEscaped(StringBuilder json, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (c < 0x20) {
                        json.append(String.format("\\u%04x", (int) c));
                    } else {
                        json.append(c);
                    }
                }
            }
        }
    }
}
