package io.quotaflow.micrometer;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.quotaflow.core.Decision;
import io.quotaflow.core.DecisionListener;
import java.util.Objects;

/**
 * Optional tracing bridge: opens one span per limiter decision with
 * attributes {@code policy}, {@code result} (allow|reject), {@code key-group}
 * and — on store rejections — {@code retry-after} in milliseconds. The span
 * is not made current; it records the decision event only.
 *
 * <p>This class sits behind a {@code compileOnly} dependency on
 * micrometer-tracing: it activates only when the application already runs a
 * tracer and adds this bridge explicitly, so its absence changes nothing and
 * pulls no transitive dependencies.
 */
public final class TracingDecisionListener implements DecisionListener {

    public static final String SPAN_NAME = "quotaflow.decision";
    public static final String ATTR_RETRY_AFTER = "retry-after";

    private final Tracer tracer;

    public TracingDecisionListener(Tracer tracer) {
        this.tracer = Objects.requireNonNull(tracer, "tracer");
    }

    @Override
    public void onDecision(Decision decision, String keyGroup) {
        Span span = tracer.nextSpan().name(SPAN_NAME).start();
        try {
            span.tag(QuotaFlowMetrics.TAG_POLICY, decision.policyId())
                    .tag(QuotaFlowMetrics.TAG_RESULT,
                            decision.isAllowed() ? QuotaFlowMetrics.RESULT_ALLOW : QuotaFlowMetrics.RESULT_REJECT)
                    .tag(QuotaFlowMetrics.TAG_KEY_GROUP, keyGroup);
            decision.retryAfter()
                    .ifPresent(retryAfter ->
                            span.tag(ATTR_RETRY_AFTER, String.valueOf(retryAfter.toMillis())));
        } finally {
            span.end();
        }
    }
}
