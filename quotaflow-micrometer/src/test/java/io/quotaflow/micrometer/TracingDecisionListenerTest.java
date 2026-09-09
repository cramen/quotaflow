package io.quotaflow.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.micrometer.tracing.test.simple.SimpleTracer;
import io.quotaflow.core.Decision;
import io.quotaflow.core.Scope;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class TracingDecisionListenerTest {

    @Test
    void everyDecisionProducesASpanWithTheContractAttributes() {
        SimpleTracer tracer = new SimpleTracer();
        TracingDecisionListener listener = new TracingDecisionListener(tracer);

        listener.onDecision(Decision.allowed("per-tenant", Scope.TENANT, 5), "tenant");
        listener.onDecision(
                Decision.rejected("per-tenant", Scope.TENANT, 0, Duration.ofMillis(250)), "tenant");

        var spans = new java.util.ArrayList<>(tracer.getSpans());
        assertEquals(2, spans.size());
        var allowed = spans.get(0);
        assertEquals(TracingDecisionListener.SPAN_NAME, allowed.getName());
        assertEquals("per-tenant", allowed.getTags().get("policy"));
        assertEquals("allow", allowed.getTags().get("result"));
        assertEquals("tenant", allowed.getTags().get("key-group"));
        assertFalse(allowed.getTags().containsKey(TracingDecisionListener.ATTR_RETRY_AFTER));

        var rejected = spans.get(1);
        assertEquals("reject", rejected.getTags().get("result"));
        assertEquals("250", rejected.getTags().get(TracingDecisionListener.ATTR_RETRY_AFTER));
    }
}
