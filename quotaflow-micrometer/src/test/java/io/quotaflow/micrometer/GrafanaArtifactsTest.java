package io.quotaflow.micrometer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class GrafanaArtifactsTest {

    @Test
    void dashboardJsonParsesAndCoversTheMetricContract() throws IOException {
        JsonNode dashboard;
        try (InputStream in = getClass().getResourceAsStream("/grafana/quotaflow-overview.json")) {
            assertNotNull(in, "dashboard resource is packaged");
            dashboard = new ObjectMapper().readTree(in);
        }
        assertTrue(dashboard.hasNonNull("title"));
        JsonNode panels = dashboard.get("panels");
        assertTrue(panels.isArray() && !panels.isEmpty(), "dashboard has panels");

        String json = dashboard.toString();
        assertTrue(json.contains("quotaflow_decisions_total"));
        assertTrue(json.contains("quotaflow_utilization"));
        assertTrue(json.contains("quotaflow_degraded"));
        assertTrue(json.contains("quotaflow_fallback_decisions_total"));
    }

    @Test
    void alertingRulesCoverUtilizationDegradationAndRejectGrowth() throws IOException {
        String alerts;
        try (InputStream in = getClass().getResourceAsStream("/grafana/alerts.yaml")) {
            assertNotNull(in, "alerts resource is packaged");
            alerts = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertTrue(alerts.contains("QuotaflowHighUtilization"));
        assertTrue(alerts.contains("QuotaflowDegraded"));
        assertTrue(alerts.contains("QuotaflowRejectGrowth"));
        assertTrue(alerts.contains("> 0.8"));
        assertFalse(alerts.isBlank());
    }
}
