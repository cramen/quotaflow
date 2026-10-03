package io.quotaflow.tck;

import io.quotaflow.core.Algorithm;
import org.junit.jupiter.api.Test;

class GcraHealthyVirtualTest extends VirtualThreadScenario {
    @Test void recordsColdAndWarmPaths() throws Exception { verifyScenario(Algorithm.GCRA, "healthy"); }
}
