package io.quotaflow.tck;

import io.quotaflow.core.Algorithm;
import org.junit.jupiter.api.Test;

class TokenBucketHealthyVirtualTest extends VirtualThreadScenario {
    @Test void recordsColdAndWarmPaths() throws Exception { verifyScenario(Algorithm.TOKEN_BUCKET, "healthy"); }
}
