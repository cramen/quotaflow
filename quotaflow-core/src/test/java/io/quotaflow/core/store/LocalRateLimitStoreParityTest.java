package io.quotaflow.core.store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quotaflow.core.Algorithm;
import io.quotaflow.core.Limit;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Token bucket and GCRA are parameterized to be exactly equivalent (emission
 * interval T = refillPeriod / refillAmount, GCRA tolerance tau = capacity * T):
 * both admit a burst of exactly {@code capacity} from a full bucket and refill
 * at the same rate. For identical request sequences they must agree on every
 * verdict, remaining capacity and retry-after.
 */
class LocalRateLimitStoreParityTest {

    private static final long MILLI = 1_000_000L;

    /** Capacity 7, one token per millisecond: T = 1ms exactly, keeping arithmetic exact. */
    private static final Limit LIMIT = new Limit(7, 1, Duration.ofMillis(1));

    private record Step(long advanceMillis, long weight) {
    }

    @Test
    void tokenBucketAndGcraAgreeOnIdenticalSequences() {
        List<Step> script = List.of(
                new Step(0, 3),   // burst 3 of 7
                new Step(0, 4),   // drain
                new Step(0, 1),   // reject, empty
                new Step(3, 5),   // reject, only 3 refilled
                new Step(0, 3),   // drain refill
                new Step(10, 7),  // full again after cap
                new Step(0, 8),   // weight above capacity, never admitted
                new Step(1, 2),   // reject, 1 available
                new Step(0, 1));  // admit last token

        List<StoreResult> tokenBucket = run(Algorithm.TOKEN_BUCKET, script);
        List<StoreResult> gcra = run(Algorithm.GCRA, script);
        assertEquals(tokenBucket, gcra);
    }

    @Test
    void parityHoldsAcrossRandomizedSequences() {
        long seed = 0x5EED;
        List<Step> script = new ArrayList<>();
        long state = seed;
        for (int i = 0; i < 500; i++) {
            state = state * 6364136223846793005L + 1442695040888963407L;
            long advance = (state >>> 33) % 4;
            state = state * 6364136223846793005L + 1442695040888963407L;
            long weight = 1 + (state >>> 33) % 9;
            script.add(new Step(advance, weight));
        }
        assertEquals(run(Algorithm.TOKEN_BUCKET, script), run(Algorithm.GCRA, script));
    }

    private List<StoreResult> run(Algorithm algorithm, List<Step> script) {
        AtomicLong nanos = new AtomicLong();
        LocalRateLimitStore store = new LocalRateLimitStore(nanos::get);
        List<StoreResult> results = new ArrayList<>();
        for (Step step : script) {
            nanos.addAndGet(step.advanceMillis() * MILLI);
            results.add(store.tryAcquire("k", LIMIT, algorithm, step.weight()));
        }
        return results;
    }
}
