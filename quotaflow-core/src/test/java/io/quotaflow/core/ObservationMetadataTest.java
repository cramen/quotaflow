package io.quotaflow.core;

import io.quotaflow.core.observation.*;
import io.quotaflow.core.store.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ObservationMetadataTest {
    @Test void invalidBudgetMetadataCannotCrossTheStoreBoundary() {
        assertThrows(IllegalArgumentException.class, () -> new StoreBudget(-1, 0, false));
        assertThrows(IllegalArgumentException.class, () -> new StoreBudget(1, -1, false));
        assertThrows(IllegalArgumentException.class, () -> new StoreBudget(1, 2, false));
        assertThrows(IllegalArgumentException.class, () -> new LevelBudget(-1, new StoreBudget(1, 0, false)));
        assertThrows(NullPointerException.class, () -> new LevelBudget(0, null));
        var sample = new LevelBudget(0, new StoreBudget(1, 0, false));
        assertThrows(IllegalArgumentException.class, () -> ChainResult.acquired(0, 0).withBudgets(List.of(sample, sample)));
        var pending = new RecoveryPending(new QuotaDomain("default", "p"), 0, new CompletableFuture<>());
        assertThrows(IllegalArgumentException.class, () -> ChainResult.pending(0, pending).withBudgets(List.of(sample)));
        assertThrows(IllegalArgumentException.class, () -> new StoreResult(false, 0, 0, pending, sample.budget()));
        var degraded = ChainResult.acquired(0, 0).withBudgets(List.of(sample)).asDegraded();
        assertEquals(new StoreBudget(1, 0, true), degraded.singleResult().budget());
        assertSame(degraded.singleResult().budget(), degraded.singleResult().budget().asDegraded());
        assertNull(StoreResult.acquired(1).budget());
        assertNull(ChainResult.acquired(0, 1).singleResult().budget());
    }

    @Test void observationsDefensivelyCopyTheirCollectionsAndRejectInvalidVersions() {
        var samples = new ArrayList<BudgetSample>();
        samples.add(new BudgetSample("p", "global", Algorithm.GCRA, new StoreBudget(0, 0, true)));
        var observation = new BudgetObservation(0, 0, "p", 0, samples);
        samples.clear(); assertEquals(1, observation.samples().size());
        assertThrows(UnsupportedOperationException.class, () -> observation.samples().clear());
        for (long[] values : new long[][]{{-1,0,0},{0,-1,0},{0,0,-1}}) {
            assertThrows(IllegalArgumentException.class, () -> new BudgetObservation(values[0], values[1], "p", values[2], List.of()));
        }
        assertThrows(IllegalArgumentException.class, () -> new ObservationConfiguration(-1, Map.of(), Set.of()));
        var roots = new HashMap<>(Map.of("p", "p")); var throttle = new HashSet<>(Set.of("p"));
        var configuration = new ObservationConfiguration(1, roots, throttle);
        roots.clear(); throttle.clear();
        assertEquals(Map.of("p", "p"), configuration.policyRoots()); assertEquals(Set.of("p"), configuration.throttlePolicies());
    }
}
