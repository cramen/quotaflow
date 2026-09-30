package io.quotaflow.core;

import io.quotaflow.core.execution.*;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ExecutionBoundsTest {
    @Test void badAdmissionPredicateAndThrowingWorkReleaseCapacity() throws Exception {
        try (var execution=new BoundedExecution(1,1)) {
            assertThrows(ExecutionException.class,()->execution.submit(()->1,()->{throw new IllegalStateException();}).toCompletableFuture().get(2,TimeUnit.SECONDS));
            assertThrows(ExecutionException.class,()->execution.submit(()->{throw new IllegalStateException();},()->true).toCompletableFuture().get(2,TimeUnit.SECONDS));
            assertThrows(ExecutionException.class,()->execution.submitStage(()->null,()->true).toCompletableFuture().get(2,TimeUnit.SECONDS));
            assertThrows(CancellationException.class,()->execution.submit(()->1,()->false).toCompletableFuture().join());
            assertEquals(1,execution.submit(()->1,()->true).toCompletableFuture().get(2,TimeUnit.SECONDS));
        }
    }
    @Test void validatesBoundsAndRejectsClosedExecution() throws Exception {
        assertThrows(IllegalArgumentException.class,()->new BoundedExecution(0,1));
        assertThrows(IllegalArgumentException.class,()->new BoundedExecution(1,0));
        var execution=new BoundedExecution(1,1);execution.close();
        assertThrows(ExecutionException.class,()->execution.submit(()->1,()->true).toCompletableFuture().get());
        assertThrows(IllegalStateException.class,()->BoundedExecution.shared().close());
        var builder=DefaultQuotaFlow.builder(AcquisitionLifecycleTest.POLICIES,new LocalRateLimitStore());
        assertThrows(IllegalArgumentException.class,()->builder.operationTimeout(Duration.ZERO));
        assertThrows(IllegalArgumentException.class,()->builder.operationTimeout(Duration.ofSeconds(-1)));
        assertThrows(NullPointerException.class,()->builder.execution(null));
        assertThrows(NullPointerException.class,()->builder.addWaitListener(null));
    }
}
