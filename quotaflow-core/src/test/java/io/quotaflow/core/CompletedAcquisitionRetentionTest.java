package io.quotaflow.core;

import io.quotaflow.core.store.LocalRateLimitStore;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CompletedAcquisitionRetentionTest {
    private record Retained(CompletableFuture<Decision> result, WeakReference<LocalRateLimitStore> store) { }
    private static Retained completed(String mode) throws Exception {
        var store = new LocalRateLimitStore();
        var executor = new AcquisitionLifecycleTest.ManualExecutor();
        var builder = DefaultQuotaFlow.builder(AcquisitionLifecycleTest.POLICIES, store);
        if (mode.equals("cancel")) builder.asyncExecutor(executor);
        if (mode.equals("fail")) builder.defaultResolver((context, policy) -> { throw new IllegalArgumentException("broken resolver"); });
        var flow = builder.build();
        var result = flow.tryAcquireAsync("p", RateLimitContext.empty(), 1).toCompletableFuture();
        if (mode.equals("cancel")) { assertTrue(result.cancel(true)); executor.runNext(); }
        else if (mode.equals("fail")) assertThrows(java.util.concurrent.ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
        else assertTrue(result.get(2, TimeUnit.SECONDS).isAllowed());
        return new Retained(result, new WeakReference<>(store));
    }
    @Test void retainingACompletedResultDoesNotRetainTheLimiterAndStore() throws Exception {
        for (String mode : java.util.List.of("allow", "cancel", "fail")) {
            var retained = completed(mode);
            for (int i = 0; i < 50 && retained.store().get() != null; i++) { System.gc(); Thread.sleep(10); }
            assertNull(retained.store().get(), "a completed result must release acquisition ownership: " + mode);
            assertTrue(retained.result().isDone());
            Reference.reachabilityFence(retained.result());
        }
    }
}
