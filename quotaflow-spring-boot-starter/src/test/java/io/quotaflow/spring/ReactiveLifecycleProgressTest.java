package io.quotaflow.spring;

import io.quotaflow.config.CachingLimitResolver;
import io.quotaflow.core.*;
import io.quotaflow.core.store.LocalRateLimitStore;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import static org.junit.jupiter.api.Assertions.*;

class ReactiveLifecycleProgressTest {
    @Test void blockingCacheMissDoesNotBlockSingleReactiveScheduler() throws Exception {
        var scheduler=Schedulers.newSingle("lifecycle-progress");var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        var cache=CachingLimitResolver.wrap((ref,group)->{
            entered.countDown();try{release.await();}catch(InterruptedException e){Thread.currentThread().interrupt();}
            return Optional.of(new Limit(10,1,Duration.ofSeconds(1)));
        });
        var policies=PolicySet.compile(List.of(RateLimitPolicy.builder("p").scope(Scope.GLOBAL).limitRef("plan").build()));
        var flow=DefaultQuotaFlow.builder(policies,new LocalRateLimitStore()).limitResolver(cache).build();
        try {
            var result=Mono.defer(()->Mono.fromCompletionStage(flow.tryAcquireAsync("p",RateLimitContext.empty(),1)))
                    .subscribeOn(scheduler).toFuture();
            assertTrue(entered.await(2,TimeUnit.SECONDS));
            assertEquals(1,Mono.fromCallable(()->1).subscribeOn(scheduler).toFuture().get(1,TimeUnit.SECONDS));
            release.countDown();assertTrue(result.get(2,TimeUnit.SECONDS).isAllowed());
        } finally {release.countDown();scheduler.dispose();}
    }
    @Test void manyThrottleSubscriptionsDoNotParkReactiveSchedulerAndCancellationFreesSlots() throws Exception {
        var scheduler=Schedulers.newSingle("lifecycle-waiters");
        var policy=RateLimitPolicy.builder("p").scope(Scope.GLOBAL).reaction(Reaction.THROTTLE).limit(new Limit(1,1,Duration.ofHours(1))).build();
        var flow=DefaultQuotaFlow.builder(PolicySet.compile(List.of(policy)),new LocalRateLimitStore()).build();
        var waiting=new ArrayList<CompletableFuture<Decision>>();
        try {
            assertTrue(flow.tryAcquire("p",RateLimitContext.empty()).isAllowed());
            for(int i=0;i<100;i++) waiting.add(Mono.defer(()->Mono.fromCompletionStage(flow.acquireAsync("p",RateLimitContext.empty(),1,Duration.ofMinutes(1))))
                    .subscribeOn(scheduler).toFuture());
            long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
            while(flow.waitQueueDepth("p")!=100 && System.nanoTime()<end) Thread.sleep(1);
            assertEquals(100,flow.waitQueueDepth("p"));
            assertEquals(1,Mono.fromCallable(()->1).subscribeOn(scheduler).toFuture().get(1,TimeUnit.SECONDS));
            waiting.forEach(f->f.cancel(true));assertEquals(0,flow.waitQueueDepth("p"));
        } finally {waiting.forEach(f->f.cancel(true));scheduler.dispose();}
    }
}
