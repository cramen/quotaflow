# Acquisition deadlines, cancellation and resolver execution

Positive `acquire` and `acquireAsync` wait budgets start at public API entry. Executor admission, key and tariff resolution, store work, queue residence and retries consume the same monotonic budget. A late store success cannot become an allow, even if JVM scheduling delayed the deadline callback. JVM pauses and callback scheduling can delay delivery; this is not a hard real-time guarantee.

A zero wait timeout, including `tryAcquire`, performs one nonwaiting evaluation and never enters the policy queue. Its default operational timeout is one second. Ordinary nonwaiting rejections retain their quota rejection; operational expiry returns a schedule-less rejection. Each positive evaluation is also bounded by the smaller of the operational timeout and the remaining caller budget. The caller's positive deadline produces `WAIT_TIMEOUT`; an earlier operational failure does not fabricate a refill schedule.

Before any evaluated rejection exists, a caller timeout identifies the requested leaf and scope, zero remaining, the bounded `unresolvable` group and no retry schedule. After a completed rejection, timeout preserves that rejection's fired-level identity and refill schedule. The terminal `waitDuration` measures elapsed time since API entry for queued or timed-out positive calls. Successful nonqueued results and ordinary nonqueued rejections report zero. This field is not literal queue residence.

## Bounded execution

The default shared compatibility service has four daemon workers and at most 1024 queued tasks. Incomplete asynchronous delegate operations also retain finite admission until physical completion. A blocked legacy resolver or store invocation cannot run on an asynchronous caller's event loop. A positive throttle call refused by dispatch admission receives `QUEUE_OVERFLOW`, without a queue-entry event. Nonwaiting calls receive a schedule-less rejection.

Waiting for quota uses continuation entries and removable timers, not one parked library worker per waiter. Policy queues retain strict priority and FIFO within a priority, with a default bound of 1000. Low-priority callers can starve until their own deadlines. Empty queues are removed; an old cleanup cannot delete a replacement queue.

```java
BoundedExecution execution = new BoundedExecution(4, 1024);
DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, store)
        .execution(execution)
        .operationTimeout(Duration.ofSeconds(1))
        .maxWaitersPerPolicy(1000)
        .build();
```

Import `io.quotaflow.core.execution.BoundedExecution`. Custom execution is caller-owned: close it after stopping acquisitions. Closing allows accepted work to drain; arbitrary third-party code can ignore interruption and retain a worker until it returns. No replacement workers beyond the configured bound are created. The process-wide shared service must not be closed.

The existing `asyncExecutor` option controls asynchronous continuation admission. Its `execute` method must return promptly; inline execution is supported because SPI invocation still crosses bounded compatibility execution. Pass request identity explicitly through `RateLimitContext`; dispatch boundaries do not automatically copy caller thread-local state. Synchronous acquisition uses the same lifecycle and waits only on the calling thread. Interrupting that wait preserves the interrupt flag and requests a timeout outcome.

## Cancellation and events

Completion, timeout, failure and cancellation compete for one terminal outcome. Cancelling the returned future before finalization releases queue/timer resources and suppresses its terminal event. Completed, cancelled and failed result futures detach from the acquisition owner, so retaining a result does not retain the limiter and store. Cancellation after finalization returns false. A command already dispatched can still debit quota; no automatic refund or distributed rollback is promised. A cancelled sequential hierarchy dispatches no later level.

`DecisionListener` remains terminal-only. `WaitListener.onQueued(queuePolicyId, keyGroup)` reports one successful queue admission, including recovery waiting. Its policy is the requested leaf that owns the queue; its bounded group is the first blocking level's group. Retries do not change that entry identity. Terminal decisions retain the final fired level. Queue or dispatch overflow produces no entry event; cancellation after enqueue retains the historical entry and produces no invented terminal decision.

Listeners must be fast and nonblocking. Callback failures are isolated. Listener and public-future callback delivery uses two shared daemon workers, separate from compatibility dispatch and deadline scheduler threads; a synchronous timeout can return its finalized decision before telemetry delivery completes. Arbitrary slow user callbacks can delay their own delivery without authorizing late admission. The companion observability work owns Micrometer mapping of the new entry SPI.

Recovery readiness is a request to re-evaluate the complete chain, never permission to admit. Positive throttle calls wait through the same queue/deadline state. Cancelling one readiness view does not cancel shared recovery. Missing keys, unresolved limits and impossible weights still reject immediately without a synthetic retry schedule.

## Resolver cache

Existing `LimitResolver` implementations remain supported. `AsyncLimitResolver` adds `resolveAsync` for completion-based providers; implementations must return promptly. The caching wrapper supports both forms:

```java
CachingLimitResolver cached = CachingLimitResolver.wrap(
        resolver, Duration.ofSeconds(60), Duration.ofSeconds(1), execution);
```

Cache TTL starts at successful completion. Explicit unresolved results use the normal TTL; exceptions, execution saturation and lookup expiry do not become successful tariff entries. A lookup timeout includes execution admission. A timed-out physical delegate retains a tombstone until it exits, so repeated calls cannot launch duplicate blocked work for that key/generation. Late results are discarded.

Each key has one in-flight lookup per generation. Joiners receive detached views: one cancellation cannot cancel other callers. `clear()` publishes a fresh generation; an old lookup cannot insert into it. Register `cached::clear` with `ConfigReloader.Builder.onApplied`; calls admitted after successful reload completion use the fresh generation. Repeated invalidation cannot exceed execution admission bounds even when old delegates remain stuck.

Coordinated dynamic quotas retain `VersionedLimitResolver` and complete immutable snapshots instead of independent per-entry TTL values. Each evaluation captures one snapshot under bounded execution. Readiness retries capture a fresh view within the original caller budget. Provider revision proof is preserved; see [conservative recovery](conservative-recovery.md). Custom stores must follow the [canonical identity migration](canonical-quota-state.md); facade/resolver source compatibility does not restore the old store SPI.

The Kotlin builder exposes `execution`, `operationTimeout` and `addWaitListener` with the same semantics. Kotlin uses cancellable suspension over the same Java future. Cancellation before Java finalization suppresses its terminal event. Kotlin prompt cancellation can still prevent coroutine delivery after Java has finalized; that already-winning Java event remains valid. Neither case implies a refund of dispatched quota.
