# Acquisition lifecycle validation

Validation date: 2026-09-30. The recovery prerequisite is committed as `5e2c853`. This record covers the subsequent acquisition/cache lifecycle implementation, not complete library release certification.

## Correctness

The final `check dependencyAudit` run passes with 539 tests and no failures, errors or skips. Core branch coverage is 712/774 (91.99%). No coverage exclusions or quality thresholds were relaxed.

The complete core PIT run killed 756/892 mutations. After the final diagnostic and result-lifetime fixes, PIT ran the complete core test set against the two changed class families, `PolicyEngine*` and `DefaultQuotaFlow*`. Replacing those families' previous results yields **763/897 (85.06%)** for the final core. SHA-256 checks confirm every other compiled core class is unchanged from the full run. Both the full and focused runs passed the existing mutation threshold; this is core evidence, not a fallback or Lua mutation result.

Generated reports are retained locally under `build/reports/lifecycle-change/`: `pit-full/`, `pit-final-families/`, `mutation-evidence.json`, compiled-class hashes, `jacoco/`, module test XML and Gradle logs. The PIT initialization scripts are retained with the reports; they change test process parallelism and the documented follow-up scope, not mutators or thresholds. Generated build reports are ignored artifacts and need collection by the release pipeline.

## Regressions and resource checks

The new tests first reproduced API-entry deadline reset, late initial/retry allow, notification after cancellation, stale tariff resurrection, and duplicate concurrent lookups. Final tests cover shared lookup cancellation, completion-based TTL, timeout tombstones, repeated invalidation with interruption-resistant delegates, actual reload hooks, synchronous/async parity, sequential-dispatch cancellation, finite work admission and removable timers.

Recovery tests require a fresh complete snapshot on retry, preserve the original deadline and distinguish readiness from impossible quota. Parent-blocked queues retain the leaf entry identity and final fired-level identity. Queue overflow emits no entry event. Terminal result futures release acquisition ownership: retaining allowed, cancelled or failed futures no longer retains the limiter and store.

Stress checks cover 200 Java waiters, policy removal/re-addition, 100 coroutine waiters and 100 reactive subscriptions. Blocking resolver/store invocation preserves single-thread caller progress. Deadline and resource tests also pass with `-XX:ActiveProcessorCount=1`; the corresponding init script and log are retained. These are correctness/resource scenarios, not throughput promises.

## Performance and release scope

Final JMH diagnostic tasks pass for token bucket and GCRA on local, fallback/facade and Redis paths. Complete raw results, iteration variability, JVM/workload metadata and verifier output are retained under `build/reports/lifecycle-change/benchmarks/token-bucket/` and `gcra/`. The separately retained `token-before-retention/` run predates the final lifetime fix and is superseded. Production/JMH/fixture source hashes were checked unchanged throughout the final runs. No reviewed matching baseline or designated certification runner was supplied, so regression comparison remains **NOT ASSESSED**. Diagnostic task success cannot certify a release, and there are no universal absolute latency/throughput thresholds.

The companion observability change still owns Micrometer mapping of the new wait-entry SPI. Runtime/compatibility matrices, dedicated soak/native checks, comparative performance certification and supply-chain gates remain separate release work. See [acquisition lifecycle](acquisition-lifecycle.md) for the operational contract and migration guidance.
