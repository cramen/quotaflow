# Conservative recovery validation

Validation date: 2026-09-30. These results cover the coordinated recovery implementation and versioned dynamic-limit snapshots. They do not certify the entire library for release.

## Correctness evidence

The repository `check dependencyAudit :quotaflow-core:jacocoTestReport` checks pass. The ordinary test reports contain 510 tests with no failures, errors or skips. Core branch coverage is 586/616 (95.13%). Core PIT kills 628/729 mutations (86.15%); this is a core mutation result, not a Lua or fallback mutation result.

The scenarios include real Redis and Valkey with token bucket and GCRA, Redis Cluster, separate-JVM ownership conflicts and crash/replacement, lost-response non-replay, missing controllers, stale sessions, configuration ABA, bounded tracking, shutdown races and snapshot revision fencing. A controlled-clock whole-trace oracle checks staggered recovery without extra credit and the 1.2x recovery acceptance bound. A separate selective-partition fixture records the documented limitation after healthy pooled consumption resumes.

The dynamic-limit regression rejects an old 10/10 seed after a new 2/1 snapshot, including attempts to attach current controller counters to obsolete provider values. Resolver revision high-water marks survive static-policy transitions and planned cohort replacement.

## Performance evidence

Diagnostic JMH runs pass for both algorithms in core, fallback and Redis. The benchmark verifier contract tests pass. Results, verifier output and source hashes are retained locally under `build/reports/recovery-change/benchmarks/`.

No matching reviewed workload/hardware baseline was supplied. Regression comparison is **NOT ASSESSED**. Absolute latency and throughput values are machine-dependent diagnostics, not release thresholds or portable performance promises.

## Reports and remaining release work

Local test XML, coverage, PIT and focused-run logs are retained under `build/reports/recovery-change/validation/`. Build reports are ignored generated artifacts and should be collected by the release pipeline; this document does not substitute for those artifacts.

The acquisition/reload lifecycle change still owns throttle queue, deadline and cancellation integration with recovery readiness. Until that consumer exists, recovery-pending decisions produce schedule-less rejection. Long-running soak and native-image certification were not run as part of this local validation; their dedicated release/CI checks remain necessary. Dependency boundary auditing is not a vulnerability certification.

See [conservative recovery](conservative-recovery.md) for provisioning, snapshot migration, ownership continuity and the exact guarantee boundaries.
