# Benchmark evidence

Throughput and latency depend on the machine, JVM, server, network and workload.
There are no universal absolute speed gates. Correctness assertions inside the
workloads remain mandatory; making shared quotas independent to improve a score
is not a valid optimization.

## Diagnostic mode

```sh
./gradlew :quotaflow-core:benchmarkGate :quotaflow-store-redis:benchmarkGate
```

The tasks run JMH, require each expected metric exactly once, validate its mode,
unit and positive finite value, and write `build/reports/jmh/verification.json`
alongside raw `results.json`. Without an explicit baseline they report regression
`NOT ASSESSED`. A successful diagnostic task is not release performance
certification. The core workload measures the bare local store, not the complete
fallback wrapper. Actual fallback and repeated-run release certification remain
work in `enforce-release-quality-gates`.

The verifier contract tests run with `python3 scripts/test_benchmark_verification.py`
and synthetic evidence; they do not run JMH or require Docker.

CI/nightly shared runners collect diagnostic evidence. Their hardware is not
assumed comparable with each other or a developer's computer. The historical
`quotaflow-store-redis/benchmark-baseline.json` is retained for provenance and is
not automatically consumed: it lacks a profile and predates the corrected
shared-parent workload. Release verification uses `requireBenchmarkComparison` and therefore remains
blocked until controlled per-module profiles/baselines are provisioned by
`enforce-release-quality-gates`; shared-runner diagnostic success cannot publish
a release. The obsolete `absoluteBenchmarkGates` and
`updateBenchmarkBaseline` options fail with migration guidance.

## Explicit comparison

Create a JSON profile describing the actual measurement environment. Every field
below must be a nonempty string with concrete values, not these placeholders:

```json
{
  "machine": "dedicated host and allocation identity",
  "cpu": "model, architecture, available cores and CPU limits",
  "memory": "available bytes and memory limits",
  "os": "OS/kernel/container image and resource configuration",
  "jdk": "vendor, exact version and JVM flags",
  "server": "Redis/Valkey version, image digest and configuration; none for local",
  "network": "client/server placement and transport configuration",
  "workload": "versioned workload identity, hierarchy shape and concurrency"
}
```

Pass it with `-PbenchmarkProfileFile=/absolute/path/profile.json`. The profile is
an operator attestation, not automatic hardware discovery: capture it from the
benchmark environment, not merely the Gradle launcher host. Keep raw JSON,
environment metadata, commit/artifact identity and all runs together. Never put
credentials or server URLs containing credentials in profiles.

After reviewing measurements, retain a module's `verification.json` as its
baseline. Compare the same module on the same environment and workload:

```sh
./gradlew :quotaflow-store-redis:benchmarkGate \
  -PbenchmarkProfileFile=/absolute/path/profile.json \
  -PbenchmarkBaselineFile=/absolute/path/reviewed-baseline.json \
  -PrequireBenchmarkComparison
```

A comparison requires exact profile and JMH configuration equality (runtime,
parameters, threads, forks, warmups and measurement settings). Missing evidence,
invalid values, mismatches or throughput decline/latency increase greater than
10% fail. No task updates the baseline automatically. Use one module per command
when supplying a baseline; core and Redis have different metrics.

This check compares individual result sets. Release certification still needs
at least three complete independent runs per version, variability analysis and
review of any noisy/inconclusive result. Retain failures and rerun under controlled
conditions; do not select the fastest run, widen the budget or present a single
comparison as complete certification. Changed quota semantics require a new
reviewed workload baseline. Baseline promotion is separate from release runs.

For offline validation of an existing measurement, use
`-PbenchmarkResultsFile=/absolute/path/results.json` and exclude the corresponding
module's `jmh` task with `-x :quotaflow-store-redis:jmh`. This validates that report;
it does not create new measurements or prove they belong to a release candidate.

Soak reports retain latency distributions while enforcing correctness/resource
invariants. Absolute 1 ms / 0.01 ms assertions are removed; they cannot establish
machine-independent quality.
