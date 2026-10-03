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
fallback wrapper. The fallback module now supplies a separate actual wrapper/facade benchmark.
Release certification uses the complete repeated local comparison and immutable evidence bundle described below.

The verifier contract tests run with `python3 scripts/test_benchmark_verification.py`
and synthetic evidence; they do not run JMH or require Docker.

Benchmarks run only on the authorized current local machine. GitHub CI, nightly
and release workflows do not execute JMH. A Gradle graph guard rejects JMH when
`GITHUB_ACTIONS=true`, including indirect scheduling through `benchmarkGate`.
Synthetic validator tests and validation of supplied evidence are still allowed.

The initial host capture is `verification/local-benchmark-host.json`. It records
hardware, OS, JVM, Docker and power/thermal observations; it is not a complete
measurement profile or certified baseline. Record the exact server image/config,
workload hashes, JMH settings and conditions for every comparison. Run candidate
and baseline sequentially without overlapping verification jobs, and retain all
repetitions and variability. Moving hosts requires a new comparable baseline.

The historical `quotaflow-store-redis/benchmark-baseline.json` remains provenance
only: it lacks a matching profile and predates the shared-parent workload.
Release verification must import a local bundle with a pinned digest and matching
commit/version/artifact/profile/baseline identities. Until that validator is wired,
the release workflow deliberately refuses promotion. Removing cloud benchmark
execution does not waive performance evidence. The obsolete
`absoluteBenchmarkGates` and `updateBenchmarkBaseline` options remain rejected.

## Explicit comparison

Create a JSON profile describing the actual measurement environment. Every field
below must be a nonempty string with concrete values, not these placeholders:

```json
{
  "machine": "authorized local host and allocation identity",
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
when supplying a baseline; core, fallback and Redis have different metrics.

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

Both algorithms can be measured separately using `-PbenchmarkAlgorithm=TOKEN_BUCKET`
or `-PbenchmarkAlgorithm=GCRA`. Run `:quotaflow-fallback:benchmarkGate` for the actual
degraded wrapper/facade path; preserve each algorithm's JMH JSON separately.

## Complete local comparison

The independent `verification/benchmark` workload uses identical source for both
library versions and measures the full facade in local, distributed shared-parent
and degraded fallback modes under both algorithms. Compile each version against
its own staged repository with `jmhJar`; this prepares executables without
starting measurement. Keep the two resulting JARs separate.

The baseline for the initial release-quality comparison is commit `9869a94`, the
last completed runtime change before this gate work. It already contains the
hierarchy, numeric, conservative recovery, lifecycle and observability changes.
Earlier lifecycle diagnostic reports are context only. A new baseline review
records its exact source/artifact hashes, workload hash, execution profile,
reviewer, timestamp and reason before measurements begin. Baseline replacement
is a separate reviewed operation; release validation cannot update it.

On the authorized macOS host, compile the read-only condition probe:

```sh
swiftc verification/benchmark/HostConditions.swift -o /path/to/host-conditions
python3 scripts/run_local_benchmarks.py \
  --profile /path/to/profile.json --baseline-review /path/to/baseline-review.json \
  --candidate /path/to/candidate.json --baseline-jar /path/to/baseline-jmh.jar \
  --candidate-jar /path/to/candidate-jmh.jar --java /path/to/jdk21/bin/java \
  --host-probe /path/to/host-conditions --redis-url redis://localhost:6379 \
  --output build/reports/local-comparison
```

The runner alternates three independent complete runs of each version, records
all raw JMH results and logs, and samples power/load/thermal information before
and after each run. A thermal-state value of `-1` explicitly means the host API
does not report it; it is never presented as a nominal temperature reading.
Such unavailable telemetry must be recorded in the reviewed profile. Reported
thermal throttling, changed power mode or excessive background load invalidates
the controlled series. No benchmark or profiler may run concurrently with other
verification workloads. Archive failed/inconclusive series too.

Validate the resulting bundle with `scripts/verify_performance_bundle.py` as
described in [verification](verification.md). Only repeated comparable evidence
can establish a >10% regression. Profile only a demonstrated regression before
choosing an optimization; rerun correctness and every compared metric afterward.

Allocation and contention diagnostics are collected separately, so profiler
overhead cannot alter the comparative gate:

```sh
python3 scripts/profile_local_candidate.py --profile /path/to/profile.json \
  --jar /path/to/candidate-jmh.jar --java /path/to/jdk21/bin/java \
  --host-probe /path/to/host-conditions --redis-url redis://localhost:6379 \
  --output build/reports/local-profile
```

This records GC allocation metrics and JFR for distributed/fallback paths under
both algorithms, plus aggregate Redis wire counters. Wire counters include setup
and control traffic; they are not presented as bytes per decision. JFR uses the
JDK's sampling/threshold configuration, so missing events do not prove zero
contention. Profiling reports always remain NOT ASSESSED for regression purposes.
