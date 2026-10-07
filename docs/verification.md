# Verification workflow

Published Java and Kotlin APIs retain the Java 17 bytecode baseline. Select the
actual test-worker runtime explicitly:

```sh
./gradlew build dependencyAudit -PtestJdk=17
./gradlew build dependencyAudit -PtestJdk=21
./gradlew build dependencyAudit -PtestJdk=25
```

Install Java 17 for compilation and the selected runtime for execution. A
build-only agent checks the running JVM before test frameworks initialize and
writes its full runtime identity to each module's `build/reports/runtime/`.
An incorrectly selected worker terminates with failure. `test-result.json`
records scenario counts, failures and skips. The agent is not published or
included in a library runtime classpath.

Required tests must execute. Missing Docker or a skipped required scenario fails
ordinary verification. For an explicitly incomplete local investigation only,
`-PallowIncompleteVerification=true` permits skips and marks the report
`INCOMPLETE`. CI refuses this opt-out; incomplete reports cannot certify a release.

Run the functional failure controls with:

```sh
python3 scripts/test_build_verification.py
```

These controls start actual JVMs and isolated Gradle fixtures. They verify a
mislabeled worker, unresolved and disallowed dependencies, a skipped JUnit test,
and the local-versus-CI opt-out boundary. Python is a verification tool, not a
published runtime dependency.

For a local sequential matrix with copied reports, use a new output directory:

```sh
python3 scripts/run_runtime_matrix.py --output build/reports/my-runtime-matrix
```

Run topology suites sequentially in a workspace: the Cluster fixture uses fixed
host ports. Core branch coverage is required by `check`; `correctnessGate`
requires independent core and fallback PIT scores of at least 80%. Lua coverage
is established by server conformance and generated model tests, not Java PIT.

```sh
./gradlew correctnessGate -PtestJdk=17
./gradlew :quotaflow-tck:test --tests '*Generated*Model*Test' -PtestJdk=17
```

Generated numeric tests use an independent exact-arithmetic reference, including
fractional intervals, clock reversals and numeric boundaries. Generated races
search possible linearizations consistent with invocation/completion order.
Failed races are reduced by re-executing candidate histories against fresh
buckets, never by deleting already-recorded debits. Failures retain seeds and
original/minimized histories in `build/reports/generated/`.

On JDK 21/25, `check` also runs `:quotaflow-tck:virtualTest`. Each algorithm/path
combination creates 100,000 virtual callers with a bounded 256-caller admission
window. The healthy path uses Redis; the degraded path uses the actual fallback
store with a deterministic recovery-control fixture; the throttle path exercises
the local facade and its real wait queue. JFR files and outcome/queue counts are
retained under `build/reports/virtual-threads/`. Each scenario uses a fresh worker JVM, retains a 512-caller cold phase and a
separate 100,000-caller warmed phase, and records phase times and process identity.
Cold JVM loading/initialization events are retained and attributed from explicit
JVM reasons, loader stacks, JVM-owned lambda/call-site linkage stacks or verified interpreted resolution instructions. A linkage classification requires an uninterrupted JVM-owned frame prefix from the blocking site to an explicit `MethodHandleNatives.link…` or `MethodHandleNatives.findMethodHandleType` VM entry point, before any application frame, and the explicit native/VM pin reason. Application bootstrap code breaks this prefix and is not exempt.
Library-owned and unattributed cold pinning fail; every warmed pinning event
fails, including late class loading. Unknown events are never silently dropped.
This certifies measured warmed paths and does not promise zero cold-start JVM
initialization pinning.

The intentionally bad `negative-control.jfr` verifies JDK 21 monitor-pinning
detection. `blocking-initializer-control.jfr` proves that library-owned blocking
inside an initializer is still detected on both JDKs. Those recordings are
negative controls, not successful workload traces.

## External consumers

`verification/compatibility.json` records required targets, not completed
certification. The Spring Boot targets were checked against the
[official stable-version index](https://docs.spring.io/spring-boot/) on
2026-10-03. Native, topology and exact-candidate evidence remain mandatory before
claiming full support.

Stage a candidate in a local repository; this does not contact Central:

```sh
./gradlew publishAllPublicationsToVerificationRepository \
  -PverificationRepository=build/consumer-repository
```

With a disposable Redis endpoint and explicit JDK installation directories, run
both Maven and Gradle consumers. Each consumer verifies the actual Boot/JDK,
real Lettuce connectivity, servlet/WebFlux annotation interception, SpEL, HTTP
429, safe response bodies and delivered metric counts:

```sh
python3 scripts/run_consumer_matrix.py \
  --repository build/consumer-repository \
  --output build/reports/my-consumer-matrix \
  --redis-url redis://localhost:6379 \
  --java-home 17=/path/to/jdk17 \
  --java-home 21=/path/to/jdk21 \
  --java-home 25=/path/to/jdk25
```

A passing local matrix is not release approval. The controlled local performance
profile, reviewed matching baseline, native/soak evidence, reproducibility and
exact-candidate manifest must all pass the release verification contract.

## Reproducible inputs

The wrapper distribution, resolved module dependency graphs and downloaded
artifacts are pinned by the wrapper checksum, `gradle.lockfile` files and
`gradle/verification-metadata.xml`. Dependency updates must include reviewed
lock/checksum changes. Normal verification uses Gradle's strict checksum mode;
never generate new checksums as part of release acceptance.

Run two isolated clean builds (binary, sources and published POM), without
reusing compiled outputs or task caches:

```sh
python3 scripts/verify_reproducible_core.py \
  --output build/reports/core-reproducibility --version 0.1.0-SNAPSHOT
```

The command preserves both builds' artifacts, hashes and logs. It requires a
new output directory so a failed attempt cannot overwrite prior evidence.

## Imported local performance evidence

`scripts/verify_performance_bundle.py` reads evidence only; it never starts JMH.
A bundle contains its manifest, independently reviewed baseline identity,
complete raw JMH JSON/logs and condition snapshots before and after every run.
All references are relative paths with SHA-256 digests. The caller pins the
manifest and baseline-review digests independently of the bundle.

```sh
python3 scripts/verify_performance_bundle.py /path/to/local-bundle \
  --sha256 "$PERFORMANCE_MANIFEST_SHA256" \
  --candidate /path/to/candidate-identity.json \
  --baseline-review-sha256 "$BASELINE_REVIEW_SHA256"
python3 scripts/test_performance_bundle.py
```

Every complete run includes local, distributed hierarchy and actual fallback
facade paths, both algorithms, throughput and p99 latency. At least three runs
per version are required. The comparator retains every result and computes a
95% percentile bootstrap confidence interval for the regression of independent
run medians (10,000 deterministic resamples). It resamples whole runs, never
correlated individual JMH samples. If the entire interval exceeds 10%, it reports
REGRESSION; if the interval crosses that boundary, it reports INCONCLUSIVE. Only
an upper bound within 10% reports PASS. Diagnostic mode always reports NOT
ASSESSED. Do not discard unfavorable runs to manufacture a pass: retain the
complete original series and collect additional controlled measurements when
uncertainty remains. The report preserves every value and both series' spread.

## Native Spring fixtures

The independent fixture under `verification/native` is compiled against staged
published artifacts. Build and execute both stacks with GraalVM 25:

```sh
./gradlew -p verification/native nativeCompile \
  -PverificationRepository=/absolute/path/to/candidate-repository \
  -PcandidateVersion=0.1.0-SNAPSHOT -PwebStack=servlet
verification/native/build/native/nativeCompile/quotaflow-servlet -Dverification.stack=servlet
./gradlew -p verification/native nativeCompile \
  -PverificationRepository=/absolute/path/to/candidate-repository \
  -PcandidateVersion=0.1.0-SNAPSHOT -PwebStack=reactive
verification/native/build/native/nativeCompile/quotaflow-reactive -Dverification.stack=reactive
```

Each executable checks native execution, annotation proxies, argument SpEL,
real HTTP responses, opaque rejection bodies, startup with an unavailable
primary, recovery of the actual fallback owner and delivered metric counts.
The primary control replies are deterministic fixture data; distributed
accounting and real transport behavior are covered separately by native store
smoke and topology/conformance tests. A successful native compilation without
a successful executable run is incomplete evidence.

## Exact-candidate collection and release acceptance

Finish source changes and build the candidate before collecting its identity.
`candidate_identity.py` hashes every version-controlled or nonignored source
file (excluding agent/planning directories), records HEAD and hashes each
published binary. Uncommitted changes are included in the source digest; they
cannot be silently equated with HEAD. A release tag must match both the recorded
commit and source/artifact digests.

```sh
python3 scripts/candidate_identity.py --version "$VERSION" \
  --artifacts /path/to/each/published-binary.jar --output build/candidate.json
python3 scripts/run_evidence_stage.py --candidate build/candidate.json \
  --kind core-coverage --output build/evidence/core-coverage \
  --reset quotaflow-core/build/reports/jacoco \
  --report 'quotaflow-core/build/reports/jacoco/test/jacocoTestReport.xml' \
  -- ./gradlew :quotaflow-core:test :quotaflow-core:jacocoTestReport \
     :quotaflow-core:jacocoTestCoverageVerification --rerun-tasks -PtestJdk=17
```

Each stage captures its invocation, exit code, fresh report hashes, start/end
times and the same candidate identity. For a non-runtime stage, list its primary
JSON/XML report first with `--report`, followed by referenced logs and diagnostics. The collector refuses source changes
during execution and stale report files. Use a new output directory per attempt;
retain failures. Full runtime stages retain JUnit XML, worker JSON, generated
histories and cold/warm JSON/JFR. Native stages retain build/run logs and the
native executable report. Consumer, soak, reproducibility and local performance
stages retain their corresponding complete reports and referenced files.

Map all required gate names to their captured directories in a JSON file, then:

```sh
python3 scripts/assemble_release_evidence.py --candidate build/candidate.json \
  --stages /path/to/stage-directories.json --output build/release-evidence
python3 scripts/verify_release_evidence.py build/release-evidence \
  --sha256 "$MANIFEST_SHA256" --candidate build/candidate.json \
  --baseline-review-sha256 "$BASELINE_REVIEW_SHA256"
```

The validator requires actual JDK 17/21/25 tests, independent core/fallback
mutation scores, core branch coverage, Redis and Valkey topology scenarios,
36 external consumers, all three native executables, a complete one-hour soak,
reproducible core builds, negative controls and the local performance comparison.
Missing/skipped/stale/tampered evidence fails acceptance. No stage can replace
another, and a runtime-change archive is never certification evidence.

The local publisher consumes a ZIP containing `manifest.json` at its root.
Retain the archive SHA-256, manifest SHA-256 and independently reviewed baseline
digest alongside the candidate. No CI release variables or remote evidence URL
are required. The importer validates the archive digest and rejects path
traversal, symbolic links, missing manifests and oversized archives. Local
acceptance binds imported evidence to the prepared candidate; publication never
rebuilds it. GitHub does not execute benchmarks or publish releases.

## Public conformance and soak

From a clean source checkout with Docker and JDK 17 installed:

```sh
./gradlew :quotaflow-tck:test -PtestJdk=17 \
  --tests '*GeneratedNumericModelTest' --tests '*GeneratedRaceModelTckTest' \
  --tests '*RecoveryProcessTckTest' --tests '*StaggeredRecoveryEnvelopeTckTest'
./gradlew :quotaflow-tck:test -PtestJdk=17 \
  --tests '*SentinelTopologyTckTest' --tests '*ClusterTopologyTckTest'
./gradlew :quotaflow-tck:valkeyTopologyTest -PtestJdk=17
./gradlew :quotaflow-tck:soakTest -PtestJdk=21 -PsoakDurationSeconds=3600
```

Topology scenarios use dedicated local ports 17000–17005 and 17100–17102; run
these suites sequentially on one host. Redis/Valkey containers are disposable.
A 60-second soak is useful for harness diagnostics but reports DIAGNOSTIC and
cannot satisfy release acceptance. Ordinary `build` includes topology checks and
excludes the one-hour soak; nightly runs retain its latency and recovery report.

## Recorded implementation verification

The completed quality-gate implementation was verified at commit
`4cf50f0856d9672cb3dbd2a9c8c7208f60057ad1`, version `0.1.0-SNAPSHOT`.
All 16 required evidence stages passed: actual-worker JDK 17/21/25 checks,
coverage, independent core/fallback mutation gates, Redis/Valkey topologies,
36 external consumer combinations, three native workloads, the one-hour soak,
reproducibility, negative controls and comparative performance.

The reviewed local Docker Desktop profile passed all 12 performance metrics
with three complete runs per version and a maximum upper regression bound of
8.44%. Earlier native-host measurements remain inconclusive and are retained
separately. Benchmarks were not executed in GitHub.

The local evidence directory is `build/reports/release-candidate-4cf50f0/`.
Its `publishing-handoff.json` identifies the verified candidate and archive:

- Verification manifest SHA-256: `c36405e2a81d1c2d640ec83d18fd0f665d8f456f37c1eae0d89e1b54576b381d`.
- Evidence ZIP SHA-256: `624636c69ea678863e62ce87f55bc6de3af760e0d08699f9ef136e02c17b1f68`.
- Baseline review SHA-256: `80ddb6e28a9f53dcadd6c649b60efdcb68b15552539385898dc14fdfe243718b`.

This records verification of that snapshot, not publication authorization.
A different commit, release version, source tree or artifact set requires
matching fresh evidence, plus the release security and signing gates.
