# Production acceptance and publication entry points

`scripts/release_acceptance.py` verifies a sealed production candidate without
publishing. `scripts/publish_release.py` performs one bounded advancement through
the same checks and the Portal/GitHub state machine. The operator invokes these entry points locally; GitHub Actions cannot publish. Live production
identity verification and final candidate certification remain outstanding.

## Read-only acceptance

Provide the independently retained sealed-manifest digest and the three reviewed
quality-evidence digests:

```sh
python3 scripts/release_acceptance.py \
  --candidate build/release-sealed --sha256 MANIFEST_SHA256 \
  --quality-archive-sha256 ARCHIVE_SHA256 \
  --quality-manifest-sha256 QUALITY_MANIFEST_SHA256 \
  --baseline-review-sha256 BASELINE_REVIEW_SHA256
```

This command requires no Central or GitHub publishing credentials. It verifies:

1. Exact sealed inventory, canonical manifest, Maven bundle and SBOM.
2. Production mode and every required Maven PGP signature against the configured
   project fingerprint and exact approved public-key bytes.
3. A clean checkout of the exact release tag, full trusted main ancestry and
   matching source identity.
4. The manifest's detached PGP signature against the same approved key, with
   repository, tag and full commit SHA bound in the signed manifest.
5. All 16 quality stages from the independently pinned archive and current
   security evidence bound to the same prepared manifest and SBOM.
6. Complete staged consumer and README example results for that candidate.

The command does not generate evidence, rebuild, sign or execute benchmarks.
Missing production key configuration fails explicitly. A rehearsal package or
test-signature report is rejected, including if all its local tests passed.
No command-line option disables a required gate. The checked-in 0.1.0
performance-only exception is restricted to the approved binary hashes; it
produces an explicit WAIVED result, not performance PASS. All other stages remain
mandatory. See [the exception and evidence command](verification.md#performance-certification-exception-for-010).

## Staged evidence contract

The sealed package binds `reports/consumers/results.json` and
`reports/examples/results.json`, together with every referenced log. These files
must be produced by actual staged execution, not hand-written to satisfy a gate.
Their common envelope requires:

- `schemaVersion: 1`, `kind: staged-consumers` or `staged-examples`;
- exact `candidate`, `preparedManifestSha256` and `executionSourceSha256`;
- `simulation: false`, `status: PASS` and a complete `runs` list.

Each run records `id`, actual `jdk`, `status: PASS`, `exitCode: 0`,
`origin: staged-repository`, the resolved public-library binary names and hashes
in `artifacts`, and a relative `log` reference with SHA-256. The log must contain
the complete line `PUBLISHED CONSUMERS VERIFIED case=ID` or
`PUBLISHED EXAMPLES VERIFIED case=ID`, emitted only after execution assertions.
Every resolved public artifact must match the candidate and a consumer must
actually resolve its target module.

Consumer IDs comprise core/Kotlin/Redis/Micrometer for both Maven and Gradle on
each configured test JDK (`core-maven-jdk17`, for example), plus every configured
Boot version, stack, tool and JDK (`starter-4.1.1-servlet-gradle-jdk21`, for
example). Required versions come from `verification/compatibility.json`.
Example IDs are `reject`, `throttle`, `keys`, `http-429` and `outage-recovery` on
the configured bytecode-baseline JDK. Missing, duplicate, failed or simulated
cases cannot satisfy acceptance. The runner also retains and scans the actual
resolved runtime union; example dependencies must be covered by that scan.
Consumer-context reachability is separate from library triage and requires its
own exact source/binary binding and executable proof. Only complete matching
reports certify the executions; the existence of a fixture runner does not.

## Explicit publication and recovery

The publication entry point takes the same candidate and digest arguments. It
uses Central credentials through the existing canonical Gradle-property mapping
from local user properties and `GH_TOKEN`, `GITHUB_TOKEN` or `gh auth login` for release operations. Invocation performs real
external operations; use it only for an authorized release from the local operator machine. Credential-free local work uses
[release rehearsal](release-rehearsal.md) instead.

The entry point packages the existing sealed files into a deterministic delivery
archive; it does not rebuild any library. The delivery archive, Maven bundle,
signed manifest, detached PGP signature and SBOM are verified as draft assets before
Central upload. Every gate is checked before mutation, again immediately before
upload, and again before promotion. The Portal adapter hashes the exact payload
bytes before constructing its HTTP request.

A call returns `PENDING` while Central is processing. Resume with the same sealed
bytes and pinned digests. The append-only GitHub asset journal retains the
deployment identity across local process failures. An upload intent with an unknown deployment
ID is not retried automatically; after recovering the original Portal ID, pass
`--recovered-deployment ID`. Coordinates and bytes must match before recovery.

Once the same deployment is confirmed `PUBLISHED`, completion revalidates
immutable bytes, signatures and all other evidence. It evaluates the archived
security scan at its original timestamp so later expiry cannot strand an already
published release. The report says `scope: completion-only`; it does not authorize
a new upload. A journal claim alone cannot enable this behavior: live Central
status and exact deployment contents are required. If GitHub completion fails,
preserve the candidate and journal and resume the existing deployment.
