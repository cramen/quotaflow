# Seal and rehearse a release candidate locally

These commands exercise packaging and publication transitions without a production
PGP key or publishing credentials. They do not establish production readiness.
The local production command uses the integrated entry point; direct Gradle upload
remains disabled. The separate [production acceptance entry point](release-acceptance.md)
requires all real evidence and rejects rehearsal output.

## Prepare and seal

Choose fresh output directories for every invocation. Preparation runs Gradle;
sealing and rehearsal never rebuild the libraries.

```sh
python3 scripts/prepare_release.py prepare \
  --version 0.1.0 --dry-run --output build/local-candidate
python3 scripts/release_candidate.py assemble \
  --prepared build/local-candidate \
  --output build/local-sealed --rehearsal
```

Assembly rechecks all seven module publications, POM/Gradle metadata, file hashes,
the SBOM's correspondence to the resolved production graph and the exact schema
validation report. Existing PGP signatures, if supplied, are cryptographically
verified and packaged with checksum sidecars. Unsigned rehearsal is allowed and
does not satisfy the production signing requirement.

The output includes `release-manifest.json`, a deterministic Maven ZIP, the
repository files, SBOM and bound evidence. The manifest is canonical JSON and
records `mode: rehearsal`, `state: SEALED_UNVERIFIED` and
`eligibleForPromotion: false`. Assembly prints its SHA-256. Preserve this digest
independently; do not replace it with the hash of a later modified file.

## Verify and rehearse

Replace `MANIFEST_SHA256` below with the digest printed by assembly:

```sh
python3 scripts/release_candidate.py verify \
  --candidate build/local-sealed --sha256 MANIFEST_SHA256
python3 scripts/release_rehearsal.py \
  --candidate build/local-sealed --sha256 MANIFEST_SHA256 \
  --output build/local-rehearsal
```

Rehearsal uses local filesystem service doubles and the same promotion state
machine and chained journal components. It simulates draft preparation, one
complete upload, promotion, GitHub completion and a fresh runner resuming completed
work. There is no configurable remote endpoint. Ambient Central/GitHub credentials
do not enable external operations.

Inspect `build/local-rehearsal/rehearsal.json` and the local `github/` and `portal/`
directories. `SIMULATED_COMPLETE` means only that local transitions passed.
`productionReadiness` remains `UNVERIFIED`; missing quality/security/consumer/
example/signature evidence is listed explicitly. The rehearsal report is outside
the immutable sealed directory.

## Include existing evidence

Supply optional directories when assembling:

```sh
python3 scripts/release_candidate.py assemble \
  --prepared build/local-candidate --output build/local-sealed-with-evidence \
  --rehearsal --report quality=build/local-quality \
  --report security=build/local-security
```

The quality directory must contain `verification.zip`, the complete evidence
archive produced by the quality collectors. The security directory contains the
unmodified output of `scripts/scan_release.py`, including its database and raw
report. Evidence copying binds bytes; it does not assert that a report passes.

Revalidate evidence with independently reviewed archive, quality manifest and
baseline-review digests:

```sh
python3 scripts/release_evidence_gate.py \
  --candidate build/local-sealed-with-evidence --sha256 MANIFEST_SHA256 \
  --quality-archive-sha256 ARCHIVE_SHA256 \
  --quality-manifest-sha256 QUALITY_MANIFEST_SHA256 \
  --baseline-review-sha256 BASELINE_REVIEW_SHA256
```

All 16 quality stages (including explicit WAIVED performance evidence only for
the policy-approved 0.1.0 binaries) must identify the same commit, source digest, version and
library hashes. The security scan must also bind the same prepared manifest and
SBOM, remain current and have no unresolved findings. This command reports only
quality/security acceptance; production signature, consumer, example and operator
requirements remain separate. It never runs benchmarks. The rehearsal command
accepts the same three quality digest options; supplied security evidence is
always revalidated, and invalid supplied evidence stops the rehearsal.

## Controls

```sh
python3 scripts/test_release_candidate.py
python3 scripts/test_release_evidence_gate.py
python3 scripts/test_release_rehearsal.py
```

Assembly controls include real isolated ephemeral PGP signatures and require GPG.
Quality/security tests use synthetic evidence labeled as test fixtures. Rehearsal
tests refuse network transport even with dummy ambient publishing credentials.
No test output or earlier prepared snapshot certifies the final release candidate.
