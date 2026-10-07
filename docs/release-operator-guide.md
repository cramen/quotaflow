# Release operator guide

The release workflow has four isolated jobs: preparation, signing, acceptance and
promotion. Preparation has read-only repository access and no publishing secrets.
Only signing receives the private PGP key and OIDC permission. Only promotion
receives Central credentials and GitHub release write permission. Actions and
downloaded release tools are pinned; job transfers have independently retained
SHA-256 values. A version-scoped concurrency group never cancels active promotion.

## Prerequisites

- A reviewed non-snapshot semantic version, a clean commit on complete trusted
  main history, and protected release tags. Review all source and dependency
  changes before creating the release tag.
- Verified ownership of the `io.quotaflow` Central namespace and a publishing
  token. Configure `CENTRAL_PORTAL_USERNAME` and `CENTRAL_PORTAL_TOKEN` only in the
  protected `release-promotion` environment. They map to
  `ORG_GRADLE_PROJECT_mavenCentralUsername` and `...mavenCentralPassword`.
- An approved public PGP key and full primary fingerprint committed at the paths
  in `verification/release-policy.json`, plus discoverable public-key publication.
  Configure `SIGNING_KEY` and its optional `SIGNING_PASSWORD` only in the protected
  `release-signing` environment. An unprotected key may have an empty passphrase.
  This setup is currently deferred; no test key substitutes for it.
- Signing and promotion environment protections appropriate to the release owners.
  Sigstore must attest the exact project repository, release workflow, tag and
  commit under the GitHub Actions issuer. No long-lived Sigstore private key is
  required.
- Python 3.11+, GPG, the Gradle wrapper, Maven, JDK 17/21/25 and Docker for local
  checks. Workflow policy tests use Ruby's standard safe YAML parser. Release
  tooling is installed through `scripts/release_tools.py`, which checks pinned
  archive and executable digests.
- Complete exact-candidate quality evidence, including locally measured performance
  against a reviewed matching baseline/profile. Follow
  [verification](verification.md); diagnostic measurements do not satisfy release
  certification. GitHub never runs benchmarks.

Keep credentials out of command arguments, source, logs and evidence archives.
Do not paste private keys into chat or issues. Missing prerequisites are explicit
failures; operators must not edit reports to turn them into passing evidence.

## Local preparation and rehearsal without keys

Follow [release rehearsal](release-rehearsal.md) for the complete credential-free
path. To execute staged consumers and README examples before sealing, run:

```sh
python3 scripts/run_staged_verification.py \
  --prepared build/local-candidate --output build/local-reports \
  --work build/local-staged-work \
  --java-home "17=/path/to/jdk17" \
  --java-home "21=/path/to/jdk21" \
  --java-home "25=/path/to/jdk25"
```

Prepare the candidate after finishing source edits; a full staged run requires
the same source digest before and after execution. It owns and removes a disposable
Redis container, uses fresh Maven/Gradle projects and isolated dependency caches,
and pauses only its own container for the outage example. `--only` is a diagnostic
filter; filtered runs cannot satisfy the complete matrix.
Application environment overrides, publishing credentials, injected JVM/Maven
options and user Maven RC files are excluded from these isolated executions.

The consumer runtime uses the explicit [security alignment](consumer-compatibility.md).
Its resolved dependency union is scanned as well as the library production SBOM.
The narrowly scoped legacy XSLT review requires matching library/proof hashes,
unexpired review metadata and executable positive/negative evidence. Renew it
only after reviewing the actual affected path again; never extend dates or rebind
hashes merely to pass the gate.

Retain source-bound control results and an honest readiness report:

```sh
python3 scripts/run_release_controls.py --output build/release-controls
python3 scripts/release_readiness.py --prepared build/local-candidate \
  --staged build/local-reports --controls build/release-controls/controls.json \
  --output build/release-readiness.json
```

Without a complete signed candidate and operator review this report remains
`UNVERIFIED`, even when packaging, controls and staged execution pass. Its
audit mapping lists the required evidence for each preceding correctness,
recovery, lifecycle, observability and quality workstream. Final readiness also
requires the sealed candidate/digests and a current operator-review document,
as enforced by the audit tool; no production key is needed to obtain the honest
partial report above.

## Transfer local quality evidence

Produce the full quality evidence ZIP from the exact commit, version and library
hashes. Retain the ZIP SHA-256, its root manifest SHA-256 and the reviewed baseline
SHA-256 independently. Make the immutable ZIP available through a trusted HTTPS
URL and configure these repository variables:

- `QUOTAFLOW_RELEASE_EVIDENCE_URL`
- `QUOTAFLOW_RELEASE_EVIDENCE_SHA256`
- `QUOTAFLOW_RELEASE_MANIFEST_SHA256`
- `QUOTAFLOW_BASELINE_REVIEW_SHA256`

The workflow imports the archive and revalidates all 16 stages against the binaries
it prepared. If toolchain/platform differences change the JAR hashes, reproduce
the matching build and verification; do not rewrite hashes in old evidence. A
snapshot report or a green build from another commit cannot authorize a release.

## Candidate review and new publication

Inspect the seven-module Maven bundle, POM/source/API documentation, production
SBOM, all quality/security/consumer/example evidence and signatures. Check the
audit/readiness report and operator prerequisites. A valid release tag pushed on
main is the concrete publication trigger, subject to environment protections.
Implementation or rehearsal alone does not authorize pushing a tag.

Preparation uploads a digest-pinned transfer. Signing imports those exact bytes,
creates PGP sidecars, seals the release and obtains the repository-bound Sigstore
bundle. Save the signing job's manifest digest, signed-transfer digest, artifact
ID and workflow run ID. Acceptance downloads and rechecks them independently.
Promotion never rebuilds or signs.

The publisher prepares and verifies uniquely named GitHub draft assets, writes
durable intent, uploads one complete `USER_MANAGED` Central deployment, persists
the returned ID and waits for validation before promoting that ID. It rechecks
actual evidence immediately before upload and promotion. A bounded wait ending
with `PENDING` is an incomplete operation, not a successful release.

## Resume without rebuilding

Prefer rerunning only a failed promotion job while the preserved signed artifact
is available. Alternatively dispatch the Release workflow **on the original tag**
with `operation=resume`, `candidate_run_id`, `signed_artifact_id`,
`manifest_sha256` and `transfer_sha256` from the original signing run. Preserve the
same quality-evidence pins. Resume skips preparation and signing. It verifies the
preserved transfer and reconciles live Central state before any mutation.

If the upload response was lost before its ID was durably recorded, stop retries
and recover the existing deployment ID from the Portal. Supply it as
`recovered_deployment`. The process verifies coordinates and every staged byte.
Absence from public Maven Central does not prove that no staging deployment exists;
never upload a replacement merely because no ID was returned.

The journal is an append-only hash chain stored as release assets. Retain all
records; do not delete, reorder or rewrite them. Gaps, forks and asset collisions
are failures. If validation fails, preserve the deployment for diagnosis. If
Central has published but GitHub completion fails, resume completes only that
release. Live `PUBLISHED` status plus matching bytes permits completion after scan
expiry; signatures and immutable evidence are still checked. There is no rollback
of published Central coordinates: corrections require a new version.

Signed CI artifacts have 90-day retention. Preserve an independent copy of the
signed transfer and its digests for incident recovery; the verified GitHub draft
also retains a complete candidate delivery archive before Central is touched.
Never reconstruct a lost signed candidate by rebuilding the same version.

## Runtime rollout and compatibility changes

Publication does not provision application Redis state. Before activating a new
deployment, follow [canonical quota state](canonical-quota-state.md) and
[conservative recovery](conservative-recovery.md): explicitly provision a fresh
namespace, register policy bindings and root controllers, declare the complete
fixed cohort and configure one stable unique ID per owner. `expected-instances`
alone does not prove fleet membership. Duplicate IDs cannot replace active owners.

For an existing namespace, stop all old local/distributed writers and establish
drained debt before codec/identity migration or cohort maintenance. Keep writers
stopped during the operation. Use the complete domain inventory, expected
incarnation and a stable maintenance token for retry. Do not use a timeout or a
disconnected JVM as proof of quiescence. Preserve debt and fences; never reset an
unknown namespace to recover availability. See [numeric state](numeric-state.md)
for immutable algorithm bindings and supported values.

Review telemetry consumers during rollout: wait events represent queue entries,
terminal allow/reject counts exclude those entries, and utilization no longer has
a `key-group` tag. Budget gauges are sampled policy summaries rather than live
fleet balances. Never put raw keys in tags or ordinary logs. Retain the documented
Lettuce protocol logging restriction even when enabling global DEBUG/TRACE.
