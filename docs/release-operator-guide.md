# Local release operator guide

All signing and publication runs on the operator's local machine. GitHub Actions
only verifies source changes; it has no release dispatch, signing or publishing
jobs. Pushing a tag does not publish. Maven coordinates are
`io.github.cramen:quotaflow-*`; Java and Kotlin packages remain `io.quotaflow`.

## Existing local configuration

Use the existing Gradle user properties (`~/.gradle/gradle.properties`, or
`GRADLE_USER_HOME/gradle.properties`): `mavenCentralUsername`,
`mavenCentralPassword`, `signing.keyId`, `signing.password` and
`signing.secretKeyRingFile` (absolute path). Java property escapes and line
continuations are supported. Explicit environment credential properties override
file properties; conflicting environment aliases fail preflight. In-memory PGP
keys remain supported. Credentials and private keys never enter candidate reports.

GitHub operations use `GH_TOKEN`, `GITHUB_TOKEN`, or existing `gh auth login`
authentication for github.com. Namespace ownership and token access must cover
`io.github.cramen`. Do not generate a replacement PGP key when the existing one
is usable. Commit only its approved public key and full fingerprint in
`verification/release-policy.json`; ensure the public key is discoverable by
Central consumers. See [artifact verification](artifact-verification.md).

The same approved PGP key signs the canonical release manifest. No additional
account, signing service, OIDC identity or browser authentication is required.
The signed manifest binds repository, version, commit and artifact hashes.

Use Python 3.11+, GPG, gh, Maven, JDK 17/21/25, Docker and the Gradle wrapper.
Install pinned release tools through `scripts/release_tools.py`. YAML policy tests
use Ruby. Review changes, commit them, and select a non-snapshot tag on complete
trusted main history before preparing the production candidate.

## Preparation, checks and signing

Follow [release rehearsal](release-rehearsal.md) for credential-free preparation
and negative-path testing. Follow [verification](verification.md) for all quality
stages, including local benchmarks. Preserve the exact-candidate quality archive,
its SHA-256, root manifest SHA-256 and baseline review SHA-256 independently.
No public evidence URL or CI repository variable is required.

Run the staged consumers and examples before sealing:

```sh
python3 scripts/run_staged_verification.py \
  --prepared build/local-candidate --output build/local-reports \
  --work build/local-staged-work \
  --java-home "17=/path/to/jdk17" \
  --java-home "21=/path/to/jdk21" \
  --java-home "25=/path/to/jdk25"
python3 scripts/run_release_controls.py --output build/release-controls
python3 scripts/release_signatures.py pgp-sign --candidate build/local-candidate
```

The staged runner uses isolated caches and its own disposable Redis container.
Filtered diagnostics cannot satisfy the full matrix. The actual runtime dependency
union is scanned; [consumer alignment](consumer-compatibility.md) and scoped
reachability reviews must still match the candidate. Do not rebind old reports.

Seal the signed candidate with complete quality, security, consumer and example
reports using `release_candidate.py assemble` (see its `--help` for report arguments),
then sign the sealed manifest:

```sh
python3 scripts/release_signatures.py manifest-sign \
  --manifest build/release-sealed/release-manifest.json
```

Signing runs locally and does not upload Maven artifacts or create a GitHub Release. Retain the manifest
digest and signed candidate. Run [read-only acceptance](release-acceptance.md)
before explicitly authorizing publication. Ordinary build success is insufficient.

## Explicit local publication and recovery

After authorization, invoke `scripts/publish_release.py` with the candidate and
all four independently retained digest arguments shown in the acceptance guide.
It validates the same gates, creates and verifies GitHub draft assets, uploads
one complete USER_MANAGED Central deployment, then completes the GitHub Release.
The assets include the Maven bundle, SBOM, signed manifest, detached PGP signature and
complete recovery archive. No library is rebuilt or signed during publication.

An exclusive per-repository/version lock under `~/.quotaflow/release-locks`
refuses concurrent attempts on this machine. Do not publish the same version from
another machine concurrently; the remote journal detects conflicting history but
is not a distributed lock. GitHub Actions invocation is refused.

`PENDING` is incomplete. Resume the same local command with identical files and
digests. Preserve the append-only GitHub journal. For an uncertain upload without
a recorded ID, recover the original Portal deployment and pass
`--recovered-deployment ID`; do not create another deployment. Coordinates and
bytes must match. Confirmed Central publication permits only completion of the
same GitHub Release, even after scan expiry, while signature and immutable-byte
checks remain mandatory. Corrections to published bytes require a new version.

Retain an independent copy of signed candidates and digests; no CI artifact
retention is involved. A rebuilt copy is not a substitute for lost signed bytes.
Readiness remains UNVERIFIED until production acceptance and a fresh operator
review pass. Operator checks cover namespaceOwnership, protectedTags,
localSigningConfiguration, localPublishingAuthentication, publishedPgpIdentity
and evidenceTransfer (retention of the pinned recovery/evidence bundle).

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
