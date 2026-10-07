# Verify release artifacts

Quotaflow requires both PGP signatures for Maven assets and a Sigstore signature
over the release manifest. The manifest binds the seven libraries, their version,
source commit, SBOM and verification evidence to the exact shipped bytes.

## Project PGP identity

The trusted full primary fingerprint and repository-relative public-key path are
declared in [`verification/release-policy.json`](../verification/release-policy.json).
The initial production key is not configured yet. A null fingerprint is a blocking
prerequisite, not a request to trust a key found beside an artifact. Test keys used
by the verification scripts cannot authorize production publication.

The release owner must review the public key's provenance, commit the approved
armored public key at the declared path, and replace the null fingerprint with
its full fingerprint. Publish that key to a Central-supported public key server
and link its fingerprint from the release notes. Keep the private key and optional
passphrase exclusively in the signing environment's secrets. Never submit them
in an issue, log, shell command argument or chat.

Obtain the public key from the trusted project checkout, compare its full
fingerprint with the independently reviewed policy, then import it into a
dedicated keyring. Verify each downloaded Maven asset against its `.asc` sidecar:

```sh
gpg --show-keys --with-fingerprint security/release-public-key.asc
gpg --import security/release-public-key.asc
gpg --verify quotaflow-core-VERSION.jar.asc quotaflow-core-VERSION.jar
```

Replace `VERSION` with the release version. A valid signature from an unexpected,
expired or revoked key is not an acceptable release signature. The automated
verifier checks the primary identity even when a signing subkey was used.

## Sigstore identity and manifest hashes

Install the pinned Cosign version with the repository's digest-checking installer:

```sh
python3 scripts/release_tools.py cosign --install
```

The installer prints the verified executable path. Use that executable to verify
the downloaded manifest and Sigstore bundle, replacing the three uppercase
placeholders with the actual version, full commit SHA and file paths:

```sh
cosign verify-blob \
  --bundle MANIFEST.sigstore.json \
  --certificate-identity 'https://github.com/cramen/quotaflow/.github/workflows/release.yml@refs/tags/vVERSION' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  --certificate-github-workflow-repository 'cramen/quotaflow' \
  --certificate-github-workflow-ref 'refs/tags/vVERSION' \
  --certificate-github-workflow-sha COMMIT \
  MANIFEST.json
```

Successful verification must include the expected workflow identity and
transparency evidence. Do not disable transparency verification, use a permissive
identity regular expression or substitute an ambient custom trust root.
After verifying the manifest signature, compare the SHA-256 of every downloaded
artifact and SBOM with its entry in that manifest. Verifying the manifest alone
does not verify a different JAR downloaded later.

## Security evidence

The production CycloneDX SBOM is scanned with a digest-pinned Grype executable.
The report retains the complete database in lossless gzip form, both compressed
and original SHA-256 hashes, original size and build timestamp, the raw finding
set and the reachability review. The database and report must be no older than
24 hours at promotion. Scanner failure is not an empty finding set.

Any non-reachability review in
[`verification/reachability.json`](../verification/reachability.json) must name
the exact advisory and Maven package version, bind to the candidate library
hashes, identify an owner, explain the result with hashed evidence, and expire
within 30 days. A new or unmatched finding blocks publication regardless of
severity. The initial dependency fixes use no exclusions.

## Local verification controls

The following commands use ephemeral test keys or public verification vectors;
they do not publish, issue production signatures, or prove project release
readiness:

```sh
python3 scripts/test_release_security.py
python3 scripts/test_release_signatures.py
python3 scripts/test_sigstore_integration.py --output build/sigstore-controls
python3 scripts/test_release_promotion.py
```

The Sigstore integration control needs the pinned Cosign installation and network
access for public test data and Sigstore trust metadata. Its vendor fixture has
a main-branch workflow identity; the production verifier still requires the
Quotaflow release tag identity shown above.
