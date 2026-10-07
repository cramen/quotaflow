# Verify release artifacts

Quotaflow requires PGP signatures for Maven assets and a PGP signature
over the release manifest. The manifest binds the seven libraries, their version,
source commit, SBOM and verification evidence to the exact shipped bytes.

## Project PGP identity

The trusted full primary fingerprint and repository-relative public-key path are
declared in [`verification/release-policy.json`](../verification/release-policy.json).
The existing local production key must match the public policy. A null fingerprint is a blocking
prerequisite, not a request to trust a key found beside an artifact. Test keys used
by the verification scripts cannot authorize production publication.

The release owner must review the public key's provenance, commit the approved
armored public key at the declared path, and replace the null fingerprint with
its full fingerprint. Publish that key to a Central-supported public key server
and link its fingerprint from the release notes. Keep the private key and optional
passphrase only in local operator configuration. Never submit them
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

## Signed manifest and artifact hashes

The same approved PGP key signs `release-manifest.json`. Verify its detached
signature before trusting its repository, version, commit, artifact hashes or
SBOM references:

```sh
gpg --verify release-manifest.json.asc release-manifest.json
```

Compare the signer fingerprint with the project policy and compare every artifact
and SBOM SHA-256 with the verified manifest. A valid manifest signature does not
verify a different JAR downloaded later. No separate signing service or browser
login is required. The signature authenticates the manifest; it does not by itself
prove that the code is safe or that all required tests ran.

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

The following commands use ephemeral test keys;
they do not publish, issue production signatures, or prove project release
readiness:

```sh
python3 scripts/test_release_security.py
python3 scripts/test_release_signatures.py
python3 scripts/test_release_promotion.py
```
