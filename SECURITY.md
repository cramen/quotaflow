# Security Policy

## Reporting a Vulnerability

Please report vulnerabilities **privately** through
[GitHub private vulnerability reporting](https://github.com/cramen/quotaflow/security/advisories/new).
Do not open public issues for security problems.

You will receive an acknowledgment within 48 hours.

## Fix SLAs

| Severity | Target fix time |
|---|---|
| Critical | 7 days |
| High | 30 days |

Fixes are released as soon as they are verified, not batched into feature releases.

## Supported Versions

- **JDK**: Java 17 bytecode baseline; certification targets JDK 17, 21 and 25, including the two latest LTS lines, 21 and 25.
- **Spring Boot**: certification targets 4.1.1 and 4.0.8 (the two latest GA minor lines at the October 2026 review), plus the advertised 3.5.16 regression line. Support does not imply every patch in a major line has been tested.
- **Evidence**: the completed quality-gate snapshot and its tested versions are recorded in [verification](docs/verification.md). Each release must supply fresh evidence for its own commit, version and artifact hashes; that snapshot does not certify a later candidate.
- **Consumer profiles**: supported fixture versions require the [tested dependency alignment](docs/consumer-compatibility.md). The legacy Boot 3.5 REST fixture has an exact, expiring XSLT non-reachability review; it is not a waiver for arbitrary applications or a replacement for vendor security maintenance.
- **Security backports**: fixes are backported to the latest minor release line for 12 months after its release.

## Supply Chain

- Every release is published with a CycloneDX SBOM attached to the GitHub Release.
- Release artifacts require PGP signatures and a PGP-signed release manifest; see [artifact verification](docs/artifact-verification.md). Production publication remains blocked until the approved public key and full fingerprint are configured and published.
- Reachable vulnerabilities at any severity, unknown reachability, missing scans and stale security evidence block a release. Exact package/advisory exclusions require a reviewed rationale, supporting evidence, an owner and an expiry.
- Secrets (tokens, keys, Redis passwords) are never logged at any level; limit keys never appear in logs above DEBUG or in metric tags.
