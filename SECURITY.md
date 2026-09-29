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

- **JDK**: the last two LTS lines (currently 17 and 21; 25 is certified).
- **Spring Boot**: the last two major lines (3.x and 4.x).
- **Security backports**: fixes are backported to the latest minor release line for 12 months after its release.

## Supply Chain

- Every release is published with a CycloneDX SBOM attached to the GitHub Release.
- Release artifacts are PGP-signed; verify signatures against the project's public key.
- Dependencies with known, reachable CVEs block a release.
- Secrets (tokens, keys, Redis passwords) are never logged at any level; limit keys never appear in logs above DEBUG or in metric tags.
