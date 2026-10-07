# Release publishing implementation status

Publication is local-only under `io.github.cramen:quotaflow-*`. GitHub Actions
verifies changes and cannot sign or publish a release. The operator uses existing
local Central credentials, PGP configuration and GitHub authentication; see the
[operator guide](release-operator-guide.md).

The immutable candidate, complete evidence gates, durable recovery journal,
Central adapter and GitHub asset reconciliation are retained. Local signing and
Sigstore identity replace the previous CI identity. Existing reports from commit
`1356fe1` describe the earlier coordinates and orchestration; they do not certify
the changed candidate. Final exact-candidate verification and production identity
checks remain prerequisites, reported honestly by `release_readiness.py`.

No external publication is implied by implementation or a rehearsal. Production
publication requires explicit operator invocation after successful acceptance.
