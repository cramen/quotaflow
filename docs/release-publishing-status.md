# Release publishing implementation status

The publishing work is being committed as a partial implementation. It does not
establish release readiness or authorize external publication. The archived plan
retains 5 completed tasks and 16 open tasks; its supply-chain delta has not been
merged into the main specification.

## Implemented and verified

- Complete-history tag validation and explicit credential property mapping.
- Local staging of all seven public modules with POMs, sources, useful API
  documentation and an aggregate production CycloneDX SBOM.
- Pinned scanner/tool downloads, exact expiring reachability review, isolated PGP
  controls and repository/workflow/issuer/commit-bound Sigstore verification.
- Promotion and transport components with failure-injection tests, including a
  durable append-only release journal and recovery without duplicate uploads.
- Runtime dependency corrections for Netty, Micrometer and Log4j. The local
  candidate scan on October 5, 2026 returned no findings and used no exclusions.

The full JDK 17 build and dependency audit passed during implementation. Local
security, signing, preparation and recovery controls also passed. These results
are historical implementation evidence, not certification of future candidate
bytes; time-limited security evidence must be refreshed before promotion.

## Next implementation priority

Complete the end-to-end publishing path before starting an unrelated release
change. Preserve the existing open acceptance criteria:

1. Assemble an immutable signed manifest and Maven bundle, and connect all actual
   security/signature/quality validators to the promotion entry point. Import
   pinned local performance evidence; never run benchmarks in GitHub.
2. Replace the old workflow publishing command with that entry point. Separate
   preparation, signing and promotion credentials and permissions, pin actions,
   and serialize operations by version. The existing direct Gradle Central upload
   task is intentionally blocked; the current workflow cannot publish a release.
3. Exercise clean Maven/Gradle consumers, every advertised Boot fixture and the
   executable README examples against the staged artifacts.
4. Run the complete credential-free rehearsal and negative-path matrix through
   the integrated entry point, including runner loss and post-Central recovery.
5. Finish the operator guide and obtain fresh complete evidence for the final
   commit, version and artifact hashes before declaring release readiness.

The approved production PGP public key and full fingerprint are still missing.
See [artifact verification](artifact-verification.md). Namespace ownership,
protected release environments, CI secrets and local evidence transfer also need
explicit operator setup. Test keys and public vendor signature fixtures do not
substitute for the project's production identity.

## Successor implementation in progress

Canonical candidate sealing and a local rehearsal are now available; see
[release rehearsal](release-rehearsal.md). Sealing verifies the prepared artifact
inventory, SBOM and metadata, packages deterministic Maven bytes and binds report
files. A read-only evidence gate revalidates all 16 quality stages and current
security evidence. The local rehearsal uses the promotion state machine and
durable journal with filesystem service doubles, including completed-run resume.

The [production acceptance and publication entry points](release-acceptance.md)
now connect these checks to the Portal/GitHub orchestrator and isolated CI jobs.
Every required gate runs before publishing mutations; missing production identity
and staged consumer/example evidence block acceptance. The external fixture runner
executes 60 consumers and five README scenarios, scans resolved runtime bytes and
retains exact-candidate evidence. Use `scripts/release_readiness.py` to inspect the
current result; historical or diagnostic runs do not certify a later candidate.
Live production identity verification and final certification remain outstanding.
Rehearsal outputs explicitly report production readiness as unverified.
