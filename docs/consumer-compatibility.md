# Consumer compatibility and runtime security

Release verification tests the public Maven/Gradle publications rather than
project dependencies. The required matrix contains 60 consumers: core, Kotlin,
Redis and Micrometer across both tools and JDK 17/21/25, plus servlet/reactive
starter fixtures for Boot 4.1.1, 4.0.8 and the additional 3.5.16 regression line.
Five executable README cases cover rejection, throttle, named keys, HTTP 429 and
Redis outage/recovery. Final certification requires the matching candidate reports.
Every starter case uses default distributed wiring with a fresh namespace and
fixed single-owner cohort. It verifies refusal before provisioning, refusal of
a duplicate owner, healthy recovery and HTTP decisions. Negative probes use
non-web contexts so they cannot dispose another context's global Reactor server
resources. The first four README cases use a controlled local store clock for
repeatable quota/queue assertions; the outage case uses actual Redis interruption.

## Tested dependency alignment

Application dependency management can override transitive library BOMs. A
successful compile or HTTP smoke test does not establish security. The initial
fixture runtime scan found affected Netty, Log4j, Tomcat, Jackson and legacy MVC
versions even though the library's own production SBOM scan had passed.
The Redis publication now declares its Netty entry modules directly, because a
transitive library's imported BOM alone did not align ordinary Maven consumers.
The standalone fixture verifies the published metadata without an application
Netty override. Application-owned Boot BOMs still require the alignment below.

The required fixture alignment is recorded in
[`consumer-runtime.json`](../verification/consumer-runtime.json):

| Component | Required fixture version |
|---|---|
| Netty BOM | 4.2.17.Final |
| Log4j BOM | 2.25.5 |
| Jackson 2 BOM | 2.21.7 |
| Jackson 3 BOM | 3.1.7 |
| Tomcat embed core, EL and WebSocket on Boot 4 | 11.0.26 |
| Tomcat embed core, EL and WebSocket on Boot 3.5 | 10.1.60 |

For Maven, import the security BOMs before the Boot BOM in your application's
`dependencyManagement`; explicitly manage the three Tomcat embed artifacts.
For Gradle, add the BOM platforms and Tomcat dependency constraints alongside
the Boot platform. The fixture generator in `scripts/run_staged_verification.py`
contains executable examples of both forms. Apply only the profile matching your
Boot line, retain framework compatibility tests and scan the actual resolved
runtime after every dependency-management change. These versions are a reviewed
profile, not a permanent vulnerability-free guarantee.

Every full staged run creates and scans the union of its actual runtime JAR
coordinates and SHA-256 hashes. The sealed reports retain the raw findings,
database identity, SBOM and all per-run classpaths. New, stale, unresolved or
unmatched findings block acceptance regardless of severity.

## Legacy MVC scope

The Boot 3.5 regression fixture resolves Spring MVC 6.2.19. The vendor's fix for
[CVE-2026-47884](https://spring.io/security/cve-2026-47884/) on the 6.2 line requires
enterprise support; its affected feature is XSLT view rendering under the stated
wildcard-mapping conditions.

The fixed REST fixture has a separate, expiring
[reachability review](../verification/consumer-xslt-review.md). It is tied to the
exact package, advisory, library binaries and proof sources. Runtime checks reject
XSLT views and wildcard controller mappings, inspect the resolver chain, and prove
the guard fails on a real unsafe XsltView context. All affected runs must contain
both proof markers. Missing evidence or expiry blocks the gate.

This exception applies only to the frozen test fixture. It is not inherited by
the library's production scan and does not certify arbitrary Boot 3.5 applications.
Applications using the affected feature need vendor-supported patched Spring
artifacts or a supported Boot 4 line. Keep application-specific reachability and
security maintenance separate from library compatibility.
