# Fixed REST-fixture reachability review

Scope: GHSA-pc63-qcmh-9cmg / CVE-2026-47884 in exactly
`pkg:maven/org.springframework/spring-webmvc@6.2.19`, as resolved by the fixed
Quotaflow Boot 3.5.16 compatibility fixture. This review does not apply to arbitrary
applications, library production scans, other advisories or package versions.

The [vendor advisory](https://spring.io/security/cve-2026-47884/) identifies XSLT
view rendering with a wildcard route and an unspecified view name as the affected
path. The 6.2 fix is available through vendor enterprise support. No patched 6.2
artifact is assumed to exist in the public repository.

The fixture exposes one explicit REST route returning a response body and the
framework error route. It configures no XSLT view. The executable
`ConsumerViewSafety` check rejects XsltView beans, XSLT-capable view classes,
unreviewed view resolvers and wildcard controller mappings. It also constructs an
unsafe context containing an actual XsltView and verifies rejection. Each affected
Maven/Gradle/JDK/stack run must retain both the positive assertion and negative
control markers. A missing marker blocks the scoped review.

The review is bound to the exact library binary set and the hashes of the fixture,
its guard and this rationale. Its owner is the automated fixture review recorded
in source control; the fixed expiry requires renewed review, never an automatically
extended date. Any changed source, unmatched package, missing evidence or expired
review blocks acceptance.

Boot 3.5 is an additional compatibility regression target, not a statement that
its stock BOM is secure for every application. Production applications using its
affected MVC feature need the vendor-supported patched framework or a supported
Boot 4 line. The application's complete runtime and reachable features remain its
own security-review scope.
