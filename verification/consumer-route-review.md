# Fixed REST fixture route reachability review

This review covers only the hash-bound Quotaflow compatibility fixtures and the
reviewed 0.1.0 library binaries. It does not establish the safety of arbitrary
Spring applications or authorize an exception in the library production scan.

Spring's [CVE-2026-47890 advisory](https://spring.io/security/cve-2026-47890/)
rates SSE fragment stream corruption Low. Exploitation requires rendering view
fragments over SSE with attacker-controlled data streamed to other users.
Spring's [CVE-2026-47892 advisory](https://spring.io/security/cve-2026-47892/)
rates functional-endpoint header predicate bypass Medium. Scanner severity
labels do not replace assessment of these conditions.

The fixed controllers expose only an annotated `/quota/{identity}` GET returning
constant `ok` text, directly or through `Mono<String>`. They do not render view
fragments, stream events to other users, or implement application authorization
through functional header predicates. Both transport stacks are explicitly
selected and checked at runtime. Framework-internal error rendering is not an
application authorization predicate or a shared SSE fragment stream.

`ConsumerRouteSafety` inspects application handler mappings for SSE media types
and emitter/event/fragment return declarations. It rejects registered MVC or
WebFlux functional route beans and nonempty functional handler mappings. Every
affected consumer run must execute these checks. Negative controls install an
actual SSE handler mapping and real MVC and WebFlux router functions with header
predicates; each unsafe context must be rejected. Source hashes and complete
candidate binary identity prevent reuse after a fixture or library change.

The review applies only to Spring WebMVC/WebFlux 6.2.19 in the fixed Boot 3.5.16
fixture. It expires with the recorded policy, never applies to another package
version, and must not be extended merely to pass a scan. Applications using the
affected features need the vendor-supported 6.2.20 fix or a fixed supported
Spring 7 line (7.0.9 or later), with their own security assessment.
