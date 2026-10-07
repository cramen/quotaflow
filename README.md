# Quotaflow

Enterprise-grade **distributed rate limiting library for JVM microservices** — a standalone, self-contained library with no framework lock-in in its core.

[![CI](https://github.com/cramen/quotaflow/actions/workflows/ci.yml/badge.svg)](https://github.com/cramen/quotaflow/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

## Features

- **Hierarchical policies**: global → tenant → user → API-key, including external provider quotas (e.g. a shared LLM API quota) as a parent level. Chains evaluate atomically in one Redis round-trip.
- **Two reaction modes**: `reject` (correct HTTP 429 with `Retry-After` and an RFC 7807 body) and `throttle` (bounded priority queue with backpressure and wait timeouts — a "budget valve" for paid external APIs).
- **Degradation, not fail-open/fail-closed**: on Redis failure, validated owners use cold conservative shares, then recover through a fixed-cohort Redis barrier. Unenrolled startup grants no credit.
- **Atomic correctness**: Lua token bucket and GCRA only — no read-modify-write, boundary-burst protection, Redis server time for refill math.
- **Dynamic configuration**: hot-reload of policies without restarts, plus a tariff resolver SPI for limits from billing/CRM systems. Fenced dynamic quotas use immutable versioned snapshots.
- **First-class observability**: every decision (allow/reject/wait) is metered by policy and key-group; utilization, degradation state, and wait metrics included; reference Grafana dashboard and alerts.
- **Framework integration**: Spring Boot starter (`@RateLimited`, servlet + WebFlux) and a Kotlin coroutines facade (`suspend` / `Flow` / DSL).
- **GraalVM ready**: reachability metadata and Spring AOT hints, verified by a native-image smoke build.

## Requirements

- JDK 17+
- Redis 6.2+ or Valkey (standalone / Sentinel / Cluster)

## Quick Start (Spring Boot)

Use the [tested consumer dependency alignment](docs/consumer-compatibility.md)
for your Boot line; an application-owned BOM can override the starter's transitive
versions. Release certification checks the resolved runtime, not only this coordinate.

```groovy
dependencies {
    implementation "io.github.cramen:quotaflow-spring-boot-starter:0.1.0"
}
tasks.withType(JavaCompile).configureEach {
    options.compilerArgs.add("-parameters")
}
```

```properties
quotaflow.redis.url=redis://localhost:6379
quotaflow.namespace=default
quotaflow.defaults.expected-instances=1
quotaflow.recovery.deployment-id=default
quotaflow.recovery.members=single
quotaflow.recovery.instance-id=single

quotaflow.policies.llm-provider.scope=global
quotaflow.policies.llm-provider.limit.capacity=10000
quotaflow.policies.llm-provider.limit.refill-amount=10000
quotaflow.policies.llm-provider.limit.refill-period=PT1M

quotaflow.policies.tenant-gold.scope=tenant
quotaflow.policies.tenant-gold.parent=llm-provider
quotaflow.policies.tenant-gold.limit.capacity=1000
quotaflow.policies.tenant-gold.limit.refill-amount=1000
quotaflow.policies.tenant-gold.limit.refill-period=PT1M
```

```java
@RateLimited(policy = "tenant-gold", key = "#tenantId")
public CompletionStage<Answer> chat(String tenantId, Prompt prompt) { ... }
```

Named expressions such as `#tenantId` require Java parameter metadata. For Maven,
set `maven.compiler.parameters=true`; alternatively use positional expressions
such as `#p0`. The explicit single-owner settings above must be replaced with
the complete stable cohort before running multiple instances.

Provision the namespace, fixed cohort and root controllers before activation; see
[conservative recovery and the single-owner setup](docs/conservative-recovery.md).
For deadline, cancellation and resolver execution settings, see
[acquisition lifecycle](docs/acquisition-lifecycle.md).

An exhausted limit produces `429 Too Many Requests` with a `Retry-After`
header and an `application/problem+json` body naming the policy and the fired
level. To queue callers with backpressure, select throttle mode on the policy
and set the annotation's waiting budget:

```properties
quotaflow.policies.tenant-gold.reaction=throttle
```

```java
@RateLimited(policy = "tenant-gold", key = "#tenantId", waitTimeout = "PT2S")
```

For ordinary Java methods the annotation waits synchronously, including when the
business method returns `CompletionStage`. Use the supported Reactor return types
for WebFlux or the Kotlin suspend facade when acquisition must remain nonblocking.

Non-Spring applications use `io.github.cramen:quotaflow-core` plus a store module
(`quotaflow-store-redis`) with the `quotaflow-fallback` coordinator; coroutine applications use
`io.github.cramen:quotaflow-kotlin` for the `suspend` API.

## Observability

`quotaflow-micrometer` bridges every decision and degradation transition to
Micrometer: `quotaflow.decisions`, `quotaflow.utilization`, `quotaflow.degraded`,
`quotaflow.fallback.decisions`, `quotaflow.wait.duration`. A reference Grafana
dashboard and alerting rules ship in the module's `grafana/` resources.
Metric tags always use aggregated key-groups — never raw keys.

## Quota namespaces and migration

Distributed namespaces must be explicitly provisioned before activation. Configure
`quotaflow.namespace` (or the core builder's `namespace`) consistently across the
fleet. See [canonical quota state and migration](docs/canonical-quota-state.md)
for fresh setup, the breaking store SPI migration, coordinated legacy cutover and
rollback. See [numeric limits and state compatibility](docs/numeric-state.md)
for supported rates, immutable algorithm bindings and the coordinated codec upgrade.
A shared policy tree occupies one Redis Cluster slot; capacity planning
must retain its shared ancestor rather than split it into per-user counters.

## Building

Requires JDK 17+ (toolchain); Docker is needed for the Redis-backed tests.

```bash
./gradlew build
```

Modules: `quotaflow-core`, `quotaflow-store-redis`, `quotaflow-fallback`,
`quotaflow-config`, `quotaflow-spring-boot-starter`, `quotaflow-kotlin`,
`quotaflow-micrometer` (published), plus the internal `quotaflow-tck`
(chaos/conformance suite) and `quotaflow-native-smoke`.

## Security

See [SECURITY.md](SECURITY.md) for the vulnerability reporting channel, fix
SLAs (critical 7 days, high 30 days), and the support policy.

## License

[Apache License 2.0](LICENSE)

### Observation and logging contracts

`quotaflow.decisions{result=wait,policy,key-group}` counts initial queue entries,
not completed requests. Retries do not add entries; cancellation preserves the
entry but emits no terminal decision. Wait entries and queue depth belong to the
requested leaf; terminal counters identify the fired level. Traffic and rejection
ratio queries must use only `result=allow|reject` in their denominator.

`quotaflow.tokens.remaining{policy}` is the minimum latest observed remaining
budget across bounded key-groups; `quotaflow.utilization{policy}` is the maximum
paired utilization. Migrate old utilization queries by removing `key-group`.
These gauges are sampled summaries, not live balances or fleet totals. Fallback
samples describe effective local shares. Unknown budgets have no gauge; known
zero capacity has utilization one. Idle samples retain their last observed value.
Configuration changes invalidate old samples without removing another limiter's
meters in a shared registry.

Listener delivery is asynchronous, ordered per listener and bounded. A throwing
listener does not affect quota outcomes or other listeners. Saturation or callback
timeout disables that listener's delivery lane and records a safe diagnostic;
`DefaultQuotaFlow.observationFailures()` must remain zero for complete observation
evidence. `flushObservations()` provides an asynchronous delivery barrier for
checks and shutdown coordination. Close owned facades and metric adapters;
manually wired throttle adapters need `sync()` after configuration reload.

Omitted `@RateLimited` USER keys use authenticated, non-anonymous identity.
Reactive identity is resolved independently for each subscription. An explicit
key expression is authoritative, including null results; shared default keys
require explicit `USE_DEFAULT_KEY`. GLOBAL policies need no identity.

Quotaflow sanitizes its own diagnostics at every log level and its outward
startup/configuration exception chains. Applications must disable independent
Redis wire/protocol payload logs: Lettuce TRACE can print HELLO/AUTH credentials
even with sanitized URI rendering. For Spring Boot, explicitly configure:

```yaml
logging:
  level:
    io.lettuce.core.protocol: INFO
```

Keep this override when enabling DEBUG or TRACE on the root logger. Do not install
transport byte-dump handlers; apply equivalent restrictions to caller-managed
clients and other drivers. The application owns these settings, including runtime
changes. Quotaflow does not change global logging configuration, and its redaction
guarantee does not cover independently emitted third-party wire dumps.

Verification commands, runtime evidence and external consumer fixtures are documented
in [the verification workflow](docs/verification.md).
