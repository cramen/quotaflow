# Quotaflow

Enterprise-grade **distributed rate limiting library for JVM microservices** — a standalone, self-contained library with no framework lock-in in its core.

[![CI](https://github.com/cramen/quotaflow/actions/workflows/ci.yml/badge.svg)](https://github.com/cramen/quotaflow/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

## Features

- **Hierarchical policies**: global → tenant → user → API-key, including external provider quotas (e.g. a shared LLM API quota) as a parent level. Chains evaluate atomically in one Redis round-trip.
- **Two reaction modes**: `reject` (correct HTTP 429 with `Retry-After` and an RFC 7807 body) and `throttle` (bounded priority queue with backpressure and wait timeouts — a "budget valve" for paid external APIs).
- **Degradation, not fail-open/fail-closed**: on Redis failure, automatic fallback to a conservative local limiter (limit ÷ instances) with spike-free recovery via state seeding.
- **Atomic correctness**: Lua token bucket and GCRA only — no read-modify-write, boundary-burst protection, Redis server time for refill math.
- **Dynamic configuration**: hot-reload of policies without restarts, plus a tariff resolver SPI for per-key limits from billing/CRM systems (TTL-cached).
- **First-class observability**: every decision (allow/reject/wait) is metered by policy and key-group; utilization, degradation state, and wait metrics included; reference Grafana dashboard and alerts.
- **Zero-config DX**: Spring Boot starter (`@RateLimited`, servlet + WebFlux) and a Kotlin coroutines facade (`suspend` / `Flow` / DSL).
- **GraalVM ready**: reachability metadata and Spring AOT hints, verified by a native-image smoke build.

## Requirements

- JDK 17+
- Redis 6.2+ or Valkey (standalone / Sentinel / Cluster)

## Quick Start (Spring Boot)

```groovy
implementation "io.quotaflow:quotaflow-spring-boot-starter:0.1.0"
```

```properties
quotaflow.redis.url=redis://localhost:6379

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

An exhausted limit produces `429 Too Many Requests` with a `Retry-After`
header and an `application/problem+json` body naming the policy and the fired
level. Throttle-mode policies instead queue callers with backpressure:

```java
@RateLimited(policy = "tenant-gold", key = "#tenantId", waitTimeout = "PT2S")
```

Non-Spring applications use `io.quotaflow:quotaflow-core` plus a store module
(`quotaflow-store-redis`) directly; coroutine applications use
`io.quotaflow:quotaflow-kotlin` for the `suspend` API.

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
