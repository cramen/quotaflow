# Quotaflow

Enterprise-grade **distributed rate limiting library for JVM microservices** — a standalone, self-contained library with no framework lock-in in its core.

## Features (planned)

- **Hierarchical policies**: global → tenant → user → API-key, including external provider quotas (e.g. a shared LLM API quota) as a parent level.
- **Two reaction modes**: `reject` (correct HTTP 429 with `Retry-After`) and `throttle` (queue with backpressure, timeouts, and tenant prioritization — a "budget valve" for paid external APIs).
- **Degradation, not fail-open/fail-closed**: on Redis failure, automatic fallback to a conservative local limiter with full observability.
- **Atomic correctness**: Lua / GCRA schemes only — no naive read-modify-write, built-in boundary-burst protection.
- **Dynamic configuration**: hot-reload of limits and tariff resolvers without restarts.
- **First-class observability**: every limiter decision (allow/reject/wait) is metered, degradation state is a metric.
- **Zero-config DX**: Spring Boot starter with `@RateLimited`, Kotlin coroutines facade (`suspend` / `Flow` / DSL).

## Status

Early development. See `AGENTS.md` for design rules, module layout, and quality gates.

## Requirements

- JDK 17+
- Redis 6.2+ or Valkey (standalone / Sentinel / Cluster)

## Building

Requires JDK 17+ (toolchain); Docker is needed for the Redis-backed tests.

```bash
./gradlew build
```

Modules: `quotaflow-core` (framework-free policy engine core), `quotaflow-store-redis` (Lettuce-backed distributed counters), `quotaflow-fallback` (local degradation limiter), `quotaflow-config` (dynamic configuration), `quotaflow-spring-boot-starter` (Spring adapter), `quotaflow-kotlin` (coroutines facade), `quotaflow-tck` (chaos/conformance test suite).

## License

[Apache License 2.0](LICENSE)
