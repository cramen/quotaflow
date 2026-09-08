# AGENTS.md

Guidance for AI agents and contributors working in this repository.

## Project Overview

**Quotaflow** is an enterprise-grade **distributed rate limiting library for JVM microservices** — the second module of the **Tiercache** family of resilience primitives.

Recommended Maven coordinates: groupId `io.quotaflow`, artifact prefix `quotaflow-*`.

Quotaflow is an **application-layer** library (not an API gateway, not a low-level Redis primitive). It provides:

- Hierarchical rate limit policies (global → tenant → user → API-key), including external provider quotas (e.g. a shared LLM provider quota) as a parent level.
- Two reaction modes: **reject** (correct HTTP 429 + `Retry-After`) and **throttle** ("budget valve": queue with backpressure, timeouts, tenant prioritization, quota visibility).
- **Degradation instead of fail-open/fail-closed**: on Redis failure, fall back to a conservative local limiter with mandatory observability.
- Hot-reloadable dynamic configuration (tariff/plan resolution via SPI, no restarts).
- First-class observability of every limiter decision (allow/reject/wait) plus degradation state.

Source documents (Russian, authoritative): `research/ТЗ_rate_limiting_enterprise.md` (the spec, requirements F-xx/N-xx/S-xx/TR-xx) and `research/Rate_Limiting_анализ_проблемы_и_обоснование.md` (problem analysis, pains Б1–Б8).

## Language Policy (MANDATORY)

**All project artifacts are written exclusively in English:**

- README files, documentation, JavaDoc/KDoc, code comments
- Commit messages, PR descriptions, issue text
- Public API names, configuration property names, log messages, exception messages
- Specs and change artifacts under `openspec/`

The only exception is the `research/` folder, which holds the original Russian source documents. Conversational replies to the user may follow the user's language, but nothing written into the repository may be in Russian.

## Module Layout

| Module | Purpose |
|---|---|
| `quotaflow-core` | Policy Engine (limit tree, decision composition), storage and key-resolver abstractions, reaction modes |
| `quotaflow-store-redis` | Distributed counter storage: token bucket and GCRA via atomic Lua scripts |
| `quotaflow-fallback` | Conservative local limiter for degradation mode |
| `quotaflow-config` | Dynamic configuration, hot-reload, tariff resolver SPI |
| `quotaflow-spring-boot-starter` | `@RateLimited` annotation, filter/interceptor, auto-configuration, 429 semantics |
| `quotaflow-kotlin` | `suspend`/`Flow`/DSL facade, non-blocking semantics |
| TCK module | Chaos/conformance tests (races, degradation, boundary burst, dynamic config) |

Explicit non-goals: no own Redis client, not an API gateway / service mesh, no strong-consistency quotas with distributed transactions, no admin UI (SPI/metrics/REST inspection only).

## Hard Design Rules (do not violate)

1. **Correctness by default.** Only atomic schemes (Lua / GCRA). Naive GET/INCR read-modify-write patterns are forbidden by construction. Fixed window is never a default (boundary-burst vulnerability); if selectable, it must carry an explicit warning.
2. **No binary fail-open/fail-closed as default.** Redis failure must trigger degradation to a local conservative limiter (distributed limit / expected instance count), with metric `quotaflow.degraded=1`, a log event, and a fallback decisions counter.
3. **Redis server time** (not client clocks) is used for refill calculations; protect against clock skew between instances.
4. **Metric cardinality control.** Raw limit keys are never metric tags — only aggregated `key-group`. Raw keys are never logged above DEBUG (risk of leaking tenant/user IDs).
5. **No blocking on hot paths** in reactive/coroutine modes; virtual-threads-safe (no pinning).
6. **Zero required dependencies in core** besides SLF4J and `tiercache-core`.

## Technology Baseline

- JDK 17+ (certification targets 21/25); virtual-threads-safe
- Redis 6.2+ and Valkey; standalone / Sentinel / Cluster; managed cloud offerings
- Spring Boot (two latest lines supported); optional Spring Cloud Gateway filter integration
- Kotlin coroutines for `quotaflow-kotlin`
- Micrometer / OpenTelemetry for metrics and tracing
- Testcontainers for integration/chaos tests; JMH for benchmarks; PIT for mutation testing
- GraalVM reachability metadata + Spring AOT support

## Key Metrics (contract names)

`quotaflow.decisions{result=allow|reject|wait, policy, key-group}`, `quotaflow.tokens.remaining{policy}`, `quotaflow.wait.duration`, `quotaflow.degraded`, `quotaflow.fallback.decisions`, `quotaflow.utilization{policy}` (0..1).

## Testing and Quality Gates

- Public TCK scenarios TR-01..TR-10 (race condition, boundary burst, Redis degradation, recovery, dynamic config, hierarchy, throttle, weighted requests, virtual-thread stress, metric cardinality) must stay green.
- Core branch coverage ≥ 90%; mutation testing (PIT) ≥ 80% on correctness paths (F-20..F-32).
- Benchmarks N-01..N-03 are blocking: ≤ 1 ms p99 added latency on allow (L2 hit), ≥ 50k decisions/s per instance, ≤ 0.01 ms p99 in fallback mode; a > 10% regression blocks the build.
- Lua scripts must be covered by property-based race tests.

## Security and Supply Chain

SBOM (CycloneDX) per release, signed artifacts (Sigstore + PGP), reproducible core build, public SECURITY.md with fix SLAs (critical 7 days, high 30 days), release blocked on reachable CVEs. Never log secrets; no sensitive tariff/quota configuration in Redis in plaintext.

## Workflow

This repository uses the **OpenSpec** workflow (`openspec/`). Use the project skills (`openspec-propose`, `openspec-apply-change`, `openspec-update-change`, `openspec-sync-specs`, `openspec-archive-change`) for proposing, implementing, and archiving changes. Requirement IDs (F-xx, N-xx, S-xx, TR-xx) from the spec in `research/` are the traceability anchor — reference them in specs, code, and tests.
