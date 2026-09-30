# Conservative degradation and recovery

Distributed operation requires a provisioned namespace, a fixed cohort, a stable
instance ID for every member, and exclusive ownership of each slot. A Redis
connection alone is not evidence that a process can spend a local share.

## Shares and startup

For capacity `C`, represented full-policy emission interval `I`, and cohort size
`N`, a local guard has capacity `floor(C/N)` and emission interval `N*I`.
Both token bucket and GCRA use this rule. New or uncertain guard state starts
empty. Fractional credit is retained conservatively; no positive capacity is
invented when `C < N`. Weights exceeding the share receive a data rejection
without a retry schedule. Healthy distributed operation can still serve them.

A new process that cannot validate ownership receives zero-credit rejections.
The Spring starter keeps a reconnectable transport and reports degradation,
including when the Redis driver is missing. Set
`quotaflow.fail-on-redis-missing=true` to fail startup for a missing driver or
unreachable endpoint. An explicitly supplied `RateLimitStore` bean still
replaces automatic wiring.

```properties
quotaflow.namespace=payments
quotaflow.defaults.expected-instances=2
quotaflow.recovery.deployment-id=payments-production
quotaflow.recovery.members=worker-a,worker-b
quotaflow.recovery.instance-id=worker-a
```

The second process uses `worker-b`. The member list must contain exactly `N`
unique IDs, including the current instance. Member order does not matter. The
single-instance defaults are namespace/deployment `default`, member `single`,
and `N=1`. These values describe a genuinely single-owner deployment; copying
that configuration into several processes does not create several eligible
shares. Duplicate ownership is rejected.

Namespace, deployment label, distributed endpoint, recovery mode, expected instance count, cohort and own ID are
startup-only. A reload changing any of them is rejected before policy
publication or applied hooks. A coordinated restart alone does not authorize a
smaller fleet declaration: old owners must be stopped and their debt drained
before explicit membership maintenance.

## Provisioning and the single-owner quick start

Use administrative connections before starting runtime owners. Runtime code
never creates missing manifests or repairs missing controllers. Start with the
namespace procedure in [canonical quota state](canonical-quota-state.md), then
register every policy binding and provision one controller for each root tree.
For a compiled `PolicySet policies` and an administrative connection:

```java
String namespace = "default";
RecoveryCohort cohort = RecoveryCohort.single();
String incarnation = "initial";
RedisRateLimitStore adminStore = new RedisRateLimitStore(connection, RedisStoreConfig.defaults());
List<PolicyBinding> bindings = policies.policies().stream().map(policy ->
    new PolicyBinding(new QuotaDomain(namespace, policies.rootPolicyId(policy.id())),
        policy.id(), policy.scope(), policy.algorithm())).toList();
adminStore.registerPolicies(bindings).toCompletableFuture().join();
RedisRecoveryController admin = new RedisRecoveryController(connection, Duration.ofSeconds(5));
admin.provisionCohort(namespace, cohort, incarnation, true, true).toCompletableFuture().join();
for (QuotaDomain domain : bindings.stream().map(PolicyBinding::domain).distinct().toList()) {
    admin.provisionDomain(domain, cohort, incarnation,
        policies.recoveryFingerprint(domain.rootPolicyId()), true, true).toCompletableFuture().join();
}
```

The boolean arguments attest to externally established writer quiescence and
drained debt. They are not checks performed by Redis. Do not set them based on
connection loss, a timeout, or an unresponsive process.

The default Spring starter can now enroll `single` and finish the initial
barrier automatically. In a non-Spring application, use an owned safe transport
and the same coordinator:

```java
RedisRateLimitStore redis = RedisRateLimitStore.connect(url, RedisStoreConfig.defaults(), Duration.ofSeconds(2));
FallbackRateLimitStore store = new FallbackRateLimitStore(
    redis.recoveryPrimary("default", Duration.ofMillis(100), true),
    RecoverySettings.single(), listeners);
DefaultQuotaFlow flow = DefaultQuotaFlow.builder(policies, store).build();
// Close store before redis when the application stops.
```

Provisioning does not grant quota. All declared members must participate before
pooled healthy operation starts. Requests during this initial barrier can be
rejected. An idle owner participates in the background; an absent owner never
counts as ready merely because a deadline elapsed.

## Versioned dynamic quotas

Redis-backed dynamic facades and coordinated fallback require a
`VersionedLimitResolver`. The provider publishes a complete immutable
`LimitSnapshot` for reference/key-group lookups. Its positive revision is ordered
by the shared tariff authority, persists across provider restarts, and is common
across the cohort. Do not derive it from an application clock or an instance-local
counter. The same revision must have the same effective content. To roll a tariff
back, publish its previous values under a larger revision.

```java
AtomicReference<LimitSnapshot> published = new AtomicReference<>(
    new LimitSnapshot(1, Map.of(
        new LimitSnapshot.Key("enterprise", "principal"),
        new Limit(100, 100, Duration.ofMinutes(1)))));
VersionedLimitResolver resolver = () -> Optional.ofNullable(published.get());

// Build the entire next view first, then publish it atomically.
published.set(new LimitSnapshot(2, Map.of(
    new LimitSnapshot.Key("enterprise", "principal"),
    new Limit(80, 80, Duration.ofMinutes(1)))));
```

The example revisions 1 and 2 represent values read from the shared authority,
not counters incremented independently by application instances.

Snapshot retrieval must be a fast in-memory operation. Load external billing or
configuration data outside the acquisition path, construct the immutable view,
then publish it. An empty `Optional` means no trustworthy view is available;
missing snapshot entries are unresolved limits. Neither case fabricates quota.
`CachingLimitResolver.wrap` preserves a versioned provider's whole snapshot
capability; it does not flatten it into independently cached TTL entries.
Legacy `LimitResolver` implementations remain valid for local/non-coordinated
stores, but cannot activate a fenced dynamic policy through the facade.

Every evaluation uses one snapshot, including static ancestors sharing a domain
with dynamic descendants. Its revision/digest accompanies the full chain. Redis
checks that identity before acquisition or seed normalization. The controller
retains a resolver revision high-water mark even while a domain becomes static
or retired, and across planned cohort replacement. Removing and re-adding a
policy cannot make an older tariff current again. A provider revision reused
with different content is a compatibility failure.

An eligible local owner can adopt a newer snapshot while Redis is unavailable by
fencing and draining the old local view. It preserves only conservatively
clamped whole credit and discards elapsed/fractional credit across the revision
transition, including parameter ABA. It cannot revive a guard already retired
by a completed barrier. An unresolved READY owner remains quiesced even when a
new snapshot offers a larger limit. A member that learned a newer authoritative
view cannot bypass it with stale local parameters after another transport failure.

All cohort members must declare the same quota-affecting view of each shared
root and obtain the same provider snapshot for full recovery. Idle members still
participate. A coordinated store represents one complete serving policy set;
independent facade configurations must not overwrite each other's view on that
store. Keep the canonical namespace/root identity for genuinely shared quotas.

## Recovery protocol

Each canonical root domain routes its whole hierarchy together. Persistent
same-slot controller metadata includes cohort incarnation, session ownership,
recovery epoch, dispatch generation, policy configuration version, provider
snapshot identity/high-water mark and phase.
Every Redis acquisition and seed checks this metadata atomically before quota
mutation. Missing or incompatible metadata is a configuration/state failure,
not permission to bypass the distributed limiter.

1. `GATHER`: participants fence their local domain, drain admitted work and
   seed at most their own conserved whole balance. Seeding never multiplies
   that balance by `N`. Existing smaller distributed balances win. A joined
   participant spends its local guard before each primary attempt.
2. `DRAIN`: after all joins, Redis advances the dispatch generation. No quota
   acquisitions are permitted. Participants perform final reconciliation and
   publish readiness for the exact epoch, session and configuration.
3. `NORMAL`: only the all-member barrier retires guards and permits pooled
   primary operation. A persistent retirement watermark prevents a member
   that missed this transition from reviving credit in a later outage.

A bounded unsuccessful drain can atomically abort to `GATHER`, advancing the
dispatch fence and invalidating old readiness. A READY owner stays quiesced
until it observes an authoritative completion or abort. Lost outcomes never
justify guessing that an operation was rolled back.

A possibly executed request cannot receive a second fallback grant. Pending
attempts and consumed bucket metadata have explicit bounds. Saturation rejects
before untracked dispatch or local consumption. Idle metadata can expire only
after its safe full-refill horizon plus one attempt-timeout grace period, so ordinary refill waiters are not raced at their due time; unresolved dispatches remain retained until
acknowledgment or authoritative fencing. Recreated local state is empty.

Policy reloads normalize even untouched tracked cells using captured current
full-policy metadata. An unavailable tariff defers recovery. Removed policies
retain their last validated metadata until reconciliation or safe expiry.

`RecoveryPending` exposes a generation-specific shared readiness signal.
Cancelling a caller's view cannot cancel the shared signal, and a wake-up does
not grant quota. A positive throttle acquisition waits in its bounded policy
queue and re-evaluates the complete chain when readiness changes, retaining its
original deadline. Nonwaiting/reject calls return a schedule-less rejection.
See [acquisition lifecycle](acquisition-lifecycle.md) for cancellation and timeout semantics.

## Restart, replacement and scaling

The runtime uses a fresh ownership nonce. A restarted process cannot claim an
occupied ID merely because the old process disconnected. Reusing a nonce through
the low-level enrollment API requires proven continuity of the same owner and
its accounting; it is not a restart recovery shortcut. Current automatic wiring
does not persist local ownership/guard state across JVM restarts.

For replacement or scaling, stop every old local and distributed writer,
establish that old debt has drained, and call `RedisRecoveryController.replaceCohort`.
Supply the expected incarnation, the complete set of provisioned domains
(including retired roots), the replacement cohort and a unique maintenance token.
After an uncertain result, retry the identical operation with the same token.

The operation advances each domain fence before reopening namespace enrollment,
preserves canonical quota keys and balances, and produces a new monotonic
incarnation. It does not silently shrink the cohort. Start the new owners with
matching settings; they enroll with fresh nonces and complete a new barrier.
Old sessions and queued old commands cannot mutate quota state. Redis fencing
cannot stop an offline JVM from running local code; external quiescence is
therefore essential.

## Custom adapters and migration

The previous `FallbackRateLimitStore(BatchRateLimitStore, StateSeeder, ...)`
constructors now reject unsupported wiring. A no-op seeder cannot authorize
recovery. Use `RecoveryPrimary` plus `RecoverySettings`; custom adapters must
implement non-consuming probes, validated ownership, atomic context checks,
bounded dispatch and no acquisition replay.

Owned Lettuce transports reject disconnected submissions, bound queues and
command timeouts, and suppress replay. Caller-owned standalone or Cluster
connections must provide equivalent settings and explicitly attest that
acquisitions cannot be replayed. `RedisRecoveryPrimary` owns neither the
connection nor the store. `ReconnectingRecoveryPrimary` owns the connection
resources returned by its factory and closes superseded or late completions.

A custom store declares `requiresVersionedLimits()` when its dynamic inputs need
snapshot proof. Recovery-aware stores are atomic batch stores; one-level calls
use a one-element chain. Plain Redis store SPI methods require `bindRecoveryContext` with a validated
context. They do not fetch a new healthy context implicitly. Prefer the fallback
coordinator, which passes captured contexts explicitly. A bound GATHER context
does not authorize an unguarded primary acquisition; stale contexts return
pending. Context-free seeding is no longer a recovery capability. Advanced dynamic callers
must carry the captured snapshot revision/digest in every `LevelRequest` and use
a matching controller context; copying current controller counters onto an older
snapshot does not authorize that snapshot.

`maxSeedEntries` in existing Spring properties now limits retained tracking
entries, not a truncated snapshot. Overflow cannot skip consumed buckets.
`failureThreshold` and `maxOpenDuration` are legacy fallback knobs; coordinated
routing stops primary admission on the first uncertain failure and uses bounded
control retries. Direct integrations should use `RecoverySettings`.

## Scope of guarantees

The continuously degraded fleet respects its divided burst and refill envelope.
The coordinated recovery barrier prevents extra credit during staggered
recovery. It does not provide strong global quotas across an arbitrary later
selective partition after healthy pooled consumption has resumed, nor recover
acknowledged state lost by Redis durability/failover. These are distinct from
lost command responses, which remain conservatively accounted.

Degradation remains visible for local, guarded, unenrolled and quiesced routes.
The adapter emits `quotaflow.degraded`, fallback decision counters and sanitized
state-transition logs. Raw quota keys and credentials are not metric tags or
ordinary log fields.
