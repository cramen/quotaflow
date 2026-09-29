# Canonical quota state and namespace migration

A quota bucket is identified by deployment namespace, root policy ID, policy ID,
scope and raw resolved key. The requesting leaf never changes an ancestor's
identity. Direct parent acquisition, child chains, snapshots and seeds all use the
same `BucketIdentity`.

## Placement and capacity planning

Redis bucket keys have the form `qf:v2:{<domain-sha256>}:<bucket-sha256>` and are
137 ASCII bytes regardless of raw-key length. UTF-8 fields are length-prefixed
before hashing; delimiters and user-supplied hash-like strings are ordinary input.
The domain hash includes the namespace and root **policy ID**, not a raw tenant
or user value. All connected descendants share that root's Redis Cluster slot.
Separate trees distribute independently. Splitting a shared parent into per-leaf
keys would change the quota, so adding Cluster shards cannot parallelize one
shared root's atomic counter. Benchmark the actual shared-root workload when
sizing that shard.

`qf:v2:{<domain-sha256>}:control` is reserved for recovery coordination. This
identity change defines its address and same-slot placement. Runtime session and
epoch fencing are implemented by the separate recovery change; neither epochs
nor sessions change bucket keys.

## Configuration and store API migration

Set the namespace through `DefaultQuotaFlow.Builder.namespace("orders")` or
`quotaflow.namespace=orders` in Spring. It is construction-time configuration;
changing targets requires the migration procedure below, not a policy reload.
The default namespace is `default` and still requires explicit Redis provisioning.
Local-only stores have in-memory bindings for their lifetime.

Replace string store keys with explicit values:

```java
QuotaDomain domain = new QuotaDomain("orders", "provider");
BucketIdentity tenant = new BucketIdentity(domain, "tenant", Scope.TENANT, "acme");
store.registerPolicies(List.of(PolicyBinding.of(tenant))).toCompletableFuture().join();
store.tryAcquire(tenant, limit, Algorithm.TOKEN_BUCKET, 1);
```

`LevelRequest` and `BucketState` now carry `BucketIdentity` in `storageKey()`.
`StateSeeder.seed` accepts `QuotaDomain` and snapshots, not a leaf key. Validate
all seed identities against that domain before writing any bucket. Code that
previously parsed a scope/policy from a string uses the typed accessors instead;
raw keys remain excluded from telemetry and non-DEBUG logs. Identity `toString`
redacts the raw key, but the accessor is intentionally available to store code.

Custom stores implement `registerPolicies(List<PolicyBinding>)` in addition to
acquisition. Validate the complete candidate before policy publication, retain
removed-policy bindings, and reject scope/root reassignment and registration
budget overflow as `PolicyConfigurationException`. Distributed implementations
must perform compare/register atomically in their authoritative backend. A local
store can use `LocalPolicyBindings`. The compilable delegate example is
`quotaflow-core/src/test/java/io/quotaflow/core/store/CustomStoreMigrationTest.java`.

Register before admitting business traffic; the facade does this at construction
and replacement. A failed authoritative registration rejects configuration
publication. Fallback can continue serving already validated identities during an
outage, but a new/unverified identity cannot gain local credit merely because its
registry is unreachable. Direct-store lazy activation reports an identity
configuration error until validation succeeds.

The default registry budget is 4096 policies per namespace, including removed
identities. An over-budget candidate changes neither registrations nor the active
policy set. Existing policies continue serving. Binding history is not evicted;
compaction or a changed namespace budget requires quiescence and a drained new
namespace. Capacity/refill edits keep the same identity; scope/root changes and
remove/re-add under another root are rejected.

## Explicit fresh provisioning

Use `RedisNamespaceAdmin` only on a control-plane thread. It is synchronous,
bounded and owns no connections. Constructors for standalone and Cluster
connections have a 30-second default operation budget; an explicit positive
budget up to one hour can be supplied for larger inventories.

```java
try (var connection = client.connect()) {
    RedisNamespaceAdmin admin = new RedisNamespaceAdmin(connection);
    admin.provisionFresh("orders", true, 4096);
}
```

The `true` argument explicitly attests this is a new deployment with no preceding
quota state. It is not a way to rename or reset a spent deployment. Acquisition,
configuration reload and reconnection never provision a namespace implicitly.
An existing manifest is never overwritten, including after a timed-out attempt
whose outcome is unconfirmed. Verify the existing state before retrying an
administrative operation. Missing/corrupt manifests are configuration failures,
not transient availability failures that authorize a local bypass.

## Legacy cutover rehearsal

1. Inventory the actual standalone server or **all primary Cluster shards** and
   every logical legacy policy. Review the server IDs returned by `primaryIds()`
   against the deployment inventory. Declare policies belonging to unrelated
   deployments explicitly as excluded. Unknown or ambiguous legacy ownership
   blocks readiness.
2. Stop every legacy writer, direct store client and local fallback admission.
   Keep them stopped through assessment, provisioning and deployment; freeze
   topology/slot changes for this maintenance interval as well. Begin/end topology
   checks detect observed changes but are not a lock against an administrator.
   The tool
   cannot prove that an offline application stopped; the quiescence attestation
   is an operator obligation, not a distributed lock honored by old versions.
3. Construct `LegacyInventory(ownedPolicies, excludedPolicies,
   reviewedPrimaryIds, true, true, true)`. The flags attest stopped writers, a
   complete inventory and independently proved drainage of all local fallback
   debt. Redis scans cannot inspect another process's local state. Stop/drain
   retry timers and pending commands too; unknown local debt blocks cutover. `assessMigration` scans every primary and returns counts,
   the maximum remaining TTL and server IDs, never raw keys. It verifies stable
   primary membership, process incarnations and slot ownership across the scan.
4. Wait for natural expiry of every affected bucket. Positive TTL blocks
   cutover; a missing expiry, unknown ownership or unstable/incomplete topology
   also blocks it. Do not delete spent keys, reset counters, omit a shard or
   infer that the whole deployment is empty from one absent key. A key without
   expiry requires an independently established drain proof before its legacy
   retention can be repaired; this tool does not guess one or delete that key.
5. Call `provisionMigrated("orders", 4096, inventory)`. It reassesses immediately;
   an old successful report is not sufficient. Only a completely drained,
   quiescent inventory can create the ready manifest. Preserve aggregate
   assessment output and the deployment/quiescence record as cutover evidence.
6. Deploy canonical writers together, register the whole policy set, and check
   that two leaves collectively exhaust a capacity-one parent before opening
   general traffic. There is no mixed-layout rolling or dual-write phase.

A legacy inventory targets the old `{leaf}:policy:scope:raw` layout. It is not a
proof that an arbitrary canonical source namespace has drained. Keep namespace
manifests and identity inventory backed up, and restrict manual Redis mutation.

## Rollback

Before any canonical consumption, rollback can occur while all traffic remains
stopped. After canonical traffic has consumed quota, stop all canonical and
fallback writers first. Inventory every canonical bucket in each source domain
on all primaries using the recorded namespace/root bindings and `singleKey` or
the domain prefix; ignore only reserved persistent metadata such as `:control`.
Require every quota bucket's natural drain/expiry before reopening legacy
writers. Any positive TTL, missing expiry, missing ownership metadata or uncertain
server/slot inventory blocks rollback. Never feed canonical state to the legacy
assessor and treat its lack of legacy matches as approval. If drain cannot be
proved, remain on the current layout with admissions stopped until the condition
is resolved. No automatic cleanup grants another full bucket.

The automated migration tests rehearse rejection with live debt, natural expiry,
explicit provisioning and preserved shared consumption in disposable Redis.
Topology tests exercise the same assessment across actual Cluster primaries.

## Shared-root benchmark profile

Both distributed JMH metrics now evaluate the same two-level hierarchy: one
provider bucket shared by 256 tenant buckets. The throughput workload asserts
that every measured decision allows; rejections cannot inflate allow throughput.
The provider identity is constant across tenants and both metrics use real
canonical keys.

The default setup owns a disposable Redis 6.2 container. For a dedicated Linux
benchmark environment, `QUOTAFLOW_BENCHMARK_REDIS_URL` selects an externally
managed benchmark Redis instead; each trial provisions a unique namespace and
the benchmark does not stop the external server. Retain JMH JSON with JDK, image,
CPU/memory and network-topology information. A macOS-to-Docker-VM route and a
same-VM Linux bridge are different measurement profiles and must not be treated
as an isolated code-regression comparison. Absolute speed thresholds do not block this change. The old baseline measures
a different workload and cannot certify a regression comparison. See
[benchmark verification](benchmarks.md) for diagnostic and comparison modes.

A diagnostic Linux ARM64 run on September 29, 2026 measured about 46,020
decisions/s and 0.772 ms p99 (JDK 17.0.20; Redis 6.2; Docker VM with five
vCPUs and 8,232,890,368 bytes RAM; isolated bridge; eight threads; one fork;
two 10-second warmups and three 10-second measurements). These figures describe
that environment, not guaranteed deployment capacity or release certification.
Raw JMH and environment reports are retained locally under
`quotaflow-store-redis/build/reports/identity-benchmarks/linux-constant-encoding/`.
