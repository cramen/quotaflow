# Numeric limits and quota state

Both token bucket and GCRA use exact whole-token credit plus a sub-token time
remainder. With the library's GCRA burst tolerance, these representations have
the same admission envelope. Algorithm identity remains explicit and immutable.

## Supported envelope

| Quantity | Range |
|---|---|
| Capacity, refill amount, request weight | Integer 1 through 1,000,000,000 |
| Refill period | 1 microsecond through 32 days |
| Nominal time per token | At least 1 microsecond |
| Represented time per token | `ceil(periodNanos / refillAmount)` nanoseconds |
| Full-refill horizon | Capacity times represented interval, at most 32 days |

`Limit` validates all bounds before duration conversion or multiplication.
Static limits, resolver-produced limits and direct store calls use the same
contract. Unsupported counts or durations raise input/configuration errors;
they cannot authorize a fallback grant. A supported weight larger than capacity
is instead an ordinary rejection with no retry schedule, and never enters the
throttle queue or charges an ancestor. Custom stores must preserve this contract.
An unrepresentable derived fallback share receives a schedule-less data rejection,
never an upward-rounded positive budget. This does not certify the complete
recovery protocol, which remains a separate change.

## Arithmetic bound

Let `H = 2,764,800,000,000,000` nanoseconds (32 days) and `T` be the
represented interval. Configuration ensures `C*T <= H`. Refill clamps elapsed
time to `H` before multiplication/addition; the remainder is below `T <= H`.
Consequently progress is below `2*H`, and a quotient candidate correction uses
at most `3*H = 8,294,400,000,000,000 < 2^53`. Whole balance plus refill quotient
is also below `2^53`. A fit-capable request's debt product is bounded by `H`.
Lua checks quotient candidates using integer multiplication/comparison instead
of trusting rounded floating division at an integer boundary. Final millisecond
retry/TTL conversion rounds upward. No repeated evaluation loses a remainder.

Redis timestamps are separate seconds and nanoseconds; production always samples
`TIME`, with its microsecond resolution. Large elapsed seconds saturate before
conversion to nanoseconds. Backward observations retain the last-accounted time;
expiry includes the future accounted offset so expiry cannot create early credit.
A timestamp more than 32 days ahead of server time is rejected as incompatible.
The local monotonic clock uses signed subtraction, including `nanoTime` wrap;
elapsed durations must remain below `2^63` nanoseconds (about 292 years).

Interval quantization adds less than one nanosecond per token, so at the minimum
1,000 ns nominal interval its relative rate reduction is below 0.1%. Refill
saturation discards excess credit; decisions never use absolute epoch nanoseconds.

## Atomic local chains

Local acquisition prepares all cells from one immutable quota-domain version at
one clock sample and publishes with one CAS. Single acquisitions and chains use
the same protocol. A rejected chain publishes normalization/refill only and has
no speculative parent debit to refund. Rejected traffic does not allocate fresh
full cells for unseen child keys. Concurrent changes invalidate preparation
and cause a fresh evaluation. A stalled request owns no admission lock.

A persistent balanced tree shares unchanged branches, limiting each bucket
update to O(log n) new nodes rather than copying the entire domain. Each domain
retains a stable reference with distinct versions even when empty. Snapshot
cleanup uses the same version check and cannot erase concurrent consumption.
Snapshots remain weakly consistent; authoritative recovery still needs its
external admission fence. Empty domain references remain bounded by policy
binding history per namespace. CAS retries provide system-wide progress rather
than a per-caller wait-free guarantee.

## Codec and compatible configuration changes

State version 3 stores algorithm, SHA-256 parameter fingerprint, whole balance,
remainder and split timestamp as explicit decimal integers. The fingerprint
covers algorithm, capacity and represented interval. No plaintext policy limits
or tariff references are stored. This checksum is not encryption of low-entropy
configuration values. A malformed value, unknown version, wrong Redis type or
algorithm mismatch raises `StateCompatibilityException`, never a fresh bucket
or an availability failure. Every chain/seed batch is decoded before any write.

On a compatible parameter change, keep `min(oldWholeBalance, newCapacity)`, drop
the old remainder and unaccounted elapsed time, and begin the new schedule. This
normalization persists even if acquisition rejects. It may temporarily reduce
utilization; increasing capacity does not create a new burst. Old-state universal
codec bounds are checked before applying new capacity/interval-specific bounds.

`PolicyBinding` now includes `Algorithm`; custom store registration must retain
that binding even after policy removal or quota expiry. Reload, restart and
another instance cannot switch algorithms within the same namespace. Seed
`BucketState.limit()` is the target distributed limit, not its scaled local
share. A seed merges only whole credit and uses that target fingerprint.
Fingerprints do not establish version order: recovery epoch/session/configuration
fencing and stale-seed races belong to the subsequent recovery protocol.

## Coordinated transition and rollback

This is a pre-release format and store-SPI break. Old manifests with bindings
that lack an algorithm, and old unversioned bucket values, are incompatible.
There is no rolling mixed-codec writer mode or implicit in-place state reset.
Follow [canonical migration](canonical-quota-state.md): stop all distributed and
fallback admissions, drain every owned source bucket naturally, retain registry
history/ownership evidence, and explicitly provision a fresh target namespace.
For an already canonical source namespace, inventory its canonical keys; the
legacy assessor only recognizes the old leaf layout. Do not interpret an empty
legacy scan as proof that canonical state is drained. Validate the numeric
limits and register algorithm bindings before starting all new writers.

Before new admissions, rollback may occur while writers stay stopped. After new
consumption, stop writers again and drain the new namespace before reopening
old writers. Never delete spent quota or shorten TTL to accelerate a cutover.

## Verification

Independent BigInteger debt oracles exercise both algorithms, fractional rates,
maximum balances/intermediates and large clock origins. Deterministic seeds and
the first failing history prefix make failures reproducible; the local history
property additionally minimizes failures by deleting operations and shrinking
weights/time gaps. Weighted contention also covers the maximum whole balance. Test-only time
injection replaces the clock provider in the loaded production Lua body, not
its arithmetic. Additional tests cover malformed last-chain members, seed batch
atomicity, parameter transitions and backward clocks. Real-time conformance runs
on Redis 6.2, Redis 8.10, Valkey and Redis Cluster.

Measurements follow [benchmark evidence](benchmarks.md); use
`-PbenchmarkAlgorithm=TOKEN_BUCKET` or `-PbenchmarkAlgorithm=GCRA` with each
module's `benchmarkGate`. The fallback module benchmarks the actual wrapper
through `DefaultQuotaFlow`. Retain raw results separately per algorithm and
profile; these numbers are diagnostics unless an explicit comparable baseline
is supplied. Repeated-run release certification is a separate quality-gate task.
