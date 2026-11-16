# Core snapshot reads

This experimental L4 adapter consumes the L3 core archive format. It ports the
immutable publication, physical-slot resolution, query admission and render code
from the [earlier parts reader](https://github.com/go-graphite/go-carbon/pull/1407); the durable publisher and mixed legacy reader
remain separate [publisher](https://github.com/go-graphite/go-carbon/pull/1429)
and [mixed reader](https://github.com/go-graphite/go-carbon/pull/1414) work because their old portable-manifest and legacy-store
interfaces do not fit the core-first snapshot boundary.

```go
reader, err := v2read.New(shards, 1<<30, 64<<20)
// Handle err.
engine, err := v2core.Open(root, config, v2core.Hooks{
    PublishBatches: reader.Publish,
})
// config.Rollups must be true. Submit the original applications with
// engine.AppendBatches / AppendBatchesPipelined, preserving Order and Clock.
series, err := reader.Fetch(ctx, name, from, until)
// series implements Values(), FromTime(), UntilTime(), Step(); Metric carries
// the aggregation method, XFF and retention ladder.
```

`FetchMany` accepts a set of names, range, common explicit clock and callback.
Every owner generation is pinned once per request. A cross-owner request has
independent owner frontiers; it does not claim a global transaction. Output is
borrowed until the callback returns. `Fetch` copies its result so it can outlive
the pin and satisfies carbonserver's existing structural timeseries interface.
The live carbonserver listener still accepts a concrete `chunkstore.Store`;
selecting this adapter in that listener and configuring L4 in the application
are not implemented in this scoped package.

The core transfers its immutable Pebble snapshot, sealed generation number and
original pending applications after each owner poll, after a successful seal,
and at open. This is an as-of view of applications consumed by core; producer
queues and the daemon's not-yet-drained journal group are not exposed. The descriptors are detached; their already detached point/policy
arenas are immutable. The callback performs no archive scan or rollup. Atomic
references let readers retain a retired view without a writer mutex. A sealed
index is shared between pending-only generations, and its snapshot closes only
when all such generations and reader pins drain. Stop producers, close the
reader, finish in-flight fetches, then close core; later core handoffs are closed
immediately. Do not close the underlying databases under an in-flight fetch.

A reader lazily builds `v2core.RollupSnapshot`, a directory of immutable part
metadata. It seeks over archive values rather than scanning all points. Later
sealed generations reuse existing part metadata. Sealed reads seek directly to
the requested name, archive and one or two physical ring-slot ranges in each
relevant part. A predecessor seek includes a dense run beginning before the
range. Singleton decoding uses stack values; dense decoding uses the shared
`v2part` reader without changing its format or interfaces.

An immutable range-result cache carries resolved slots and part identities
across generations. Only added parts are decoded; absent parts invalidate cached
results. It retains at most eight concurrent generations per range and 64 entries
total, with a conservative byte limit of the smaller of 16MiB and one eighth of
the reader budget per owner. Generations share this cache without retaining old
snapshots or metadata objects. All cache locks are read-side only. The writer publication callback never acquires them.
The read budget conservatively charges the whole directory and maximum cache
capacity while rendering; shared objects can be charged more than once.

For a metric with pending applications, the reader loads the sealed state of
that name's archives for the existing canonical reducer, then resolves greatest
replacement order **before** time filtering. The same bounded cache avoids
rescanning historical parts for this state. Original application boundaries and
admission clocks are never inferred from point timestamps. This preserves
occupied NaNs, signed zero, wrapped invalidations, exact XFF edges and archive
choice. Pending reduction is read work and never changes durable reducer state.
Expiry and incarnation reset authority come from the same pinned metadata.

Directory construction and retained metadata still scale with the number of
parts and names. Pending materialization and disk files retained by pinned
Pebble snapshots need independent accounting before high-cardinality production
qualification. Legacy Whisper overlays and online fallback are handled by the
application's adapter rather than this package.

`TestClassicPendingAndSealed` drives actual core plus actual classic Whisper,
54 schema/method/XFF combinations and 6,480 pending/sealed queries. It covers
sparse data, late corrections, original groups, wraparound, advancing/backward
clocks, archive edges, NaN payloads, infinities, signed zero and subnormals.
Publication lifetime, reader admission, cancellation and blocked-consumer writer
progress tests are also carried forward or exercise the real core hook.

An explicit native host trial is available with `V2READ_HOST=1 go test
./internal/v2read -run '^TestHostConcurrent$' -v -timeout=180s`. It uses six
alternating unrestricted producer arms (zero/four readers), fresh L3 stores,
full core completion timing, raw/archive-capable policies, per-arm read p50/p99,
process census and load averages. The timed query workers alternate fine and
coarse archive ranges. Set TMPDIR to a private host root, constrain
CPU affinity and retain every row; shared results remain provisional. This
harness measures core ingestion rather than application TCP/journal capacity.
Set `V2READ_SECONDS` to 1–60 to select each arm's duration (default 4). It records
every completed request's latency and uses actual reader completion time for
query throughput. The fixture has four names, four producers and four owners,
with repeated timestamp corrections; it does not qualify representative
high-cardinality/history workloads.
