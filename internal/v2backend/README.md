# Immutable-parts daemon adapter (experimental)

This package is the first real backend for the restart-only `[v2]` daemon
boundary. It is intended for isolated qualification. It is **not ready for a
production cutover**.

## Data flow

```text
TCP receiver batches / other receivers -> cached canonical-name routing
  -> bounded per-owner receipt batches -> one journal append per batch
  -> active receipt frames (one-second group sync)
  -> sealrollup classic reducer -> portable GCP3 SSTs
  -> one atomic Pebble ingest: archive replacements
       + metric policy/catalog edits + owner journal frontier/apply order
  -> immutable Pebble snapshot + immutable name/glob index -> partsread
HTTP/gRPC serializer -> release snapshot and response memory reservation
```

Owners and databases use the existing daemon options independently. Routing is
by the portable virtual shard of the exact canonical metric name; persistent
keys contain placement group, virtual shard, partition, immutable part UUID and
canonical name. Numeric read IDs exist only inside one immutable snapshot.

The journal records exact original point bits, metric bytes, policy, placement,
receipt clock and receipt boundaries. Metric names and opaque policy/placement
definitions appear once per stream/process dictionary epoch, before references
in the same checksummed journal record. Applies contain varint metric references,
clock/timestamp deltas and exact 64-bit value bits XORed with the previous value
for that name. The PDX1 envelope checksums predictor epochs and payloads; prediction
does not canonicalize NaNs, signed zero or any other value. Independent 64KiB-window
zstd blocks compress each batched record, with a raw fallback when compression
would grow it. Replay limits expansion to the journal's 8MiB payload limit and
validates block checksums before decoding any applies. Record headers still cost
32 bytes, so tiny scalar appends can exceed 20 bytes per point; batching amortizes
that cost. Replay rebuilds dictionaries from the retained epoch prefix, skipping
already committed applies. Old V2B1/V2M1 and PDC1/PDC2 records remain readable.
An older binary cannot decode new PDX1 records. Dictionary and predictor
checkpoints are required before journal trimming can be enabled. Lossless
compression cannot promise four bytes per point for arbitrary high-entropy values.

The restart-only `v2.journal-codec` selector defaults to `"pdx1"`.
`"pdc3"` enables the per-series timestamp/value and full arrival-clock predictor.
Replay dispatches both formats by their record magic regardless of the selected
writer. Encoder state belongs to one ordered stream; the retained epoch prefix
is still required. The PDC3 decoder checks record sequence and prior-record CRC.

Each seal replaces a small binary V2C2 owner header plus the touched metric
frontiers and scope revisions in the same atomic ingest as the SST data. Latest
metric/revision keys replace earlier values; restart scans them once, without an
unbounded chain of checkpoint deltas. Legacy JSON checkpoints migrate atomically
on the first seal, including untouched metrics. Older binaries fail closed on
V2C2 checkpoints, so downgrading requires restoring a compatible store rather
than reopening the migrated directory.

The frozen name catalog shares immutable size-tiered segments. Point-only seals
reuse the complete catalog and glob index; admitting new names merges colliding
size tiers without changing existing reader IDs or old views. Durable identity
continues to use the complete PG/vshard/name key. Scratch retains at most 16MiB of
key/value chunks per owner; oversized grouping/frame/sample buffers are released
after sealing. This limits retained scratch, not total process RSS or live heads.

Startup still completes journal/database recovery and replay before opening
listeners. It does not satisfy the 100ms first-live-append requirement. A durable
admission epoch and process fencing must be available before historical recovery
can safely move into the background. Flush first freezes the heads,
syncs the nonempty streams once, then seals the owners concurrently. Admission
continues into new active heads while the frozen heads seal. One frozen generation
is allowed at a time; a 256 MiB credit budget per owner limits logical queued,
active and frozen frame charges. Credits release after publication. Shared
receiver-batch arenas can retain other owners' frames, so these credits are not
a strict retained-memory bound. A failed journal or seal is reported through
the daemon's existing fatal backend error boundary.
Shutdown drains receivers and responses, flushes, then closes snapshots and DBs.

CacheQuery copies both frozen and active accepted receipts, including original
point bits and arrival order. Find, Info and Fetch perform no seal or journal sync.
An owner exchange pins one coherent sealed snapshot/frontier and copies immutable
pending/active frame headers. It filters receipts already included in that pinned
cursor. Render runs the exact reducer on the read side and combines its detached
slot replacements with the pinned sealed source. The response pin retains both
resources through serialization. No reader acquires a writer mutex. Read-side
reduction and rebuilding find indexes still consume CPU; multiarchive heads may
scan the complete lower-archive seed. Admission budgets reserve reducer input
and replacement workspace before allocation, and reserve occupied seed-map slots
in bounded chunks as they are encountered.

Reads seek at most two physical ring ranges per referenced part. Original
sample timestamps and occupied NaNs are retained, including physical overwrite
semantics. Returned slices belong to the caller; direct callers must invoke
`Series.Release`. HTTP and gRPC retain reservations through serialization.

An existing chunk store is opened read-only and combined with new parts by
`mixedread`. Existing classic `.wsp` history requires offline conversion.
Writes to an existing multi-archive legacy metric are rejected until its exact
physical fine/coarse state can seed the reducer; guessing that state would
violate bit parity.

## Usage

Use a fresh scratch data directory with existing storage schemas and aggregation
configuration, then enable the existing `[v2]` configuration. No additional tuning
options are introduced. Owners, databases and data directory are restart
invariants stored in `V2FORMAT`. Reusing the directory with different geometry
fails. The normal disabled configuration continues using the existing backend.

`TestV2PortableDaemonTCPHTTP` and `TestV2DaemonSIGKILLReplay` exercise the real
factory and TCP receiver. `TestPortableBackendOracle1680` compares 70 schema
shapes, all six aggregation methods and four query widths with classic Whisper.
The opt-in `V2_BACKEND_THROUGHPUT=1` daemon test runs for 120 seconds and reports
backend-accepted points, process CPU, peak RSS and device write bytes; run it
inside an externally imposed CPU and device IO limit. Optional
`V2_BACKEND_DURATION`, `V2_BACKEND_CPUS`, `V2_BACKEND_OWNERS` and `V2_BACKEND_DBS`
control this isolated harness. The single-archive fixture has 1,048,576 distinct
names and advancing timestamps. `V2_BACKEND_CONNECTIONS` (default 64) and
`V2_BACKEND_CARDINALITY` size the sender independently.
`V2_BACKEND_OFFER_1M=1` paces its TCP connections
to approximately 1M points/s; omit it for an unpaced run.
`V2_BACKEND_PROFILES=<directory>` records CPU, mutex and block profiles.
`V2_BACKEND_SENDER_CPUS=<Linux CPU list>` starts the TCP senders in a separate
process on the specified CPUs, excluding them from daemon CPU/RSS accounting.
`V2_BACKEND_SENDER_MAXPROCS` (default 96) overrides the test binary's CPU cap for
that process. `V2_BACKEND_SENDER_CAPACITY=1` selects the independent 15-second
discard-sink ceiling test. Sender and backend CPU lists must be disjoint.
`V2_BACKEND_EXEC_NS=<pre-exec Unix nanoseconds>` reports actual first TCP append
latency from the runner timestamp. The benchmark always prints throughput,
journal bytes/point, CPU/point, RSS and post-sender flush receipts. Five-second
periodic receipts preserve partial results if an external health guard stops it.
V2 TCP admission uses bounded 1024-point batches and flushes complete points before
blocking on a socket read; other backend paths retain their existing batch size.
`TestPortableBackendLiveOracle1680` runs the same classic oracle without Flush;
`TestLiveReadsNeverSealAndSurvivePublication` verifies frozen/active visibility
while sealing is blocked and preserves pinned responses across publication.

## Qualification limits

* This is one initial origin, generation 1. There is no import/delete/recreate,
  replica handoff or distributed incarnation authority. Generation 1 does not
  authorize copying conflicting replicas into this store.
* Per-owner atomic publication is implemented. There is no cross-owner transaction.
* Provenance retains the actual receiver receipt boundaries. Exact render parity
  against classic is established for identical UpdateMany boundaries/clocks;
  equivalence to arbitrary classic cache/persister batching remains unproved.
* Journal trimming, physical retention and superseded-part reclamation are not
  implemented. The journal remains the retained source of original receipt
  envelopes; duplicate raw receipt rows are no longer written into each SST.
* Active heads have per-owner admission credits. Catalog growth and startup reconstruction,
  policy/name cache growth, reducer workspace and retained fine-archive state
  for multi-archive rollups still need host-scale bounds. A single archive
  retains no historical reducer ring in memory.
* Policy and canonical-name metadata are cached per metric. TCP receiver
  batches partition by owner, preserve point apply boundaries, and share one
  journal append per owner batch. Scalar receivers remain supported. This
  adapter does not inherit the standalone core benchmark's throughput.
  Monotonic scaling, settled compaction, live replay, receiver drops and a
  sustained production intake remain qualification gates.
* The 256 MiB response budget covers render work and owned values; complete
  catalog/find response and mixed-reader sizing still need a unified bound.

Dependency packages are composed from their sibling changes as separate commits
so reviewers can distinguish that code from the adapter itself. The deliberate
compatibility delta can be removed when the backend, reducer and read packages
land together with these outstanding gates satisfied.
