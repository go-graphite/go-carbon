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
receipt clock and receipt boundaries. Startup replays after the atomically
committed owner cursor before opening listeners. Flush first freezes the heads,
syncs the nonempty streams once, then seals the owners concurrently. Admission
continues into new active heads while the frozen heads seal. One frozen generation
is allowed at a time; a 256 MiB credit budget per owner limits logical queued,
active and frozen frame charges. Credits release after publication. Shared
receiver-batch arenas can retain other owners' frames, so these credits are not
a strict retained-memory bound. A failed journal or seal is reported through
the daemon's existing fatal backend error boundary.
Shutdown drains receivers and responses, flushes, then closes snapshots and DBs.

CacheQuery copies both frozen and active accepted receipts, including original
point bits and arrival order. Find, Info and Fetch currently force a seal before
opening the immutable read view. This is a correctness bridge: a read can wait
for journal sync, reduction and SST publication. A pinned immutable live-head
reader remains necessary to remove that latency.

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
control this isolated harness. `V2_BACKEND_OFFER_1M=1` paces its 32 TCP connections
to approximately 1M points/s; omit it for an unpaced run.
`V2_BACKEND_PROFILES=<directory>` records CPU, mutex and block profiles.

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
* Active heads have per-owner admission credits. Full catalog reconstruction,
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
