# Immutable-parts daemon adapter (experimental)

This package is the first real backend for the restart-only `[v2]` daemon
boundary. It is intended for isolated qualification. It is **not ready for a
production cutover**.

## Data flow

```text
TCP / other receivers -> canonical-name routing -> serial owner
  -> per-owner partlog stream (one-second group sync) -> active receipt frames
  -> sealrollup classic reducer -> portable GCP3 SSTs
  -> one atomic Pebble ingest: archive replacements + original receipt envelope
       + metric policy/catalog edits + owner journal frontier
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
syncs all streams once, then seals the owners concurrently. A failed journal or
seal is reported through the daemon's existing fatal backend error boundary.
Shutdown drains receivers and responses, flushes, then closes snapshots and DBs.

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
inside an externally imposed CPU and device IO limit.

## Qualification limits

* This is one initial origin, generation 1. There is no import/delete/recreate,
  replica handoff or distributed incarnation authority. Generation 1 does not
  authorize copying conflicting replicas into this store.
* Per-owner atomic publication is implemented. There is no cross-owner transaction.
* Provenance retains the actual receiver receipt boundaries. Exact render parity
  against classic is established for identical UpdateMany boundaries/clocks;
  equivalence to arbitrary classic cache/persister batching remains unproved.
* Journal trimming, physical retention and superseded-part reclamation are not
  implemented. Raw receipt envelopes are deliberately retained.
* Active heads, full catalog reconstruction and retained fine-archive state for
  multi-archive rollups still need host-scale memory and CPU bounds. A single
  archive retains no historical reducer ring in memory.
* Each receiver receipt currently incurs policy encoding and a journal append.
  This adapter does not inherit the standalone core benchmark's throughput.
  Monotonic scaling, settled compaction, live replay, receiver drops and a
  sustained production intake remain qualification gates.
* The 256 MiB response budget covers render work and owned values; complete
  catalog/find response and mixed-reader sizing still need a unified bound.

Dependency packages are composed from their sibling changes as separate commits
so reviewers can distinguish that code from the adapter itself. The deliberate
compatibility delta can be removed when the backend, reducer and read packages
land together with these outstanding gates satisfied.
