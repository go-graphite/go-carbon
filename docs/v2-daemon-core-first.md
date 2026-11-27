# Core-first daemon experiment

The numeric immutable-part engine is the storage path. This experiment measures
small additions to that path without translating points into a second key layout.
It is an offline harness, not a replacement production daemon.

## Package boundaries

`internal/v2daemon` owns independent, single-caller producers. Each producer owns
its name catalog, pending rows, journal file and cursor. No mutex is
shared by producers. `internal/v2core.Engine.AppendPipelined` copies named-layer numeric rows into
the existing bounded owner queues. The numeric L0 control keeps synchronous seals
through `Append`. The core retains its own sorting, immutable SST
creation, monotonic parts, snapshots and durability frontiers.

`internal/partlog` owns the durable name catalog.
`NameCatalog.InternBatch` hashes each name once before probing its single
collision-safe index. Hash slots refer to canonical catalog metadata; complete
name equality decides identity even on full-hash collisions. Compact catalog
entries contain names and definitions, without unused predictor state.
`InternHashed` also accepts a hash already computed by a routing caller. The
current harness routes by numeric ID and does not hash names for core routing.
Metadata is appended only for unseen names; changed definitions are rejected.
Stream-local catalog IDs are
mapped bijectively to core IDs as `(localID-1)*producers + producer + 1`.
Producer count and name-to-producer ownership must remain stable. IDs are scoped
to this experiment, not portable replica identities. A future receiver must
route each name to the same producer; the fixed fixture supplies that ownership.

One placement-group directory sits above the untouched core databases. The
fixture uses a single placement group. The numeric core API does not yet expose
a placement-group/routing-vshard key contract: moving groups as standalone SSTs
across differently sized clusters is therefore **not implemented or qualified**.
The core owner must publish that contract before a production multi-group daemon.
No caller may invent a competing key encoding in this adapter.

```text
name + exact point -> producer-local catalog -> numeric row
                     PDC4 encode -> pending group
                     catalog sync -> journal append + sync
                     core FIFO admission -> core-native immutable parts
```

L2 groups for one second, with a bounded pending-row limit that may force an
earlier sync. Idle producers explicitly drain to acknowledge their final group.
References remain private until the catalog is durable. Core appends happen only
after journal sync. A returned Append accepts pending memory; only Drain is a
durable acknowledgment. Seal markers remain in input order and are applied
through the same core API after their journal group is durable. Named-layer
submission returns after bounded admission, while core seal completion returns
the existing dispatch credit asynchronously. No receipt queue or background
daemon sender is added. Each producer completes fanout before its next packet,
so seals retain per-stream order. An owner consumes at most one packet per
producer in a poll and combines seals already available in that poll into one
atomic ingest. Completion handles contain no queued data; pipelined seal credit
and synchronous acknowledgments stay held until the combined ingest completes.
Size and timer limits still flush the same bounded head. The owner keeps original row bits, sequence,
apply clocks and policies; its existing planner remains the only owner of
LastUpdate/revision state. Core durability is separate from the journal ACK and
is observed through committed frontiers, synchronous seals, or Close. Owner
failures are sticky and are returned by subsequent admission and Close. The
one-second journal grouping deadline and pending-row/credit limits are unchanged.
The harness
reports intake and complete/drained throughput separately, with final Drain and
Close wall times reported individually. Sync rates and forced
groups are reported rather than assuming one sync per second.

L1 holds rows until its seal marker, syncs changed names once, and submits rows
to core. L0 bypasses both the catalog and journal. All layers preserve identical
numeric IDs, points, per-name sequences and 64 logical barriers on the fixed
corpus: 1,048,576 names, 256 updates, four updates/name/barrier, 48 producers and
12 databases. Names and input buffers are prepared outside timing, but new-name
admission, lookup and metadata writes remain inside L1/L2 timing.

## Removed adapter work

There is no portable per-point planner, replacement SST namespace, repeated
per-series activity metadata, reference SST, private checkpoint SST, or second
key layout. The core frontier is the only core publication cursor. Catalog
metadata changes only on admission; schema changes require an explicit future
versioned definition protocol, not silently changing an existing catalog entry.
Rollups, retention and reader publication will be added only after earlier
layers pass their budgets. They must use the existing seal reducer and snapshot
reader modules; this harness does not claim classic render parity yet.

| Former adapter responsibility | Core-first owner |
| --- | --- |
| Per-point portable key planning and replacement raw SSTs | Existing core numeric append, sort and SST writer; translator removed |
| Private checkpoint SSTs and per-series part references | Existing core durable frontier; duplicates removed |
| Per-seal activity metadata for every touched series | Removed; durable catalog writes new names only |
| Seal output and part numbering | Existing core immutable-part writer and monotonic part counter |
| Snapshot lifetime/publication | Existing core snapshot ownership; full reader publication remains L4 |
| Rollup/retention parts and portable placement keys | Future additions inside core, using existing reducer; no daemon-side version |

## Layer budgets

| Layer | Addition | Acceptance |
| --- | --- | --- |
| L0 | Core only | Same-run numeric baseline |
| L1 | Durable catalog IDs | Cumulative throughput >=95% of L0 in two consecutive pairs |
| L2 | Journal | Cumulative >=90%; journal <=4 B/point |
| L3 | Exact seal rollups | Existing reducer, exact classic behavior |
| L4 | Immutable reader publication | Existing snapshot reader/publisher |
| L5 | TCP receivers | At least 32 connections |
| L6 | Replay and first accept | Replay through append; first accept <=100ms |

All layers require compaction <=2 B/point, total written <=63.8 B/point
(approximately 11 B/point is the aim), and exact values, times and order.
A failed layer blocks adding the next layer. The optional L2 measurement can be
run diagnostically after an L1 failure, but must be labeled unqualified.
Process writes, SST bytes ingested and compaction bytes before close, catalog/journal sizes,
at-rest size, intake throughput and complete CPU are reported separately.
A finite fixed-corpus run does not establish settled sustained throughput.
Reported stream stage times are summed elapsed
times across producers, not CPU time or additive process wall time. In particular,
core submission includes queue and dispatch-credit waits. The synchronous L0
control also waits for each seal acknowledgment; named layers pipeline them.

## Usage

```sh
go test ./internal/v2daemon -run TestFixedCorpusAllLayers
```

Each layer runs the fixed corpus and verifies every stored tuple through
the existing `v2core.ScanRaw` decoder on reopened immutable snapshots. A bitmap
rejects duplicate and missing metric/sequence pairs; timestamps and IEEE value
bits are checked individually. All persisted catalog names are checked. Sample
reads through `v2core.Read` remain as an independent query-path check. Verification
runs outside the timed intake/drain/close interval. No running daemon is contacted. L2 also decodes every journal record outside timing and checks every
name, definition, clock, timestamp and IEEE value bit against the fixture.
A separate correctness suite includes NaNs, signed zero, infinities,
subnormals, signed times, name collisions across streams and journal decoding.
There is no log reclamation, crash replay, periodic idle service loop, receiver
ACK protocol, rollup, retention or production daemon switch in this first step.
