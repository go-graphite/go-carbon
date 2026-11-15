# Verified history-part conversion

This experimental offline package copies already stored archive slots into the
portable placement-group/vshard part layout. Coarse archive values are copied,
never derived again from finer data. It is a building block, not a daemon switch
or a production migration command.

`ArchiveSource.ScanMetrics` supplies immutable metric policies and occupied slots
from **every** existing archive. GCA2 values preserve raw IEEE bits, including
occupied NaNs and negative zero, and retain schema-only empty archives. The
compressed representation reuses the current exact sealed-point codec; timestamps
outside its unsigned range use a lossless fallback. This temporary value envelope
is not the production incarnation/source-priority contract.

A `Unit` selects one placement group and routing vshard, optionally restricted to
an exact bytewise name range `[Lower, Upper)`. The JSON journal preserves arbitrary
name bytes. Exact canonical names remain portable metric IDs. Bounded chunks are
sorted into SSTs, each restricted to one time partition and unique part prefix.
Different chunks have disjoint key ranges even when their metric names overlap.
There are no destination reads before ingest. After ingest a snapshot reads every
new key/value and compares its framed SHA-256 digest with the source-built digest.
Stock Pebble chooses the lowest nonoverlapping ingest level; `OnIngest` exposes L0
ingest bytes rather than promising a particular level.

The synced journal advances through:

```text
building -> prepared -> ingested -> ready -> published -> reclaimed
```

A crash while building discards only that unit's uncommitted staging tables.
Prepared tables survive ingest because Pebble consumes disposable hardlinks;
resuming after an ambiguous ingest reuses the exact keys and does not rescan the
source. Source and destination incarnation identities fence stale journals.
`ready` means durable digest verification only. It does not authorize routing,
legacy deletion, or replica copy publication.

`Publisher` must atomically and durably publish routing with the catalog,
tombstones, provenance, and reader generation. `Reclaimer` must wait for old reader
pins, delete actual legacy data spans, and wait for physical space recovery.
Both operations must tolerate retries. Reclamation is refused without a publisher
and a maximum batch byte bound. The source's raw metadata/data layout determines
which spans are reclaimable: name-order catalog entries alone do not imply
name-order archive SSTs. An exclusively locked read-only Pebble handle cannot
coexist with a writable deletion handle; the integration must provide fenced
reader close/reopen or a separately pinned immutable view.

`SpaceGuard` checks free bytes before each unit, reserves concurrent workers'
bounds, and leaves the configured headroom. It allows an extra complete copy for
cross-filesystem ingest. It does **not** enforce a total-store percentage budget:
the scheduler must account for cumulative size growth and bound concurrent
batches until reclaimed. Space errors and verification failures return errors;
routing is never published for a failed verification.

## Qualification tool

`go run ./tool/parts-convert -dir /new/private/path -workers 4` creates only a
synthetic destination. It reports end-to-end conversion plus durable verification,
ingest placement, compaction counters, CPU, and allocated bytes. The directory
must not already exist. `-compare-coldpack` builds an actual current native
continuation/coldpack baseline and compares identical stored archive images after
compaction. This checks stored slots, not full continuation/render semantics, and
is not a real-traffic resource qualification.

Production prerequisites remain a raw read-only legacy scanner that never
materializes rollups, the final immutable catalog/incarnation/source-tier envelope,
a reader publication fence, and a physical legacy reclaim adapter. The generic
continuation snapshot API may materialize coarse values and is unsuitable as that
scanner. Tests cover archive-bit preservation, actual target corruption,
crash-after-ingest recovery, publication/reclaim retries, identity fences, free
space refusal, and cross-process exclusion. Do not use `Ready` as a GC fence.
