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
is not a real-traffic resource qualification. `-partition-seconds` explores
compression versus expiry granularity; its default is one day. Wider partitions
can improve sparse coarse-archive compression but require independent TTL and
read-amplification validation. Changing this diagnostic flag does not define a
production partition policy.

Production prerequisites remain the final immutable
catalog/incarnation/source-tier envelope, integration with the daemon's reader
publication, buffered-tail/OOO render parity, and total-store disk admission. The
generic continuation snapshot API may materialize coarse values and is unsuitable
as a physical scanner; `TransferSource.StoredSnapshot` instead uses
`ArchivePoints` directly for every archive. Tests cover archive-bit preservation, actual target corruption,
crash-after-ingest recovery, publication/reclaim retries, identity fences, free
space refusal, and cross-process exclusion. Do not use `Ready` as a GC fence.

## Delete-as-you-go offline copies

The experimental command below **deletes source data**. Its source must be a
self-contained, exclusively owned offline copy, with all WALs copied locally.
The source and destination must be separate, non-nested directories. A source
already held open by another process is refused. Neither directory may belong
to a running daemon.

```sh
go run ./tool/parts-convert -convert-copy /private/legacy-copy \
  -dir /private/converted -copy-batch-metrics 32 -copy-batch-mib 128
```

Restart with the same directories to resume. Each source and destination gets a
durable random incarnation marker. The command uses placement group `legacy`,
exact canonical name IDs, and the existing portable routing hash. It copies raw
fine and coarse archives, schema-only archives, aggregation method and XFF bits.
It does not qualify buffered tails or OOO overlays as a full render replacement.

A global bytewise name batch owns the IDs found in `m/<name>`, not a contiguous
name-ordered archive range. `LegacyPlan` freezes those catalog values, revisions,
and all time-bucket prefixes. Reclamation covers every generation under each
selected ID:

| Keys | Owned range |
| --- | --- |
| `c`, `u`, `r` | Prefix + big-endian ID, including all generations |
| `t` | Prefix + big-endian bucket + `u` + big-endian ID, for every bucket |
| `m/`, `o/` | Exact selected canonical names |

Only adjacent selected IDs are coalesced; unconverted ID holes stay intact.
The catalog sequence and unrelated names are retained. Bucket discovery seeks
between prefixes rather than scanning every archive block for every batch.

`NameConverter` persists its full active plan before ingest, then verifies all
participating vshards before advancing one durable cursor. Its first three cursor
JSON fields match `mixedread.Cursor`. A publication callback must atomically
install the cursor, immutable catalog and part generation; the offline command
has no daemon reader, so its publication is just the durable cursor. A persisted
cursor is safe to replay because the complete target batch was synced and
verified first. Persisted receipts are reverified against the actual target on
recovery, before source deletion. A pending plan survives deletion of its source
catalog and prevents starting another batch until reclamation finishes.

`ReaderGate` fences new reads and drains existing whole-request pins. The physical
adapter closes the read-only Pebble handle, acquires the exclusive writable
handle with the existing comparer/merger, validates remaining catalog/revision
records, syncs the deletes, manually compacts each range, closes the DB to finish
obsolete-file cleanup, and reopens the read-only handle. Failed reopen leaves
admission fenced; every step tolerates retries. The source is immutable except
for this adapter's own deletes. This is incompatible with simultaneous writes
from an old daemon.

The aggregate bound covers all new output in one batch. It **does not enforce**
`max(5% of the initial store, one batch)` total extra disk: shared-SST rewrites,
temporary compaction output, and cumulative format-size changes need a separate
admission policy. The result therefore reports `DiskCapQualified=false` and
`MixedRenderQualified=false`. A real-store 50 GB run and concurrent mixed-reader
render parity remain release gates. No synthetic throughput establishes them.

Tests exercise physical SST reclamation, preservation of an unconverted ID hole,
old generations and extreme bucket values, catalog/revision conflict refusal,
reader-pin draining, failed reopen, and six actual process SIGKILL boundaries
from durable target verification through completed legacy compaction. The real
offline-source fixture also checks exclusive-lock refusal, bounded admission,
three-batch conversion, and completed-run idempotence.
