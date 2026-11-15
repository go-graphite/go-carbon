# Immutable read publication experiment

`Publication` atomically publishes a coherent `View`: catalog/index, frozen head,
immutable part set and sealed archive blocks. Its owner serializes generation
order. `Pin` retains that entire generation; replacement closes its resources
only after the last reader releases it. Head buffers must be copied or ownership
transferred before publication. Sources must reference pinned storage snapshots,
never a live database or mutable writer buffer.

`Budget` charges each active handle for retained resources plus its requested
merge/output allowance. `Render` reserves32bytes per output slot before allocating;
the reservation remains until handle close, including earlier render results.
The conservative per-reader resource charge can count shared resources more than
once. Callers stream find results and must not retain values after handle close.
Source implementations separately bound iterator/decode memory and must include
that bound in `RetainedBytes` or the work reservation; this reference package
cannot account for hidden allocations inside arbitrary source callbacks.

`CatalogFuncs` connects an existing immutable catalog and Graphite find index;
it does not implement a competing glob parser. Render clamps the interval,
selects the archive using the query start's age, uses Whisper's next-slot
alignment, and merges head/parts/blocks by source order. All sources must emit
versions of the requested *physical ring slots*, including newer timestamps
outside the query. Only after finding the newest replacement does the reader
check that its timestamp belongs to the query. Stored NaNs remain occupied;
values are copied as IEEE bits. Equal sequence with different content fails.

This is an adapter boundary, not a configured daemon backend. The existing
carbonserver response fields map directly to `Series.{From,Until,Step,Values}`;
its typed chunkstore wiring still needs an explicit integration change. Coarse
slots must be produced by an exact rollup engine before publication. Raw rows
alone cannot reconstruct already-expired fine neighbors or historical XFF
propagation. This package does not recompute rollups at query time.

`go test -tags=sealrollup_integration -run TestCombinedRenderDifferential
./internal/partsread` exercises the optional sealrollup dependency once that
sibling experiment is in the checkout. It feeds identical original batches to
sealrollup, classic Whisper, native exact and continuation storage, then compares
1h/1d/7d/30d renders from one durable part containing every retained batch
version, bit for bit. Separate snapshot tests exercise head/part/block ordering
and pinned replacement. The tag keeps the standalone snapshot primitive
buildable before that stacked dependency lands.
This bounded test fixes the admission clock; clock advancement and ring wrapping
also need the reducer's separate compatibility suite before production use.

Render workspaces use bounded size-class channels, reusing only exact query
lengths after the owning handle closes. Earlier results retain separate arrays.
Values, winner sequences/timestamps, occupancy and sparse touched-slot indices
use 29B/slot within the unchanged 32B/slot active reservation. Idle caches retain
less than 112MiB across all size classes; requests above 131,072 slots are uncached.
A cache size mismatch is discarded and allocates exactly the requested length.
Allocation initializes values to the original NaN bits. Close resets only touched
values and occupancy, so reuse avoids dense array initialization and clearing.
Only touched slots need the final timestamp validation pass.

`Handle.RenderInto` overlays into a caller-owned, prefilled `Series.Values`.
It validates the selected archive and aligned From/Until/Step/length before
writing, returning `ErrShape` on mismatch. Untouched slots keep the base values;
occupied NaNs and newer epochs outside the query invalidate legacy slots. Winner
bits remain intact until the sparse final pass, preserving equal-sequence conflict
detection. The supplied Values must not alias any live handle's workspaces;
returned Occupied has the usual handle lifetime. Errors after a source starts
can leave the supplied values partly modified, so callers discard them on error.

`SlotRanges` returns at most two physical-slot intervals for a valid aligned
query. Disk sources can seek an index by series/archive/physical-slot/version
without scanning finer archives or every part. All versions in those intervals
must be returned, including timestamps outside the query; filtering by wall time
first would lose ring invalidations. Frozen sources use binary series bounds and
seek over sorted timestamp runs that cannot occupy the requested physical slots.

`Series.Occupied` reports a physical-slot replacement even when its winner has a
timestamp outside the returned interval. Mixed readers must use this bitmap to
mask legacy values, including occupied NaNs and wrapped-slot invalidations. It
shares the handle/callback lifetime of `Values`.

The opt-in `TestSnapshotReadBenchmark` uses locked Linux reader threads with a
separate CPU mask (`PARTS_READ_BENCH_CPUS`). Launch its process with `taskset` on
writer CPUs; overlapping masks are rejected. Keep GOMAXPROCS, the writer CPU
mask, engine settings and publication hook identical across reader counts. The
harness reports allocation/GC counters, Pebble cache/snapshot/zombie metrics and
debt. Use standard test CPU/mutex/block profile flags and sample per-thread masks
from `/proc/PID/task/TID/status` during each arm. Read counts and latency exclude
warmup and final settle. Numeric append-part keys remain an offline fixture;
this adapter does not implement the portable placement-group codec.

With the sibling seal reducer available, `-tags sealrollup_integration` enables
`TestSnapshotLatencyComparison`. Set `PARTS_READ_COMPARE_DIR` to a new private
directory. It compares exact bits and alternating-order timings with the native
continuation engine for three schemas and 1h/1d/7d/30d ranges, using authoritative
archive records in a disk-backed fixture source. Fresh engine caches retain the
OS page cache. Its JSON records disclose the fixture codec and absence of sustained
ingest; wide globs and the eventual production source require separate evidence.

The latency fixture defaults to 200 warm and 30 fresh-engine-cache samples per
class, with alternating read order. `PARTS_READ_PROFILE=1` adds process
allocation/GC deltas and CPU labels; the indexed-Pebble adapter also records
source seek/record decode/merge timers. These
profile timings are diagnostic and must not be used as latency gate results.
`PARTS_READ_COMPARE_REPEATS` raises the warm sample count for CPU sampling.

`Render` results borrow read-only Values and Occupied until Handle.Close; consumers
must copy before modifying either slice. Sparse cache reset relies on this
contract. `RenderInto` writes caller-owned Values, while its Occupied stays
read-only and handle-owned. Never use a live Render workspace as the overlay base.

`BlockManifest` version1 persists each immutable block's complete key, physical
span, archive, file, offset, length and IEEE CRC-32. `PublishBlockPart` writes and
syncs data plus manifest in a staging directory, syncs that directory, then
renames it and syncs the parent. Part names are unique and never reused. A failed
parent sync can leave a complete part to reconcile by generation. This helper
publishes one part; its owner must commit the shard catalog/frontier reference
before exposing the corresponding `View` or trimming its journal.

`OpenBlockPart` validates all extents and opens every referenced file before
publication. The owner serializes this open against retirement. Attach the source
to the view and put `source.Close` in `View.Close`; open descriptors then pin old
inodes even after compaction unlinks their directory. `OpenBlockPartWithIDs`
resolves read handles from full portable keys when catalog IDs differ after a
copy or restart. Queries use the immutable directory, check a full block's CRC
before emitting its samples, and retain every physical-slot version.

Archive groups are limited by encoded bytes: 12KiB for sub-minute archives and
4KiB for coarser archives. `ArchiveBlockBytes` exposes that writer policy;
`PublishBlockPart` rejects oversized groups. Source admission reserves metadata,
file resources and a maximum decode buffer per pinned reader. Scratch is local
to the call and never shared with writers or another read.

The latency comparison now defaults to this durable file/manifest source.
`PARTS_READ_COMPARE_STORAGE=pebble` selects the older indexed-Pebble comparison.
Manifest recovery/open and view construction occur before timed Render, while
native Fetch includes its view construction; source publication cost and
sustained ingest remain separate gates. Both sources retain OS page cache.
The portable daemon backend and the numeric core writer benchmark still need
writer-side manifest integration; standalone fixture passes do not qualify them.

## Backend integration contract

The backend owns the following protocol. `BlockManifest` publishes one complete
part; it does not persist the catalog, journal cursor, active part set, or a live
publication. Completing the protocol is required before enabling this reader in
the daemon or claiming writer-isolation qualification.

1. **Persist one coherent envelope.** For each owner generation persist the
   catalog revision (including archive policies and complete portable identities),
   the sealed journal frontier vector, and the complete active part-reference set
   in the same atomic durable transaction as the corresponding raw/archive SSTs.
   A reference identifies a unique relative part path, manifest format version,
   manifest generation and placement-group/routing-vshard scope. Record the
   catalog-to-portable-key mapping needed by `OpenBlockPartWithIDs`; old local
   numeric IDs are not a copy/restart identity. Use the core owner's final key
   codec rather than inventing another placement-group encoding here.
2. **Stage before committing references.** Freeze or transfer the owner buffers;
   compute exact authoritative archive replacements at seal/compaction boundaries.
   Call `PublishBlockPart` and wait for its data, manifest and directory syncs
   before committing any reference to that part. A successful rename followed by
   a failed parent sync is an uncertain publication, not permission to overwrite
   the name. Open and validate every referenced file while retirement is excluded.
   If opening, validation or durable envelope commit fails, keep the old live
   view, report the error, and retain the journal needed for recovery. Do not
   acknowledge a new durable frontier from an incomplete envelope.
3. **Publish a matching immutable live view.** After durable commit, build one
   `View{Generation, Frontier, Catalog, Head, Parts, Blocks}` from that envelope
   and detached frozen heads. The view's frontier is the sealed frontier;
   unsealed head visibility must not advance journal retirement. Publish the
   whole view once, in owner generation order. Transfer resource ownership to
   `Publication.Publish`; queries retain it with `Pin` until response encoding
   finishes. Retained-byte accounting includes the entire catalog, directory,
   frozen heads, open file resources and bounded per-reader decode scratch.
   A `BlockFileSource` has a single close lifetime: open a separate descriptor
   set per view, or supply an owner-level shared-resource reference count when
   a part survives across views. Reusing the same source with independent
   `View.Close = source.Close` callbacks would close it under a newer view.
4. **Recover from committed references.** Load the last committed envelope before
   opening reads or replaying intake. Open all referenced parts, check each
   manifest version/generation and portable scope against its reference, remap
   read IDs from the recovered catalog, then publish that coherent view. A
   missing, truncated or corrupt referenced part is a recovery failure; silently
   omitting it is data loss. Directory presence alone never advances a frontier.
   Staging directories and complete unreferenced parts are uncommitted orphans;
   reconcile them against committed envelopes before deletion and never promote
   them merely because their generation is larger. Replay after the sealed
   frontier through the same writer path. Trim journal segments only through
   both the input log's fsynced frontier and the conservative minimum committed
   owner frontier, after referenced-part durability has been established.
5. **Retire after pins drain.** Compaction/retention first produces durable
   replacements and commits the replacement active set, then publishes its new
   view. The old view remains readable until all handles close. Its close
   callback releases its descriptor ownership; delete a part only after no live
   or recoverable committed envelope references it and all owning views have
   drained. Serialize this decision with opening sources. OS descriptor pinning
   tolerates unlink after open, but is not a replacement for durable reference
   tracking. Preserve occupied NaNs, newer physical-slot invalidations and
   retained versions until they cannot expose older values through either a
   part or the legacy overlay. Whole-part copy applies the same durable-reference
   protocol at the receiver before publication or journal retirement.

Version1 data payloads are fixed 24-byte little-endian time/bits/sequence records.
A compressed part codec needs an explicit persisted version/codec discriminator
and its matching checked decoder; it cannot be substituted under version1.
The current latency fixture qualifies only its declared payload format.

The minimum integration tests cover crashes before/after part rename, before/after
atomic envelope commit and before live publication; reopen with changed local
catalog IDs; concurrent compaction with paused old readers; and failure to open a
referenced part. Compare recovered and uninterrupted renders bit for bit. Reader
load trials must consume those published parts with the same writer publication
work enabled for every zero-reader and reader arm.
