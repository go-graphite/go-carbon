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
1h/1d/7d/30d renders from frozen head, parts and blocks bit for bit. The tag keeps
the standalone snapshot primitive buildable before that stacked dependency lands.
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
class, with alternating read order. `PARTS_READ_PROFILE=1` adds per-class source
seek/record decode/merge timers, process allocation/GC deltas and CPU labels;
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
