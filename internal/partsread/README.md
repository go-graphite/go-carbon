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

Render workspaces for exactly 3,600 or 86,400 output slots are reused after the
owning handle closes. Earlier results from the same handle keep separate arrays.
Active workspace stays charged at 32B/slot; fixed idle channels retain at most
76MiB separately from that admission budget. Other lengths allocate exactly the
requested number of slots and are never cached. Reuse clears occupancy before
merging and initializes absent values to the original NaN representation.

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
