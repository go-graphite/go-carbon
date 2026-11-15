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
