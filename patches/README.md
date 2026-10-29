# Vendored dependency patches

Run `go mod tidy && make vendor` when changing dependencies. The `vendor` target
regenerates the module sources and applies the patches below. It fails if a patch
no longer applies, so dependency upgrades require reviewing the maintained delta.

## Pebble cache shard capacity

`pebble-explicit-cache-shards.patch` adds a positive shard-count constructor to
Pebble v1.1.5. The default constructor is unchanged.

`pebble-range-delete-cache.patch` adds an optional cache for range-deletion
blocks, preserving normal data/index cache concurrency. Exact stores allocate
one eighth of the configured budget, capped at 1 GiB, to a four-shard deletion
cache; the rest remains the ordinary Pebble cache. An 8 GiB budget becomes
7 GiB of ordinary caching plus 1 GiB for deletion blocks. A real 13.7 MiB block
fits without forcing large-block eviction to hold ordinary metadata cache locks.
Readers and databases retain/release both caches; obsolete files evict from both.
Each database uses its own namespace in the deletion cache, including when
different primary caches share one deletion cache.
Store statistics include combined size/hits/misses and separate deletion-cache
counters. File format, admission/eviction algorithms and write durability do not
change. Review or remove these patches when the pinned upstream supports an
equivalent separation or bounded large-block caching.

## Pebble asynchronous WAL rotation

`pebble-async-wal-rotation.patch` applies to `github.com/cockroachdb/pebble v1.1.5`.
Only the experimental exact chunk store enables `WALRotationAsync`. Stock rotation
closes and syncs the previous WAL while holding the global commit mutex. This
blocks ordinary interval-synced writes behind the disk's fsync latency.

The patch installs the next buffered LogWriter immediately and performs the old
close, next-file create/reuse and directory sync on a background handoff. The next
WAL is never created before the previous EOF is durable. Its writer waits for
that handoff before issuing file IO; Sync requests wait for actual persistence.
A flush gate prevents premature old-log reclamation. Checkpoints and Close wait
for the handoff. At most one handoff is outstanding; a second rotation waits, and
the existing memtable limits still apply. WAL errors retain fail-stop behavior.
The WAL format and strict recovery rules are unchanged.

`internal/chunkstore/wal_rotation_test.go` exercises blocked fsync, exact-store
write visibility, durability across four simulated power-loss boundaries, error
propagation, concurrent natural rotation/recycling, compaction, checkpoint and
shutdown. The patch remains local until an upstream release provides equivalent
ordering and bounded asynchronous rotation. Review or remove it when upgrading
Pebble; do not mechanically carry it across versions.

## Pebble provider directory sync

`pebble-provider-dir-sync.patch` adds `Options.Local.AlwaysSyncDir` to Pebble
v1.1.5. The object provider's `Sync`, which precedes every manifest commit of
new tables, then always syncs the store directory. Stock Pebble skips that
sync when no local object was created or removed since the provider's
previous `Sync`, and clears the flag before syncing.

With `store-parallel-table-syncs` the store defers each table's sync to that
directory sync (`internal/chunkstore/deferred_sync.go`). If another job's
`Sync` ran after a job created its table but before the table was finished,
the job's own `Sync` found the flag cleared and skipped the barrier, so the
manifest could record a table whose data was not yet durable: a power loss
then left the store referencing an empty or partial table. The store sets the
option only when it defers table syncs; otherwise behaviour is unchanged.
`TestDeferredSyncBarrierAfterAnotherJobsProviderSync` reproduces the
interleaving on Pebble's provider and a strict in-memory filesystem. Remove
the patch with the deferred syncs, or when upgrading to a Pebble whose
provider no longer skips the sync.

## Vellum builder node reuse

`vellum-builder-reuse.patch` applies to `github.com/blevesearch/vellum v1.2.0`.
Full index rescans rebuild both metric and directory FSTs. Stock Vellum allocates
replacement builder nodes after discarding a shared empty final node or finding
an equivalent registry node. It also allocates an unfinished node on every visit
to a key depth greater than its fixed 64-node cache.

The patch returns discarded nodes to the existing pool and caches unfinished
nodes beyond depth 64. The overflow cache is bounded by the longest key seen by
that builder; reset clears the retained references. The FST encoder, registry,
file format, row values, and scanner metadata refresh remain unchanged.

`carbonserver/fst_builder_reuse_test.go` checks stock-encoder byte compatibility,
lookup results, and allocation cost for long keys. The patch is local until an
upstream Vellum release contains both recycling fixes and long-key cache reuse.
Remove the patch and its application from `make vendor` when adopting that release.

## Whisper migration snapshots

`go-whisper-visible-snapshots.patch` applies to
`github.com/go-graphite/go-whisper v0.0.0-20261015144539-7282ab6234ef`.
It adds an archive export that includes the compressed buffered rollups visible
to Fetch. The existing physical archive export keeps its behavior. Migration
first folds OOO data into a private snapshot, then reads this visible view.

Callers can place private OOO snapshots beside the destination. Linux uses a
reflink when available and otherwise copies sparse data extents, preserving
large sidecar holes. This avoids expanding a sparse source into the system
temporary volume. Other platforms retain the existing read-copy behavior.

`internal/whisperio` and `persister/storage_migration_test.go` compare migrated
values against cwhisper Fetch, including buffered rollups, OOO corrections,
all six aggregation methods, compaction and reopen. The native evaluation also
checks Linux reflink and sparse fallback contents. Remove this patch and its
application from `make vendor` when pinning an upstream version with these APIs.


## CWhisper source-window read buffers

`go-whisper-block-buffer-reuse.patch` applies after the Whisper snapshot and
archive-window patches. It reuses temporary compressed block read buffers in
4 KiB–1 MiB size classes. Larger blocks remain unpooled. The reader owns its
scratch until decoding has returned independent scalar points, including on
error paths; encoding, OOO handling and archive selection are unchanged.

This targets the allocation cost of source-window reads during mirrored writes.
`persister/whisper_read_scratch_test.go` verifies retained results and concurrent
independent readers across files, OOO corrections, and small/large blocks. Remove
the patch and its `make vendor` application when an upstream version provides
this buffer reuse.

## CWhisper bit decoding

`go-whisper-bit-reader.patch` uses bounded word reads for interior fields and
byte reversal for the existing byte order. Short fields, partial final bytes,
cursor positions and EOF behavior retain the original decoder's results.
It applies after the source-window buffer patch.

The vendored `compress_bits_test.go` compares values and reader state with the
original implementation across widths, alignments, truncated buffers and random
sequences. `make test` includes this dependency package explicitly, because
`go test ./...` excludes vendored tests. Remove the patch and its application
when the pinned upstream includes equivalent decoding and oracle coverage.

## Integration deltas

`pebble-integration-delta.patch` and `go-whisper-integration-delta.patch` hold
every vendored change the patches above do not: edits made in `vendor/` directly
and never captured as a patch. They apply last and make `make vendor` reproduce
the committed `vendor/` exactly.
- Pebble: the table foreign flag loaded once per table, the snapshot-free fetch
  read view and the exit close.
- go-whisper: continuation storage, sparse archives and storage files, plus the
  in-memory overlap replay and its tests.

Split a feature out of them into its own patch when it next changes.

## go-whisper-overlap-in-place.patch

Applies after the integration delta, which it builds on. A batch that starts in
the latest base slot, or meets a sidecar, corrects the encoded tail in place
instead of replaying the whole compressed file, so completed coarse rollups no
longer trigger a full rewrite every hour. Batches reaching behind the latest base
slot still take the full rewrite.

## go-whisper-late-tail-correction.patch

Applies after go-whisper-overlap-in-place.patch. The tail correction now also
applies to batches reaching behind the latest base slot or the base retention.
They used to replay the whole compressed file, which encoded the next
unfinished coarse window, so a metric receiving late points replayed every
coarse window indefinitely (about 107 replays/s per sparse workload, 4 cores). The
late points themselves now take the sidecar path of every other file.

A rollup propagated within the file aggregates its whole stored window. When
it lands behind the coarse watermark, over an unfinished value a replay or
compaction encoded, it now replaces that value in the same "rollup" rewrite
instead of being diverted to the sidecar, where it only filled holes and the
stale partial stayed visible. Explicit coarse writes and late base points keep
their previous handling. `overlap_late_tail_test.go` runs the digest scenarios
and the sparse shape against a classic file in lockstep with the previous rules.

## go-whisper-in-place-corrections.patch

Applies after go-whisper-late-tail-correction.patch. Two corrections that
rewrote the whole compressed file now patch only the blocks they change, under
the same file, when the storage can replace stored points in place
(`StorageBlockPatcher`, which the shared store's continuation files implement;
plain files keep the rewrite):

- A buffered rollup correction (the "rollup" rewrite) replaces the corrected
  coarse values in their blocks.
- A sidecar slot collision, which replayed the whole file, folds the sidecar in
  place with the merge the replay starts with. A batch strictly newer than every
  stored base interval then takes the incremental path; a batch reaching the
  newest stored base interval or behind it, or a fold that would append past an
  archive's encoded range, still replays. The result equals the replay's,
  except where the replay encodes an unfinished coarse window and later
  corrects it to a value classic whisper does not store; there it keeps the
  classic value.

`planInPlace` stores the points the rewrite's merge would. A changed block is
re-encoded from its first point; points that no longer fit carry into the next
block. An overflowing current block, files needing extension, Mix aggregation
and overlapping blocks fall back to the rewrite. Outcomes are counted by
`RollupCorrections` and `SidecarSlotMerges`. `continuation_in_place_test.go`
checks every fetch against the rewriting path and classic whisper in lockstep. `buildRewrite`'s buffer
precedence moved into `applyExtraToBuffer`, shared by both paths.

## go-whisper early sidecar folds

`go-whisper-early-sidecar-folds.patch` (applied after the in-place corrections
patch) adds `FoldOutOfOrderInPlace`: a due out-of-order sidecar is folded into
the main file in place, exactly as `MergeOutOfOrderWithOptions` would merge it,
whatever its size and without the merge rate limit. It never rewrites the file;
when the fold cannot be placed in the existing blocks the ordinary merge rules
apply. Small sidecars otherwise stay until they collide with ordinary input a
base retention later and replay the whole file. `persister_sidecar_folds_total`
counts the results.

## go-whisper-ooo-direct-writes.patch

Applies after go-whisper-in-place-corrections.patch. With
`Options.OutOfOrderDirect` (go-carbon `store-ooo-direct-writes`), late base
points the compressed encoder rejects are stored in the blocks covering their
intervals instead of an out-of-order sidecar, when the storage implements
`StorageBlockPatcher`: the coarse windows they change are recomputed from the
union of the file's points and the late ones and replaced in place, within the
update, as classic whisper's write and propagation do. An existing sidecar is
folded in place before the update. Points that cannot be stored that way (a
window past the encoded range outside any buffer, an overflowing current
block, an unfoldable sidecar, Mix or older formats, plain files) still go to
the sidecar. Counted by `DirectLateWrites`, `DirectLatePoints` and
`DirectSidecarFolds`. Remove when sidecars are retired.

## go-whisper-import-sidecar-folds.patch

Applies after go-whisper-ooo-direct-writes.patch. Adds
`FoldOutOfOrderInPlaceAt`, the fold direct late writes make before an update
(`foldSidecarForDirect`), reading the sidecar as of a given time and returning
its decision instead of counting it; the shared fold now reports its result to
both. go-carbon's continuation import uses it with `store-ooo-direct-writes` to
fold a converted file's sidecar into its blocks during the conversion, so the
metric is written once without a sidecar instead of being rewritten by its
first update's fold. Remove with the direct-writes patch.

## go-whisper-off-grid-blocks.patch

Applies after go-whisper-import-sidecar-folds.patch. Conversion keeps a
CWhisper block's exact points, including points off the archive's grid that
corrupt production blocks hold (a raw first point off the grid moves every
delta-coded point after it). Continuation storage accepts them, but the
virtual file re-encoded each block through `EncodeContinuationBlock`, which
required grid alignment, and every rewrite (sidecar merge, replay merge,
extension, correction) reads each block that way. Such a metric failed every
merge with `continuation points must increase on their archive grid` and the
persister requeued its batch forever.

- `AppendPointsToBlock` stores a change of interval that is not a whole number
  of steps in the full-interval form, which the decoder restores exactly. The
  delta forms truncated it, moving that point and every later one in the block.
  Aligned points encode exactly as before.
- `EncodeContinuationBlock` accepts positive, strictly increasing points, as the
  rest of the continuation path does, and decodes a block holding off-grid
  points again to prove it reads back unchanged. Its errors name the block, its
  descriptor (first, last, count, step) and the first offending point.
- `buildRewrite` stores off-grid points on the grid as a fetch serves them
  (`alignBlockPoints`), so a merged late point is not shadowed by a stored point
  of the same interval. Of several points in one interval an on-grid one wins:
  writers store only on-grid points, so it is a later write (a late point an
  in-place patch stored next to converted points, which reads hid). Otherwise
  the latest wins, as a fetch serves it. A streamed block is thereby repaired
  in place by the first rewrite that reads it. When the file cannot produce a
  block's bytes at all, typed storage serves the block's exact points instead
  (`rewriteFallbackPoints`). Both are counted by `RewriteBlockFallbackCount`
  (`persister_rewrite_block_fallbacks_total`).
- On typed storage a rewrite streams the current block rather than copying its
  bytes: the copy resumed appends at the header's bit position, which describes
  the writer's bytes, not the block re-encoded from exact points.
- `planBlock` leaves a block holding off-grid points to the rewrite. With
  direct late writes, late points for such a block therefore go to the sidecar
  (`persister_ooo_direct_writes_total{result="sidecar_unsupported"}`) and an
  import fold into it is declined, until the first merge rewrites the block
  onto the grid. Without this, a direct write stored the late point next to
  the off-grid point of its interval, where reads did not see it, and an
  import fold failed the conversion's verification.

`continuation_off_grid_test.go` round-trips off-grid shapes and checks the
error text, and `TestAlignBlockPoints` the interval rule.
`internal/whisperio/continuation_unaligned_merge_test.go` reproduces the
production failure through conversion, the store and the metadata cache, with
and without direct late writes and a source sidecar, and checks that the first
merge leaves no stored point off the grid.

## go-whisper in-place planning scratch

`go-whisper-in-place-scratch.patch` applies after the off-grid blocks patch.
In-place correction planning uses the existing optional `ReadStorageBlockInto`
transport with pooled stored-point and re-encoding scratch, each bounded at
1 MiB. Returned plans and carry retain independent storage; ordinary readers
keep their owned-read contract. The full block is still read and validated.
`rollup_scratch_test.go` compares owned and borrowed readers, checks retained
plans and read errors, and measures allocations on a 7200-point block.
Remove the patch when the pinned dependency supplies equivalent scratch reuse.

## go-whisper-atomic-sidecar-fold.patch

Applies after the other Whisper patches. When `OutOfOrderDirect` cannot fold an
existing sidecar in place, atomically committed storage uses `MergeOutOfOrder`
before accepting newer late values. Publishing a rewrite and deleting its
sidecar must commit together; ordinary filesystem writers keep their old path.
The continuation codec also folds any new fallback diversion before committing
its update. This keeps base last-write-wins and coarse hole-filling precedence
without leaving pending sidecars. Remove when upstream supplies this atomic
fallback or the continuation codec no longer uses the patched direct path.
`internal/whisperio` tests conversion rewrites, live-write ordering, interrupted
batches and physical/visible merge equivalence.

## Complete vendor replay

All dependency changes remain vendored under their original module identities.
`make vendor` regenerates the pinned module sources and applies each patch in
Makefile order. No separate dependency forks are required.

- `pebble-explicit-cache-shards.patch` exposes explicit cache sharding for the
  shared store while preserving the default shard selection.
- `pebble-range-delete-cache.patch` keeps range-deletion cache invalidation
  consistent with shared-store metadata removal.
- `protobuf-json-stable-spacing.patch` preserves the existing JSON wire spacing.
- `pebble-integration-delta.patch` captures the remaining reviewed shared-store
  options and integration fixes against the pinned Pebble version.
- `go-whisper-archive-windows.patch` exposes source-window archive reads used by
  verified storage migration.
- `go-whisper-integration-delta.patch` captures the transactional storage and
  continuation integration against the pinned Whisper version.
- `go-whisper-overlap-in-place.patch`, `go-whisper-late-tail-correction.patch`,
  `go-whisper-in-place-corrections.patch`, `go-whisper-early-sidecar-folds.patch`,
  `go-whisper-ooo-direct-writes.patch`, and `go-whisper-import-sidecar-folds.patch`
  preserve exact correction ordering and fold out-of-order state through the
  canonical writer. Their regression tests remain in the vendored sources.
- `go-whisper-late-tail-test-groups.patch` partitions differential scenarios into
  independently selectable groups while retaining the complete coverage and
  aggregate assertion. It applies after the other Whisper changes.
- `go-whisper-storage-file-accessor.patch` exposes the underlying StorageFile
  without requiring an OS file; continuation storage uses this accessor.

Each patch is tied to the current module versions. Remove a patch only after
pinning a dependency with equivalent behavior and verifying exact vendor parity.

## Whisper sparse archive snapshots

`go-whisper-sparse-archive-snapshots.patch` reuses the bounded extent-aware reader
for classic `ArchivePoints` snapshots, with scratch sized to each stored extent.
Sparse virtual archives otherwise allocate
full-retention buffers during transfer verification. The oracle compares dense,
sparse and empty histories, duplicate timestamps and exact floating point bits,
and checks bounded reads and error propagation. Remove this patch once the pinned
upstream implements the same snapshot behavior.
