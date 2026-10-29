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

## Integration deltas

`pebble-integration-delta.patch` captures the remaining reviewed shared-store
changes against the pinned Pebble version: the table foreign flag loaded once
per table, the snapshot-free fetch read view and the exit close. Split a feature
out into its own patch when it next changes.

## Complete vendor replay

`make vendor` regenerates pinned module sources and applies the remaining
Pebble, Vellum and protobuf patches in Makefile order. These retain their
original module identities. Review each patch when upgrading its dependency.
Whisper production code comes directly from the pinned upstream module.

## Upstream Whisper integration

The archive-window, continuation storage, overlap correction and atomic sidecar
folding changes are supplied by the pinned upstream module after
[go-whisper #54](https://github.com/go-graphite/go-whisper/pull/54). No Whisper
source patch is applied by `make vendor`.

Go's module vendor command omits dependency tests. `WHISPER_VENDOR_TESTS` retains
the existing dependency regression/oracle tests by copying their exact bytes from
the same pinned module cache. `make test` continues to include the Whisper package
explicitly. The list preserves the previous vendored test surface without a
separate fork, private patch or second source of truth.

The upstream version also fixes failed-rewrite temporary cleanup and bounds
sparse classic snapshots and read scratch. These are the intentional reviewed
source changes accompanying patch retirement. All other vendored Whisper Go
files and retained tests must match the pinned module byte for byte.
