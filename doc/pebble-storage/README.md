# Pebble store: design notes

go-carbon can keep metrics in one [Pebble](https://github.com/cockroachdb/pebble)
database per host instead of one Whisper file per metric
(`storage-backend = "pebble-chunk"`). Some code, the migration phase value
`shared` and older notes call it the *shared store* (`sharedStore`): one store
shared by all metrics and by the persister, carbonserver and embedded buckyd.
These notes call it the *Pebble store*. Metrics follow the same split:
`pebble_*` for Pebble engine internals, `store_migration_*` and
`store_relayout_*` for go-carbon's own processes, and `persister_pebble_*` /
`persister_whisper_*` for persister writes to each backend (earlier builds
exported these as `store_*`, `persister_shared_*` and `persister_original_*`).

The notes describe the design as implemented, the algorithms and data
structures, how to tune it, and the production problems met while rolling it
out, with how each was diagnosed and fixed.

## Reading order

1. [architecture.md](architecture.md): components, data flow, who owns the
   database, open and close.
2. [data-layout.md](data-layout.md): key spaces, store formats v6 and v7, the
   continuation virtual files, descriptors, point groups, hourly buckets and
   paged headers.
3. [lsm-levels.md](lsm-levels.md): how data moves from memtables through L0
   to L6, what compaction reads, writes and drops, and how it goes wrong.
4. [write-path.md](write-path.md): one persister update step by step, with
   the cost of each step, out-of-order merges and commit.
5. [read-path.md](read-path.md): find and render, what a fetch reads, and
   what makes reads expensive.
6. [migration.md](migration.md): online conversion of existing Whisper files
   (staging, ingest, publish, reclaim, lease, hints, archive mode) and the v7
   relayout.
7. [tuning.md](tuning.md): every `store-*` setting with guidance, reloadable
   settings, and the metrics to watch.
8. [challenges.md](challenges.md): production problems as symptom, diagnosis,
   fix and result, including the compaction-debt arithmetic and why L0 tables
   stay small.

## Status

Experimental and opt-in. The file backend remains the default. Format v7
(`store-time-buckets`) is one-way: once a store is v7, older binaries refuse
it.

## Older notes

These remain for history and detail; the files above supersede them where they
differ.

- [shared-storage.md](../shared-storage.md): configuration, expiration, buckyd
  transfer API, qualification gate.
- [shared-exact-blocks.md](../shared-exact-blocks.md): exact encoding, WAL
  rotation, bulk conversion measurements.
- [incremental-storage-migration.md](../incremental-storage-migration.md):
  online conversion procedure and results.
- [shared-exact-live-migration.md](../shared-exact-live-migration.md):
  mirrored live migration.
- [time-partitioned-layout.md](../time-partitioned-layout.md): v7 design note
  and the analysis behind hourly buckets and paged headers.
- [storage-testing.md](../storage-testing.md),
  [storage-correctness-plan.md](../storage-correctness-plan.md),
  [storage-performance-plan.md](../storage-performance-plan.md),
  [storage-performance-results.md](../storage-performance-results.md).

## Independent reviews

Independent reviews, with archived reproducers and the status of each finding on
this line:

- [shared-storage-review.md](../shared-storage-review.md): seven correctness,
  durability and lifecycle findings, all still open.
- [time-partitioned-layout-review.md](../time-partitioned-layout-review.md):
  v7 marker, relayout, paged-header rollback and deferred-sync findings.
- [shared-storage-performance.md](../shared-storage-performance.md): CPU and
  memory parity measurements and the reader and header changes.
