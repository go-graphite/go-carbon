# Architecture

## Components and data flow

```
 receivers (tcp/udp/pickle/http/kafka/pubsub)
        │  points
        ▼
 cache (sharded map, writeout queue)  ◀────────────── carbonserver merges
        │  pop one metric's batch                       unpersisted points
        ▼                                                into replies
 persister workers (crc32(name) % workers)
        │
        ├─ metric owned by the store ──▶ chunkstore.Store (internal/chunkstore)
        │                                  │  one *pebble.DB, one process
        │                                  │  continuation codec (internal/whisperio)
        │                                  ▼
        │                               Pebble: memtables → WAL → L0 … L6 tables
        │
        └─ metric not converted yet ──▶ original CWhisper file (incremental mode)

 carbonserver find/render ──▶ index (trie / file-list cache) + Store.Fetch
 embedded buckyd ───────────▶ the same Store handle (transfer API)
 conversion workers ────────▶ Store.StageFileSnapshot / PublishFileOwnership
```

- **One owner.** One go-carbon process opens the database and holds Pebble's
  directory lock. Persister, carbonserver, conversion and the embedded buckyd
  all use that one `*chunkstore.Store` (`app.MetricStore`). Never open the
  directory from another process.
- **Backend selection.** `storage-backend = "pebble-chunk"` with a separate
  `store-dir`. `store-encoding = "exact"` selects exact point blocks, which the
  incremental and continuation modes require.
- **Continuation mode.** With `store-incremental-migration = true` the store
  runs the *continuation codec*: each metric is a set of virtual CWhisper
  files kept as Pebble keys, and the real go-whisper library reads and writes
  them through a key-value file system. Behaviour therefore matches CWhisper
  exactly, including aggregation, retention and out-of-order handling. See
  [data-layout.md](data-layout.md).
- **Two backends during conversion.** Until a metric is converted, the
  persister writes its original file and readers read that file. Ownership
  moves per metric, atomically, when conversion publishes it
  ([migration.md](migration.md)).

## Key types

| Type | Package | Role |
|---|---|---|
| `Store` | `internal/chunkstore` | Owns `*pebble.DB`, caches, sync loops, ingester, relayout, stats. |
| `Options` | `internal/chunkstore` | Everything `Open` needs; built from `[whisper] store-*` settings in `carbon/app.go`. |
| `Metadata`, `MetricConfig` | `internal/chunkstore` | Catalog record: name, retentions, aggregation, ID, generation, revision, ownership state. |
| `ContinuationCodec` (interface) | `internal/chunkstore` | Create, Import, Update, Fetch, Snapshot, Verify, Drop, RelayoutPlan for one metric. |
| `continuationIO` | `internal/chunkstore` | A metric generation's key-value view (reader + optional batch); maps codec suffixes to store keys. |
| `ContinuationCodec` (struct) | `internal/whisperio` | The CWhisper implementation of the interface. |
| `continuationFiles`, `continuationFile` | `internal/whisperio` | Virtual file system over `continuationIO`; one per operation. |
| `Whisper` | `persister` | Workers; routes each metric to the store or its original file. |
| `deferredSyncFS` | `internal/chunkstore` | Optional `vfs.FS` wrapper that syncs new tables in parallel ([challenges.md](challenges.md#flushes-waited-on-one-table-sync-after-another)). |

## Opening the store

`chunkstore.Open(dir, Options)` (`internal/chunkstore/store.go`):

1. Validate options (budgets, sizes, bloom bits).
2. Wrap the file system in `deferredSyncFS` when `ParallelTableSyncs > 0`.
3. Check the incremental marker (`INCREMENTAL-FILE-MIGRATION`), which binds
   the store to one source directory by path, device and inode.
4. Read the format marker (`CHUNKSTORE`). An empty directory gets a new
   marker. An existing v6 store with `TimeBuckets` is *planned* for v7 but
   not yet marked.
5. Build Pebble options: the chunk merger, a dynamic
   `MaxConcurrentCompactions` (an atomic the reload path can change), the
   write-stall listener, L0 thresholds, base-level size, table-reader budget,
   bytes-per-sync, per-level target file sizes and bloom filters. Exact
   blocks also enable asynchronous WAL rotation and size the memtable queue:
   `MemTableStopWritesThreshold = clamp(maxUnflushed / memtableSize, 4, 16)`.
6. Create the caches. With exact blocks one eighth of `store-cache-size` (at
   most 1 GiB) is a separate range-deletion cache; the rest is the block
   cache, sharded by CPU count.
7. `pebble.Open`. Only after it succeeds, holding the directory lock with
   validated options, is a v6 marker replaced by v7. A failed open leaves the
   store readable by older binaries.
8. Start the snapshot-release worker, the WAL syncer, the incremental source,
   the file ingester (`store-migration-ingest`), the periodic WAL sync loop
   (`store-sync-interval`), the catalog membership scan and, with time buckets
   and a positive rate, the relayout.

## Closing the store

`Store.Close` stops background work before Pebble:
relayout, ingester, catalog membership scan, periodic sync loop, snapshot
release, WAL syncer, then `pebble.DB.Close` (which flushes and syncs the WAL),
then any deferred table syncs still pending, then the caches.

At the application level `App.stopAll` stops listeners, the persister and the
cache before closing the store. A `SIGUSR2` restart dumps the cache to disk
and restores it on start, so unpersisted points survive.

## Configuration reload

`SIGHUP` re-reads the config. Most store settings require a restart
(`storageSettingsChanged` in `carbon/sharedstore.go` lists them). These apply
live:

- `store-compaction-workers` (`Store.SetCompactionWorkers`): running
  compactions finish first.
- a positive `store-time-buckets-relayout-rate` (`Store.SetRelayoutRate`):
  enabling or disabling relayout needs a restart; a rate of 1 pauses it.
- cache `max-size` (`Cache.SetMaxSize`), write strategy, quotas and the usual
  non-store settings.

## Concurrency boundaries

- **Per-metric stripes.** `lockMetric` and `lockMetricMutation` serialize
  mutations of one name. A small metadata ring per stripe caches recent
  `Metadata`.
- **Read/write coherence.** `fileReadMu` (32,768 stripes) carries
  `OriginalReadGuard` (readers, shared) and `SharedWriteGuard` (a persister
  confirming a popped batch, exclusive). A reader then sees a batch in the
  cache, in the store, or both, never neither.
- **Commits.** Pebble's commit pipeline serializes WAL appends. The WAL syncer
  coalesces explicit durability requests into one sync.
- **Persister.** Each metric hashes to one worker; conversion of a metric
  marks it `converting` so its writeouts wait until publication.
