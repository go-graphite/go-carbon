# Write path

One persister worker writes one metric's popped cache batch per update. On a
host receiving mostly 1-second metrics a batch carries a few hundred points,
because the cache accumulates points between a metric's writes.

## Routing

`persister.(*Whisper).store` → `incrementalWriteRoute(metric)`
(`persister/incremental_migration.go`), under the per-metric writer lock:

1. `Store.Metadata(name)`: a catalog hit means the store owns the metric →
   **store write**.
2. Otherwise open the original file under the fenced source directory → write
   the **original** with CWhisper (until the metric is converted).
3. If neither exists, discard any abandoned staged import of that name and
   create the metric in the store.

## Store write, step by step

`storeShared` (`persister/shared.go`) → `Store.UpdateManyMaintained` →
`updateContinuation` (`internal/chunkstore/continuation_codec.go`):

```
lock stripe ─▶ metadata (ring cache) ─▶ create if absent
     │
     ▼
pop batch from cache (points stay visible until confirmed)
     │
     ▼
NewIndexedBatch ─▶ continuationIO(reader = batch, writer = batch)
     │
     ▼
ContinuationCodec.Update  (internal/whisperio/continuation_codec.go)
  1. newContinuationFiles + prefetchNames      Visit n/ (≤ 64 names)
  2. whisper.OpenWithOptions(main file)        Get manifest; Visit H pages (v7)
  3. UpdateManyForArchive(points)              go-whisper's own algorithm:
       ├─ compressed archive append ─▶ AppendStorageBlock
       │     Get descriptor b|archive|block
       │     Get predecessor group (decoded once, reused)
       │     Set group(s) c|… or t|bucket|…   (exact encode + zstd)
       │     Set descriptor
       ├─ block rotation (append from slot 0) ─▶ DeletePrefix(old groups)
       │     then a fresh descriptor in the current hour's bucket
       ├─ header changes ─▶ writeHeader marks dirty 128 B pages
       └─ out-of-order points ─▶ OOO sidecar file (virtual)
  4. maintenance: compactContinuation          merge the OOO sidecar when due
  5. Close + flush                             Set dirty header pages / manifest,
                                               namespace changes, file deletes
     │
     ▼
revision++ and last-update in r|id|gen  ─▶  batch.Commit(NoSync)
     │                                        periodic WAL sync (store-sync-interval)
     ▼
SharedWriteGuard ─▶ confirm the batch in the cache
```

- **Indexed batch.** The update reads its own writes, so every read merges
  the batch with the database (memtables and tables). Its prefix walks
  (`Visit`: names, header pages, v6 block groups) and its probes of rarely
  rewritten keys (`Probe`: sidecar groups, sidecar and v7 manifests) share one
  iterator over the metric's key prefix and the batch
  (`continuationIO.withIterator`). Its first seek opens a table in each level
  and L0 sublevel holding the prefix; later reads reuse them instead of
  opening them again. A read after a write first refreshes the iterator's view
  of the batch (`SetOptions` with unchanged options keeps the tables open).
  Probes are `SeekPrefixGE` on the whole key, so they consult bloom filters.
  Keys most updates rewrite (block descriptors, the predecessor group, a v6
  main manifest) keep a fresh `Batch.Get`, which stops at the newest version
  where the merged iterator would also read every older version below it.
  Time-bucketed keys (v7 groups) and reads inside a `Visit` callback use fresh
  lookups too. `store-update-point-lookups = true` gives every read its own
  lookup again; `SIGHUP` switches updates that start afterwards.
- **Commit and durability.** With `store-sync-interval = "1s"` commits are
  `NoSync` and a background loop syncs the WAL; a crash can lose up to the
  interval. Explicit durability (publication of a conversion) goes through
  the WAL syncer, which coalesces requests into one sync.
- **Confirmation.** Popped points remain visible to readers in the cache until
  the persister confirms them under `SharedWriteGuard`, so a concurrent read
  sees the batch in the cache, in the store, or in both.

## Out-of-order points

CWhisper's compressed archives accept appends only in time order. Late points
go to an out-of-order sidecar (`p` groups in a second virtual file).
Maintenance (`ContinuationMaintenance`, thresholds from the persister's
out-of-order policy, rate-limited by a ticker) merges the sidecar back with
`MergeOutOfOrderWithOptions`. A merge rewrites affected archives into a new
virtual file and deletes the old one: `continuationFiles.deleteFile` deletes
the whole file prefix.

Out-of-order bursts follow restarts: the cache dump is restored while newer
points have already been written. Rewrites are much more expensive than
appends (they decode and re-encode whole archives), so restart catch-up is
slower than steady state.

## Deletes inside an update

`continuationIO.DeletePrefix` (`internal/chunkstore/continuation_io.go`):

- **Fresh generation (`pendingKeys`).** The batch holds all of the data, so
  only its own keys are deleted, as point deletes.
- **Indexed batch.** List the visible keys under the prefix and point-delete
  each, up to `continuationPointDeleteLimit` (1<<20), which no virtual file
  reaches.
- **Otherwise.** One range tombstone, counted by
  `pebble_continuation_range_deletes_total`.

Point deletes are deliberate: every range tombstone in a memtable makes the
next reader re-fragment all of that memtable's range tombstones while every
other reader waits ([challenges.md](challenges.md#readers-queued-behind-range-tombstone-fragmentation)).

## Cost of one update

Measured with goroutine samples and CPU profiles on busy hosts:

| Step | Cost driver |
|---|---|
| Metadata lookup | Usually the per-stripe ring cache; a miss is one point `Get`. |
| Namespace and header load | Prefix scans (`Visit`), which cannot use bloom filters and seek every L0 sublevel. The namespace scan is the update iterator's first seek. Paged headers add a scan of up to tens of small keys. |
| Predecessor group | One point read; bloom filters help. |
| Encode | Exact encoding and zstd of each touched 128-point group. |
| Header write | v7: only changed 128 B pages (about half the bytes of a full multi-archive header). |
| Commit | Memtable insert plus WAL append through Pebble's commit pipeline. |
| Background | Flush to L0, then compaction through L6: on v6 roughly 10x the flushed bytes on hot hosts, about half that on v7. |

What makes updates slow in practice, in order of observed impact:

1. **Disk reads** of headers and groups when the block cache is cold (after
   every restart) or when compaction saturates the device.
2. **Read amplification** from many L0 sublevels: each seek visits every
   overlapping L0 table plus one table per level.
3. **Memtable range tombstones** (see above).
4. **Write stalls** when flushes cannot drain the memtable queue.

## Writes to original files

Unconverted metrics are written with stock CWhisper to their `.wsp` files.
Those writes are buffered in the page cache; with long dirty-expire settings
they accumulate (tens of GB on a busy host) and are written back in bursts
that compete with Pebble's syncs. Hosts finish converting before the store
reaches its best throughput.
