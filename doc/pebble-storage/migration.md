# Migration: converting Whisper files online

`store-incremental-migration = true` converts an existing CWhisper tree into
the Pebble store while the host keeps receiving writes and serving reads.
Ownership moves one metric at a time; every converted metric is verified
exactly before its original is reclaimed. Procedure, preconditions and older
measurements are in
[incremental-storage-migration.md](../incremental-storage-migration.md).

## Preconditions

A new, empty, physically separate `store-dir`; `store-encoding = "exact"`; an
enabled, flocked persister; the trie index with `file-list-cache-version = 2`;
tags, buckyd, the rollback mirror and whole-store live migration disabled.
The store records the source tree's path, device and inode in
`INCREMENTAL-FILE-MIGRATION` and refuses a different source later.

## Phases

`/admin/info?scopes=storage` reports `migration.phase`:

```
files ──▶ index ──▶ files ──▶ shared
  │        wait for the        │
  │        carbonserver index  └─ every source converted (or skipped)
  └─ resume pending cleanups first
```

`cancelled` and `failed` end a run; owned data is kept. Counters are per
process run (`store_migration_*`).

## Choosing what to convert

Two sources feed the workers (`carbon/incremental_migration.go`,
`carbon/migration_priority.go`):

- **Cold scan.** `whisperio.WalkMetrics` over `data-dir`, the authoritative
  full walk. Paths must be canonical and untagged.
- **Hints.** After every successful write to an original file the persister
  offers the metric name (`SetIncrementalMigrationPriority`). Hints are
  best-effort (bounded queue of 4,096, deduplicated) and only change order.
- **Burst.** `store-migration-hint-burst` is how many hints are taken before
  the next cold-scan path. `0` converts cold files first; a large value
  converts written (hot) metrics first.
  - **Hot first** moves the write load into the store fastest: most update
    volume sits in a small fraction of metrics.
  - **Cold first** keeps the store's write share, and so its compaction load,
    low while the host converts.

## Admission

Each conversion passes, in order:

1. **Worker budget.** `store-migration-workers` is the maximum. With
   `store-migration-control-file` a local JSON lease sets the active count
   without a restart:

   ```json
   {"version": 1, "generation": 7, "workers": 512, "expires_at": 1792922000}
   ```

   The file is re-read every second. Expiry must be in the future and at most
   five minutes ahead; a newer generation (or the identical lease) is
   accepted. Missing, invalid or expired leases fall back to one worker; a
   valid zero pauses admission while pending cleanups continue. An external
   controller (a small script) can raise and lower the count from cache
   growth, write stalls or latency.
2. **Workspace.** `store-migration-bytes-in-flight` bounds capture, decode and
   verification memory. A larger metric runs alone.
3. **Disk.** `store-migration-min-free-bytes` plus the job's working size must
   stay free, reserved atomically across workers.
4. **I/O slot.** `store-migration-io-slots` (default 2,048) bounds conversions
   inside file I/O at once. Slots are released while a conversion waits for
   ingestion.

## Converting one metric

`persister.(*Whisper).convertFile`:

```
take I/O slot ─▶ lock metric ─▶ mark "converting" (its writeouts wait)
   │
   ├─ already owned? ─▶ skip to reclaim
   │
   ▼
ReadSnapshotIn(source)            capture main + OOO files, decode all archives
   │
   ▼
Store.StageFileSnapshot
   ├─ new name ─▶ stageFileIngest: queue for a grouped ingest
   └─ existing unowned stage ─▶ restage in a batch (drop the old generation)
   │
   ▼
Store.PublishFileOwnership        verify stored values == snapshot, exactly;
                                  write ownership + f/cleanup/<id>; WAL sync
   │
   ▼
reclaim originals                 unlink (or archive) main + OOO file,
                                  sync the directory, CompleteFileCleanup
```

- **Failure before publication** leaves the original authoritative. The
  metric is logged and counted in `store_migration_skipped_total`; the run
  stops only after more than `maxSkippedConversions` such failures.
- **Duplicate work** (a hint and the cold scan reaching the same name) returns
  `ErrConversionInProgress` and is not an error.
- **Crash at any step** is recovered by the next run: staged-but-unpublished
  metrics are restaged, and published metrics with pending cleanup are found
  through `f/cleanup/` and reclaimed.

### Grouped ingestion

Writing a converted history through the memtable would write it again in the
WAL, the flush and every compaction level, and its flushes would stall live
writers. Instead (`internal/chunkstore/file_ingest.go`):

- **ID ranges.** Fresh stages reserve IDs in durable blocks of 1,024, so a
  group's keys occupy a contiguous range no live writer allocates into.
- **Grouping.** A group fills to `store-migration-ingest-group-bytes`
  (default 64 MiB; one metric per 16 KiB, so 4,096 metrics at the default), or
  waits up to `store-migration-ingest-linger` (default 1 s) for more. Up to
  `store-migration-ingest-parallel` (default four) groups build and ingest
  concurrently.
- **Publication.** Each metric's catalog record commits under its own stripe
  lock, after the group's tables are ingested. Free stripes commit in turn and
  busy ones wait concurrently, so a group never holds hundreds of stripes
  while persister updates queue behind it. A name the complete catalog filter
  has never seen skips the existence lookup.
- **Tables.** Each group is written as sorted tables: one of metric-prefixed
  keys, and one per time bucket (v7). Pebble ingests each into the lowest
  level it does not overlap, normally the bottom one, so converted history
  skips the memtable and most compaction.
- **Why linger.** Each ingestion holds a commit sequence number until its
  manifest edit syncs, so later writes wait behind it. Lingering bounds
  ingestion to about one per second instead of one per handful of metrics.

`pebble_ingest_l0_bytes_total` and `pebble_ingest_memtable_overlaps_total` show
when ingestion lands in L0 or overlaps memtables, which should be rare.

### Reclaiming originals

- **Immediate** (default): unlink main and OOO files inside the conversion,
  then sync the directory.
- **Deferred** (`store-migration-reclaim-delay`): queue reclamation, batched
  by directory, at least that long after publication. Freeing inodes next to
  files still being converted made XFS force its log on those opens.
- **Archive** (`store-migration-archive-dir`, same filesystem, outside both
  trees): durably link each original into the archive before removing it.
  This keeps a snapshot of each metric at its cutover (not a live mirror) and
  uses as much space as the originals did. Choose it before conversion and
  keep the setting across restarts; the store refuses to start without the
  archive directory while its archive marker exists.

After reclamation, rolling a host back requires a compatible binary or a
replica copy.

## Operating a conversion

- **Reads.** Converting hosts can be taken out of read traffic (replicas
  serve) and returned automatically when the phase reaches `shared`. Never
  remove more than one replica of a shard.
- **The persister comes first.** Conversion reads whole files and adds ingest
  and compaction work. On a disk-bound host heavy conversion starved the
  persister's own reads and its throughput fell by half or more. Watch
  persister updates per second, not only cache size and stalls, and lower
  the lease when it drops ([challenges.md](challenges.md#conversion-starved-the-persisters-reads)).
- **Write-back.** Until a host finishes, writes to unconverted originals keep
  the page cache full of dirty pages. Store syncs queue behind that
  write-back, so a partially converted host is slower than a finished one.
- **Thread limits.** Each worker can hold an OS thread in file I/O. Very large
  worker counts (above roughly 6,000 on a large host) exhausted threads and
  crashed the process; the I/O slot bound and a controller thread guard
  prevent that.

## Relayout (v7)

After a v6 → v7 upgrade, existing blocks keep their v6 keys until rewritten.
`store-time-buckets-relayout-rate` moves their cold history in the background.
The newest block in each archive remains on its current path until it rotates.
The normal restart maintenance gate still runs before this work.

1. Scan the catalog up to the starting ID ceiling, sort by ID, and let
   `store-time-buckets-relayout-workers` (1–32, default 1) claim ascending chunks.
   Each chunk has `min(4096, max(1, candidates / (workers * 8)))` candidates.
   Groups flush at 64 MiB, 4,096 metrics or a chunk boundary. One large metric
   can exceed the byte target. Workers share the configured copy rate.
2. Before planning and ingestion, check pressure and the shared byte budget.
   Reserve each admitted group's planned bytes across all workers. Ingest its
   unchanged copies as a history-bucket SST, then acquire each metric's mutation
   lock and compare the descriptor with its plan.
3. In **one WAL batch per metric**, switch unchanged descriptors, point-delete
   their old groups (or discard copies of changed blocks), increment the
   revision, and update the group's durable reclaim journal. Maintenance does
   not refresh metric activity. Release mutation locks and planning snapshots
   before any pacing or compaction wait.
4. Finished groups add to a shared filling batch. Keep copying until the next
   group would exceed `store-time-buckets-relayout-reclaim-bytes` (default
   **32 GiB**), or the pending batch reaches that threshold. Available disk
   space can lower the limit: it never exceeds half the free bytes above the
   min-free reserve, allowing for reserved copies that have not finished yet.
   An indivisible group can exceed a deliberately smaller configured threshold,
   but must fit the filesystem budget and owns its batch alone.
5. A short reclaim barrier joins admitted groups and seals their journal records.
   Release the barrier while this batch compacts, allowing other workers to
   fill at most one further batch. One worker owns the synchronous reclaim;
   there is no detached maintenance goroutine. The filling batch and sealed
   batch together still obey the same filesystem headroom cap. Merge their
   journaled deleted spans, keeping old metric keys (`u`) and discarded history
   copies (`t`) separate. Check reserve, debt and disk-sample validity before
   each span, then call stock Pebble `Compact(start, end, true)` synchronously.
   Disjoint file ranges share the normal compaction worker and output-byte
   budgets; overlapping ranges still serialize inside Pebble.
   Only after every span succeeds are that sealed batch's journal keys removed;
   records published by later groups remain. Reclaim a final
   partial batch before marking the run done.

Compacting after every group rewrites the same overlapping L6 files repeatedly.
Batching amortizes those rewrites over many groups. Size the byte threshold well
above the overlapping file size, and measure compaction bytes per copied byte.
The opt-in `TestRelayoutReclaimCadenceMeasurement` uses 48 MiB L6 targets versus
1 MiB groups to expose this cost; tiny per-group files cannot model it. It also
scales the default L0:L6 target ratio (2:128 MiB). Large L0 targets can make sparse
metadata flushes overlap many history files: Pebble's initial grandparent-overlap
budget is ten L0 target file sizes and its flush heuristic can enlarge it. Keep
the reclaim threshold well above that overlap budget too, especially when
overriding target file sizes.

### Cursor and crash recovery

The cursor may advance after a group's descriptor/delete/journal batches commit,
while physical reclamation is still pending. This is safe because the reclaim
obligation is in the **same atomic batch** as the deletes, and precedes the
cursor in the WAL. A durable cursor cannot skip a missing obligation.

The journal is `R/relayout/reclaim/<group-first-ID>`. It records copied bytes and
bounds of keys actually deleted, and is updated incrementally with successful
metric publications. The cursor now uses `R/relayout/cursor`, preserving its
9-byte value. `z/relayout` is read as the legacy fallback. Maintenance keys sort
before the metric data, so they do not introduce a new high-key tail spanning
all remaining `u` keys when a memtable flushes.

On every start or in-process retry, **force startup catch-up before interpreting
the cursor**, including a completed cursor. Read and merge the journal, compact
its spans, and clear it only after success. Already moved descriptors need not
appear in the next plan. A crash after compaction but before journal removal
repeats that compaction safely; a crash or cancellation before all spans finish
retains the entire obligation. Corrupt journal state stops the run with an error.
No on-disk data-codec or Pebble format upgrade is introduced.

Parallel chunks can finish out of order. Cursor publication still uses the
longest completed prefix and cannot cross an unfinished chunk. The journal is
independent of that prefix, so later chunks' cleanup also survives a crash.
A batch generation check prevents queued reclaim requests from needlessly
reclaiming a fresh partial batch after another worker already did the work.

### Pressure, pacing and operation

- **Limits.** `store-time-buckets-relayout-min-free-bytes` defaults to **200 GiB**;
  `store-time-buckets-relayout-max-compaction-debt` defaults to **32 GiB**.
  The reclaim threshold is separately configured by
  `store-time-buckets-relayout-reclaim-bytes`, default **32 GiB**. All are
  positive and reload on `SIGHUP`, without reopening the store.
- **Pause/resume.** New copies and new reclaim spans pause below the reserve,
  above the debt limit, or while disk-usage samples fail. Workers share one
  pressure sample for up to 250 ms; the sampler releases the guard mutex before
  Pebble metrics and filesystem IO. Charge copies published since that sample
  against cached free space, with no reclaim credit until resampling. Paused
  workers recheck every second; limit reloads invalidate samples and wake waiters.
  Raising the reserve above free space still pauses
  new reclaim IO. Partial batches remain journaled while paused. Capacity-less
  in-memory filesystems have no disk cap.
- **IO.** One merged reclaim runs at a time across all relayout workers, using
  the existing `store-compaction-rate` budget. A completed reclaim and any
  following span must pass the debt gate again; they cannot release a queue of
  simultaneous per-group compactions. Foreground mutation locks are not held.
- **Copy pacing.** The rate remains shared across workers, with no burst credit
  from stalls. Live reclaim-threshold changes are checked while copy pacing
  waits as well as before the next group. Worker count and enabling/disabling
  relayout still require a restart.
- **Headroom.** The threshold counts planned copied bytes, not exact filesystem
  allocation. Allow for table overlap, framing, in-flight groups and pinned
  readers. Long-lived snapshots/iterators can delay file deletion. Reserve and
  admission checks stop further growth while that space is unavailable.
- **Cancellation.** Check context before and after each stock Compact call.
  Cancellation joins an already-started **batch span**, potentially 32 GiB plus
  overlapping SSTs, then starts no further spans. This can take much longer than
  a single group. `Close` releases compaction pacing before joining maintenance;
  context cancellation or StopBackground alone still respects the current IO
  budget. Throughput, forced flushes, queues and whole-file overlap determine
  elapsed time. L6's default target is 128 MiB, or `store-target-file-size` when
  overridden; a single metric can also exceed the group target.
- **One run.** Only one relayout runs at a time. Planning the same metric in
  overlapping runs could let one delete the other's newly live copies.

### Metrics

Existing `store_relayout_{metrics,blocks,skipped,bytes}_total`, rate, cursor
progress, done/failed and pressure metrics remain. Cursor progress measures
published descriptors; it can reach the ceiling while a final reclaim is paused.
`store_relayout_done` is set only after the final journal is cleared.

- `store_relayout_reclaim_bytes`: configured threshold per batch; at most one
  sealed batch and one filling batch are outstanding, and headroom may lower it.
- `store_relayout_unreclaimed_bytes`: known pending journal bytes, loaded when
  relayout starts and updated as metrics publish.
- `store_relayout_reclaims_total` and `store_relayout_reclaimed_bytes_total`:
  completed merged batches and the copied bytes they cover. Startup catch-up may
  cover work copied by an earlier process.
- `store_relayout_paused`, `store_relayout_pauses_total{reason}` and
  `store_relayout_pause_reason{reason}`: shared pressure state, one WARN/counter
  entry per continuous pause. Reasons are `low_free_space`, `compaction_debt`
  and `disk_usage_error`; recovery logs once.
