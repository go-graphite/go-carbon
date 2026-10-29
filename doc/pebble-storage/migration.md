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

After a v6 → v7 upgrade, existing blocks are still v6 and move only when
rewritten. `store-time-buckets-relayout-rate` (bytes per second) moves them
in the background (`internal/chunkstore/relayout.go`):

1. Scan the catalog once, then walk metrics existing at start in ID order up
   to that ID ceiling.
2. The codec plans each metric (`RelayoutPlan`): every v6 block except each
   archive's newest is copied byte-for-byte to the history bucket.
3. A group of metrics (64 MiB or 4,096 metrics) is written as one table
   covering only that ID range in the history bucket, then ingested.
4. Under each metric's lock, a block switches only if its descriptor still
   matches the plan: the descriptor flips to v7 and the old groups are
   point-deleted. A block rewritten meanwhile keeps its new state and its
   copies are deleted.
5. The cursor `z/relayout` resumes after restarts.

- **Pacing.** Per interval from a live rate: a stalled period earns no burst
  afterwards. A positive rate change applies on `SIGHUP`.
- **One run.** Only one relayout runs at a time; two runs planning the same
  metric could delete copies the other had made live.
- **Metrics.** `store_relayout_{metrics,blocks,skipped,bytes}_total`,
  `store_relayout_rate_bytes`, `store_relayout_progress_ratio`,
  `store_relayout_done`, `store_relayout_failed`.
- **When it pays.** Relayout moves cold history out of the way of hot
  headers. For workloads whose fine archives roll over within a day or two it
  barely changes compaction (a key-level model showed under 2%), and its
  copies and deletes add compaction while they run. Run it slowly, and pause
  it when the host is compaction-bound.
