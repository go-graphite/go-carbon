# Tuning

Numbers below come from a large production host unless stated: about 3M
active metrics receiving about 3M points/s, a store of about 1 TB on one NVMe
device that sustains about 700 MB/s of mixed writes, 1.5 TB of RAM, 192
cores. Scale them to your hosts.

## Settings

R = applies on `SIGHUP`; otherwise a restart is required.

### Store engine

| Setting | Default | Guidance |
|---|---|---|
| `storage-backend` | `files` | `pebble-chunk` enables the store. |
| `store-dir` | `data-dir/.store-chunks` | Use a separate directory; incremental conversion needs a new, empty one on a separate tree. |
| `store-encoding` | `legacy` | `exact` for continuation and incremental modes. |
| `store-cache-size` | 256 MiB | Pebble block cache (plus a range-deletion cache of 1/8, at most 1 GiB). Not an RSS limit. The large host ran about 100 GB; the persister's header and group reads dominate hits. |
| `store-memtable-size` | 64 MiB | One memtable. 2 GiB on the large host. |
| `store-memtable-max-unflushed-bytes` | 8 GiB | Queue of memtables before writes stall (`max(4, min(16, budget/size))` memtables). Deeper absorbs bursts (restart catch-up) but lengthens WAL replay after a crash: sixteen 2 GiB memtables once took nearly seven minutes. 16 GiB on the large host. |
| `store-compaction-workers` | 0 (= 1) | **R.** Concurrent compactions. Pebble raises concurrency with compaction debt up to this cap. 10–16 on the large host. Lower it when compactions saturate the disk and flushes wait behind them; raise it when L0 piles up and the disk has headroom. |
| `store-compaction-rate` | 0 (off) | **R.** Aggregate compaction SST output bytes/s; positive for a fixed budget, `-1` for automatic pacing from local SST ingress, measured write amplification, recent output and debt growth (floor 8 MiB/s). Auto bypasses during warm-up, high debt, L0 or memtable pressure, write stalls and conversion. Shutdown releases every mode. WALs, flushes and ingested tables stay unpaced. |
| `store-compaction-rate-headroom` | 2.0 | **R.** Auto mode's multiplier on the modelled compaction rate, from 1 to 4. Lower values smooth write bursts but slow catch-up; debt growth still adds budget. |
| `store-compaction-pacing-warmup` | `1h` | **R.** Auto mode's unpaced learning time after start, from `0s` to `24h`. Zero starts pacing after the first sample. Reloads measure from the original start; the rate EWMA keeps its one-hour time constant. |
| `store-l0-compaction-concurrency` | 0 (Pebble 10) | Extra compaction per this many L0 sublevels, within the worker cap. |
| `store-l0-compaction-threshold` / `store-l0-stop-writes-threshold` | 0 (Pebble 4 / 12) | L0 sublevels that start L0 compactions and stop writes. A higher compaction threshold drops more overwritten versions per byte rewritten, at more read amplification. One host ran 8 / 32. Keep the stop threshold well above the compaction threshold. |
| `store-lbase-max-bytes` | 0 (64 MiB) | Base level size. |
| `store-target-file-size` | 0 | Sets every level's table size. Leave at 0: Pebble's 2 MiB L0 doubling per level is right for this workload. |
| `store-l0-target-file-size` | 0 (2 MiB) | Flush table size only. Larger L0 tables mean fewer syncs but each overlaps more of the other sublevels and the base level. 64 MiB raised L0 to 14 sublevels and stalled writes ([challenges.md](challenges.md#large-l0-tables-piled-up-sublevels)). |
| `store-bytes-per-sync` | 0 (Pebble 512 KiB) | Incremental `sync_file_range` during table writes. With a large dirty page cache every incremental sync waited behind write-back; 64 MiB on the large host. |
| `store-parallel-table-syncs` | 0 | Sync new tables this many at a time before the manifest commit, instead of one after another. 64 on the large host: write stalls went from 25% to 0 ([challenges.md](challenges.md#flushes-waited-on-one-table-sync-after-another)). |
| `store-bloom-filter-bits` | 0 | Whole-key bloom filter per table (10 ≈ 1% false positives). Helps point lookups; tables gain filters as compaction rewrites them. Readers without the setting ignore filters, so it is safe to roll back. |
| `store-update-point-lookups` | false | An update's prefix walks and reads of rarely rewritten keys share one iterator over the metric's keys and batch, opening each level's table once, not once per read ([write-path.md](write-path.md#store-write-step-by-step)). `true` gives every read its own lookup again, for comparison or rollback. **R**: updates that start afterwards switch, so both paths can be compared in one process. |
| `store-max-open-files` | 0 (Pebble 1,000) | Table readers kept open. Size it above the table count: hosts with 23,000–37,000 tables and a budget of 8,192 reopened 35,000–48,000 tables per second. 65,536 removed the reopens and cut CPU by about 8%. Stay well inside the process file limit. |
| `store-sync-interval` | `1s` | Periodic WAL sync; a crash may lose up to the interval. `0s` syncs every commit. |
| `store-time-buckets` | false | v7 layout. One-way ([data-layout.md](data-layout.md#format-marker)). |
| `store-time-buckets-relayout-rate` | 0 | Bytes per second moved into the history bucket. **R** for a positive rate; set 1 to pause. Turning it on or off needs a restart. |

### Conversion

| Setting | Default | Guidance |
|---|---|---|
| `store-incremental-migration` | false | Online conversion ([migration.md](migration.md)). |
| `store-migration-ingest` | false | Group fresh conversions into ingested tables. Recommended. |
| `store-migration-ingest-linger` | `1s` | Wait for a group to fill. Lower it only with few workers. |
| `store-migration-ingest-group-bytes` | 64 MiB | Stage bytes per ingested table; entries scale with it (4096 at 64 MiB). Pebble ingests one table at a time, so conversion throughput is about one group per ingestion. Raise it, with more workers to fill groups, for faster conversion at the cost of memory. |
| `store-migration-ingest-parallel` | 4 | Groups building and ingesting at once. |
| `store-purge-invalid-names` | false | Once after start, delete metrics stored under names now refused (over 4000 bytes, or a node over 255 bytes). See [lsm-levels.md](lsm-levels.md). |
| `store-migration-workers` | 0 (4) | Maximum workers; the lease picks the active count. |
| `store-migration-control-file` | "" | JSON lease file; re-read every second. |
| `store-migration-io-slots` | 0 (2,048) | Conversions inside file I/O at once. A conversion that reads slowly holds a slot, so with few slots and a fast disk throughput is slot-bound: raising 640 to 1,920 on a 64-core VM took conversion from about 90 to about 2,000 metrics/s. |
| `store-migration-hint-burst` | 3 | Hints per cold-scan path; 0 = cold first, large = hot first. |
| `store-migration-bytes-in-flight` | 256 MiB | Workspace bound. |
| `store-migration-min-free-bytes` | 8 GiB | Disk reserve for writes, WAL and compaction. |
| `store-migration-reclaim-delay` | `0s` | Batch unlinks by directory after a delay. |
| `store-migration-archive-dir` | "" | Keep originals instead of deleting them; uses the same space. |

### Persister and cache

- **`max-updates-per-second`** sets how many store updates the persister
  attempts, and so most of its CPU and the store's write volume. The stock
  setting on the large host was 162,500.
- **Cache size.** The cache is not a backlog; its steady size is set by
  persister throughput: `cache ≈ receive rate × active metrics ÷ updates per
  second`. A slower persister means a bigger cache, not lost points, until
  `max-size`. Watch the slope, not the level.
- **Cache `max-size`** is **R**: raise it with `SIGHUP` when a host falls
  behind and has memory. The large host used about 40 bytes per cached point.

## Signals to watch

| Signal | Query sketch | Meaning |
|---|---|---|
| Write stall fraction | `rate(pebble_write_stall_seconds_total)` | Share of time writes wait on Pebble (memtable queue or L0 limit). |
| Memtable queue | `pebble_memtable_bytes` vs the unflushed budget | Pinned near the budget means flushes are the bottleneck. |
| L0 | `pebble_l0_sublevels`, `pebble_level_bytes{level="0"}` | Growing means compaction cannot keep up; reads slow down with it. |
| Compaction per update | `rate(pebble_compaction_write_bytes_total) / rate(persister_pebble_updates_total)` | Layout efficiency. v6 about 35–50 KB, v7 about 24 KB on the large host. |
| Compaction input dropped | `1 - rate(pebble_compaction_write_bytes_total) / rate(pebble_compaction_read_bytes_total)` | Share of compaction input deleted or superseded. A delete wave (after a v7 upgrade) raises it well above the host's usual level (6–9% → 31%). |
| Per-level compaction | `rate(pebble_level_compaction_write_bytes_total)` by level | Where write amplification lands. |
| Debt | `pebble_compaction_debt_bytes` | Pebble's estimate; see [challenges.md](challenges.md#compaction-debt-looked-inflated) before reacting to it. |
| Range tombstones | `rate(pebble_continuation_range_deletes_total)` | Should be zero. |
| Write share | `persister_pebble_updates_total` vs `persister_whisper_updates_total` | Conversion progress by load. |
| Block cache | `pebble_block_cache_{hits,misses}_total` | Hit ratio of 97%+ is normal on a warm host. |
| Ingestion | `pebble_ingest_*_total` | L0 bytes and memtable overlaps should stay near zero. |
| Conversion | `store_migration_*` | Workers, in flight, skipped, waiting for space. |
| Relayout | `store_relayout_*` | Rate, progress, failures. |
| Host | dirty page cache, device utilization | A large dirty backlog delays every store sync. |

Diagnosis method that paid off repeatedly: take a goroutine dump
(`/debug/pprof/goroutine?debug=1`) and classify persister workers, flushes and
compactions by their top frames before changing any setting.
