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
| `store-compaction-rate` | 0 (off) | **R.** Aggregate compaction SST output bytes/s; positive for a fixed budget, `-1` for automatic pacing from local SST ingress, measured write amplification, recent output and debt growth (floor 8 MiB/s). Auto bypasses during warm-up, high debt, L0 or memtable pressure, write stalls and conversion. Shutdown releases every mode. WALs and ingested tables stay unpaced; flushes have a separate budget. |
| `store-compaction-rate-headroom` | 2.0 | **R.** Auto mode's multiplier on the modelled compaction rate, from 1 to 4. Lower values smooth write bursts but slow catch-up; debt growth still adds budget. |
| `store-compaction-pacing-warmup` | `1h` | **R.** Auto mode's unpaced learning time after start, from `0s` to `24h`. Zero starts pacing after the first sample. Reloads measure from the original start; the rate EWMA keeps its one-hour time constant. |
| `store-compaction-debt-bypass` | true | **R.** Auto mode only: stop pacing when debt exceeds twice its learned baseline and 4 GiB. Set `false` to keep pacing as debt builds; debt growth still raises the budget. Stalls, L0 depth, memtable pressure and conversion still bypass. Watch debt, L0 depth and write stalls when reducing pacing budgets. |
| `store-flush-rate` | 0 (off) | **R.** Exact-block stores only: memtable flush SST output bytes/s; positive for a fixed budget, `-1` for 4× the one-hour EWMA of SST ingress (floor 4 MiB/s, after the first observed ingress). Every mode releases a flush a caller waits for, write stalls (plus five minutes), memtable pressure, a second queued memtable, conversion and shutdown. WAL recovery flushes run at full speed. `AsyncFlush` stays paced. |
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
| `store-time-buckets-relayout-workers` | 1 | Workers sharing an ascending chunk queue, 1–32. Restart required. All workers share the rate above; each buffers about 64 MiB of group values, plus planning overhead (a single metric may exceed the target). Chunk boundaries can flush smaller groups; one worker retains serial grouping. |
| `store-time-buckets-relayout-min-free-bytes` | 214748364800 (200 GiB) | **R**, positive. Pause new relayout plans/ingests and reclaim spans below this filesystem reserve; resume automatically when free bytes recover. Reserve space for foreground writes, admitted groups and overlapping SST outputs. |
| `store-time-buckets-relayout-max-compaction-debt` | 34359738368 (32 GiB) | **R**, positive. Pause copies and each new reclaim span above this Pebble debt estimate. One merged reclaim runs at a time, using `store-compaction-rate`. Independent of cold-pack's limit. |
| `store-time-buckets-relayout-reclaim-bytes` | 34359738368 (32 GiB) | **R**, positive. Copied bytes per shared reclaim batch; cap by half available space above min-free, reserving admitted copies. Keep well above overlapping L6 file size to amortize rewrites. Reclaim at the limit and completion; durable journals preserve unfinished work across cursor progress/restarts. Smaller limits reclaim sooner at higher write amplification. Cancellation may join one full batch span. |

Relayout reclamation also pays for sparse metadata flush overlap. Pebble starts
with ten L0 target file sizes of grandparent overlap and can enlarge it; a large
`store-target-file-size` without a smaller `store-l0-target-file-size` can make
that cost substantial. Size the reclaim budget above both this overlap and L6
boundary files, and measure compaction write bytes per copied byte.

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
  attempts, and so most of its CPU and the store's write volume. **R.** Zero
  is unlimited, positive values keep fixed pacing, and `-1` enables automatic
  pacing. Auto needs `cache.writeout-min-points` and `writeout-max-delay` and
  applies to all storage backends. A fixed cap well above average demand can
  empty the eligible queue in a burst, then idle until the next batching cycle.
- **`max-updates-per-second-auto-min`** (default `1`) and
  **`max-updates-per-second-auto-max`** (default `0`, unlimited) bound auto
  pacing in updates/s. Both are **R**. The floor must be positive, and a
  positive ceiling must be at least the floor. Pressure, age and recovery
  guards respect the ceiling. Auto uses one token budget through restart,
  recovery and shared commits; each metric in a commit spends one token.
  Restart CPU admission still applies. Positive fixed rates retain their
  existing restart and pressure behavior.
- **Auto eligibility.** Metrics have stable, independently hashed phases
  repeated every `writeout-max-delay`. Restored durable points use the same
  phases; early writes do not shift subsequent phases. This replaces relative
  delay spread in auto mode, while the spread point threshold still applies.
  Sparse points also use these phases. Held startup windows begin at release.
- **Auto controller.** Every five seconds it samples cache arrivals, eligible
  metric count, point count and capacity. Its steady target is cached metrics
  divided by max delay, with 3% headroom. Demand and effective rate follow
  increases immediately and decreases with a one-minute time constant.
  Additional guards raise the target when point count exceeds 120% of recent
  peaks, point capacity reaches 70%, byte capacity is under pressure, or the
  eligible queue cannot drain before the oldest arrival reaches max delay
  plus slack. Slack is `max(30s, min(240s, max-delay / 4))`. Urgent restoration
  targets the ordinary delay instead of bypassing the limiter. Age also uses
  `input_log_oldest_unpersisted_age_seconds` when the log is enabled.
  The age bound needs sufficient worker/storage capacity and a sufficient
  ceiling; storage stalls and an undersized ceiling can violate it.
- **Auto observations.** `persister_auto_update_rate` is the effective token
  limit, including restart and recovery. `persister_auto_update_rate_target`
  is the bounded target before downward smoothing, and
  `persister_auto_update_rate_reason{reason}` identifies the current target's
  reason. `persister_auto_update_rate_adjustments_total` counts rate changes
  by reason across reloads. Changes of at least 25% produce an INFO log.
  Graphite stats expose `autoUpdateRate`, `autoUpdateRateTarget` and
  `autoUpdateRateReason.*`. These observations exist only in auto mode.
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
| Flush pacing | `pebble_flush_rate_bytes`, `pebble_flush_pacer_target_rate_bytes`, `pebble_flush_pacer_bypasses_total{reason="queue"}` | Compare the effective budget with the target and bypass reasons. Rising queue bypasses mean flushes cannot keep up at the configured rate. `pebble_flush_pacer_bytes_total` and `pebble_flush_pacer_wait_seconds_total` track paced work and token waits. |
| Range tombstones | `rate(pebble_continuation_range_deletes_total)` | Should be zero. |
| Write share | `persister_pebble_updates_total` vs `persister_whisper_updates_total` | Conversion progress by load. |
| Block cache | `pebble_block_cache_{hits,misses}_total` | Hit ratio of 97%+ is normal on a warm host. |
| Ingestion | `pebble_ingest_*_total` | L0 bytes and memtable overlaps should stay near zero. |
| Conversion | `store_migration_*` | Workers, in flight, skipped, waiting for space. |
| Relayout | `store_relayout_*` | Rate, descriptor progress, pending/reclaimed bytes and batches, failures and pressure pauses by reason. |
| Host | dirty page cache, device utilization | A large dirty backlog delays every store sync. |

Diagnosis method that paid off repeatedly: take a goroutine dump
(`/debug/pprof/goroutine?debug=1`) and classify persister workers, flushes and
compactions by their top frames before changing any setting.

## Temporary maintenance boosts: November 2026 observations

These settings were measured in production in November 2026. “Steady” means
the baseline used in those measurements, not a compiled default. Anonymous
dataset labels match [resource costs](resource-costs.md) and
[cold-pack measurements](../cold-pack.md#production-measurements-24-november-2026).
Use current setting definitions above when applying these historical results.

**R** = applies on `SIGHUP` (`systemctl kill -s HUP --kill-who=main go-carbon`,
then check `go-carbon.log` for `config successfully reloaded`); otherwise the key
only takes effect at the next restart. A `SIGHUP` that carries a changed
restart-only key is rejected as a whole, so other changes in the same reload do
not apply either.

### Rules before boosting

1. **Boost one host first, with a peer.** Compare against a replica that keeps
   steady values, at equal request rate: store updates/s, `cache_points`,
   `pebble_compaction_debt_bytes`, L0 bytes, read p99 (5-minute window) and CPU.
2. **Observe health and retain known steady values.** Investigate updates below
   10% of the comparison peer while cache grows; read p99 above
   max(2× peer, peer + 5 ms) for 3 samples; debt above
   max(2× peer, peer + 8 GB, 12 GB) for 3 samples; or L0 above 10 GB.
   These were comparison thresholds for this workload, not universal limits.
   Signals require operator investigation; they do not automatically revert settings.
3. **Boost one lever at a time.** Do not stage a boost into a restart wave or
   combine it with a cache-size change.
4. **Write the end date down.** Leftover boosts outlive their reason: on 4 Nov,
   37 stores still ran 64 cold-pack workers at 1 GiB/s from a finished rollout,
   and 78 stores still had the compaction boost the next morning. Scan the fleet
   and return to steady values.
5. **Reconcile configuration management.** Record any temporary override and
   its expiry with the owner of the persistent configuration.

### Compaction catch-up

| Key | Steady | Boost | Effect | Undo |
|---|---|---|---|---|
| `store-compaction-rate-headroom` | 1.5 | 4.0 (max) | **R.** Multiplies the auto pacer's compaction budget. With debt bypass it let relayout and the first cold-pack pass run about 2× faster on D4/D2. | `SIGHUP` back to 1.5. |
| `store-compaction-debt-bypass` | false | true | **R.** Stops pacing entirely when debt exceeds twice its learned baseline and 4 GiB. | `SIGHUP` back to false. |

- Steady values verified on 5 Nov: store-D4-2 and store-D5-2 ran 1.5/false
  for 8 h against peers on 4.0/true with equal updates, cache and p99; debt
  swings between 0 and about 12 GB and drains on its own.
- Use the boost only while a large one-off job (relayout, first cold-pack pass,
  conversion) is compaction-bound. D3 was left out of the rollout boost.
- `store-compaction-workers` (R, 10-16 on large hosts) is the other knob; raise
  it only when L0 piles up and the disk has headroom.

### Cold-pack (exact)

| Key | Steady | Boost | Effect | Undo |
|---|---|---|---|---|
| `store-cold-pack-workers` | 8 | 64 (first pass) | **R.** Parallel packing workers. 64 workers at 1 GiB/s finished first passes in hours instead of days. | `SIGHUP` to 8. |
| `store-cold-pack-bytes-per-second` | 268435456 (256 MiB/s) | 1073741824 (1 GiB/s) | **R.** Read + batch byte budget shared by the workers. | `SIGHUP` to 256 MiB/s. |
| `store-cold-pack-interval` | `24h` | `10m` (first pass only) | **R.** Pass spacing. The measured release re-read all history on repeat passes to find a few new blocks (D6 10-36 MB/s for 4-60 blocks/s; D2 read p99 35 vs 15 ms during a pass), so never shorten it after the first complete pass. | `SIGHUP` to `24h`. |
| `store-cold-pack-max-compaction-debt` | 32 GiB D1 / 128 GiB others | — | **R.** Cold-pack pauses above this debt; leave it as the brake. | — |
| `store-cold-pack-write-window` | `24h` (default) | `6h` | **R.** Packs ranges older than the window. store-D4-3 at 6h: net −7.4% disk against its peer in 6 h, equal p99/updates. Changing it resets the cursor, so the next pass re-reads everything (+0.5 cores for its duration). | `SIGHUP` back to `24h`. |

- **Near-pause instead of off** when a pass hurts reads: 1 worker at 1 MiB/s
  keeps the cursor and costs almost nothing.
- **Historical float-repack experiment** (D6) at 8 workers
  starved persister writes through compaction pressure (updates 6.4k → 600/s in
  2 min); 2 workers at 32 MiB/s was safe.
- Repeat-pass read cost depends on the current watermark implementation;
  see [cold-pack](../cold-pack.md). The near-pause result above is historical.

### Relayout (v6 → time buckets)

| Key | Steady after relayout | Boost | Effect | Undo |
|---|---|---|---|---|
| `store-time-buckets-relayout-rate` | 300 MiB/s D1 / 1 GiB/s others (finished stores) | 1 GiB/s | **R** for a positive rate; 1 pauses. Turning it on/off needs a restart. | `SIGHUP` to a lower positive rate, or 1 to pause. |
| `store-time-buckets-relayout-workers` | 4 D1 / 8 others | 8-32 | **Restart only.** Work-stealing workers share the rate. A `SIGHUP` with a changed value is rejected, so edit it only right before a restart. | Edit the file before the next restart. |
| `store-time-buckets-relayout-reclaim-bytes` | 256 GiB (32 GiB D1 default) | — | **R.** Copied bytes per reclaim batch. Smaller batches keep disk growth bounded; without reclaim, disk grew by about the bytes copied (store-D7-1 +0.9 TB/h). | — |
| `store-time-buckets-relayout-min-free-bytes` | 200 GiB | — | **R.** Free-space brake; check free space before any relayout. | — |
| `store-time-buckets-relayout-max-compaction-debt` | 128 GiB (32 GiB D1) | — | **R.** Debt brake. | — |

### Persister throughput

| Key | Steady | Boost / lever | Effect | Undo |
|---|---|---|---|---|
| `max-updates-per-second` | -1 (auto) | fixed cap | **R.** Lower caps mean fewer, fuller updates: host CPU −41-57% in cap tests, at the cost of a larger cache (memory, crash-loss window, dump size). Too low a cap lets the cache grow without bound (D1 concurrency 24: 1.7M committed vs 3.1M points/s intake). | `SIGHUP` back to -1. |
| `max-updates-per-second-auto-max` | 22000 | higher | **R.** Ceiling for auto catch-up after a restart. | `SIGHUP`. |

- Restart catch-up is CPU-bound (4-8× CPU for 6-12 min after a restart). Small
  stores once wrote 0/s for hours because their restart CPU allowance was below
  their mandatory work; the restart admission budget and warm-up gate govern it.
- `GOMAXPROCS` 64 gave no win; `GOGC=150` (D2/D1) saves 7-11% RSS at equal CPU.

### Changes that looked like boosts but were not

- **Block-cache cuts** (store-D7-1 to 4 GiB: about 0% hits). Change cache sizes
  only as a separate guarded canary, never inside a rollout.
- **Cold-level zstd** (`store-compression-cold = "zstd"`): −4.2% disk for 2-2.7
  cores on store-D1-4; parked.
- **Prefix range cache v1**: read p99 2.5 → 5-6 ms; v2 fixed it.
- **Pebble updates per commit** above 1: no gain at 2/4/8.

### Restarts and relays

A store restart is a relay event: each relay queues about 5 s of points per
store (`HostQueueSize`), so a store that is unreachable longer drops points at
the relays. On 5 Nov 2026 one dataset's relay pool had shrunk to 8 relays per region;
release restarts dropped 6.3M and 7.6M points in two regions until 14 relays per
region were restored. Before a restart wave, check relay count and queue size
with the relay owner and watch `nanotube_throttled_host_records` per store.
