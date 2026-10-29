# Persister timings, runtime CPU and memory: metrics and dashboard rows

This note lists the metrics that show where persister time, CPU and memory go
on a `pebble-chunk` (shared store) host, and the dashboard panels built from
them. The panels follow a "timings" layout: each stage has a wall-time panel
and a work-sum sibling, plus a total and a max/mean/quantile spread.

## Metrics

### Persister stage timing (`internal/stagetime`)

| Metric | Type | Meaning |
|---|---|---|
| `persister_stage_seconds_total{stage}` | counter | Seconds updates spent in the stage, summed across workers |
| `persister_stage_calls_total{stage}` | counter | Calls of the stage |
| `persister_update_duration_seconds` | histogram | Wall time of every update (metric writeout), 64 µs to 8.4 s buckets |
| `persister_update_seconds_max` | gauge | Slowest update since the previous scrape (reset on scrape) |

Stages, in update order:

| `stage` | What it covers |
|---|---|
| `total` | The whole writeout (`Whisper.store`), recorded for **every** update |
| `lock_wait` | Waiting for the persister's per-metric mutex and the store's metric lock |
| `route` | Metadata lookups that choose the backend (original file or store) and prepare the metric |
| `load` | Reading the metric's virtual file names and header |
| `tail_read` | Reading and decoding stored block groups the update extends (Pebble `Get`/`Visit`) |
| `decode` | The whisper update logic itself: merging points and propagating rollups, excluding nested reads and encodes |
| `encode` | Encoding changed blocks and headers into the write batch, excluding reads it issues |
| `ooo_merge` | Folding out-of-order sidecar points into the archives |
| `commit` | Applying the write batch to Pebble |
| `original_write` | Writing a metric still owned by its original whisper file |

**Sampling.** Only `total` is recorded for every update. One update in
`stagetime.SampleEvery` (8) times its other stages, and their counters are
scaled by 8, so rates estimate the whole workload while most updates stay
off the clock. Stage times are exclusive: `decode` excludes the reads and
encodes it issued, and `encode` excludes the reads inside it. Time that
belongs to no stage (for example cache confirmation after the commit) is in
`total` only.

**Work.** Counters sum time across workers, so
`rate(persister_stage_seconds_total[1m])` is "worker-seconds per second": the
number of workers busy in that stage on average, like a CPU-total sibling.
For `total` it is the count of busy workers; compare it with the configured
worker count to see saturation. These are wall-clock times: a stage waiting
on disk or a lock counts the same as one burning CPU. Use profiles (below)
for CPU.

**Overhead.** On the update-path benchmark
(`internal/whisperio` `BenchmarkContinuationUpdatePath`, about 23 µs per v6
update and 32 µs per v7 update on a laptop), the instrumentation costs about
126 ns per update averaged over sampled and unsampled updates
(`internal/stagetime` `BenchmarkUpdateOverhead`): 0.4–0.6%, within the
benchmark's noise. Production updates are larger, so the share is smaller.

### CPU by stage: on-demand profiles

A sampled update labels its goroutine with `persister_stage=<stage>`. Take a
CPU profile from the pprof listener and break it down by label:

```sh
curl -s "http://127.0.0.1:7007/debug/pprof/profile?seconds=30" > cpu.pprof
go tool pprof -tags cpu.pprof                                  # CPU per persister_stage value
go tool pprof -tagfocus persister_stage=decode -top cpu.pprof  # functions inside one stage
```

Only one update in 8 carries labels, so labelled samples are a representative
subset of persister CPU; unlabelled persister samples are the other updates.
Labels are cleared when the update finishes.

### Runtime CPU and memory classes

go-carbon registers client_golang's Go collector with every runtime metric
(`/.*`), so these already exist:

- CPU: `go_cpu_classes_{user,gc_total,gc_mark_assist,gc_mark_dedicated,gc_mark_idle,gc_pause,scavenge_total,idle,total}_cpu_seconds_total`
- Memory: `go_memory_classes_{heap_objects,heap_unused,heap_free,heap_released,heap_stacks,os_stacks,metadata_*,profiling_buckets,other,total}_bytes`
- GC: `go_gc_cycles_total_gc_cycles_total`, `go_gc_heap_goal_bytes`, plus `go_goroutines` and `go_threads`
- Process: `process_cpu_seconds_total`, `process_resident_memory_bytes`

### Memory consumers

| Metric | Meaning |
|---|---|
| `pebble_block_cache_bytes` | Pebble block cache in use (a Go heap allocation in pure-Go builds) |
| `pebble_memtable_bytes` | Memtables, including queued ones |
| `pebble_table_cache_entries` | Open table readers (each holds a file descriptor and index state) |
| `cache_points`, `cache_metrics` | Points and metrics in go-carbon's write cache |
| `cache_estimated_bytes` | Estimated write-cache memory: 16 B per point plus about 160 B per metric |
| `store_migration_working_bytes_reserved` | Conversion working memory currently reserved |

### Pebble compactions and filesystem I/O

| Metric | Type | Meaning |
|---|---|---|
| `pebble_compactions_total{kind}` | counter | Completed compactions by kind: `default`, `move`, `delete_only`, `elision_only`, `read`, `rewrite`; `multi_level` is a subset of `default` (do not sum it) |
| `pebble_fs_ops_total{op,kind}` | counter | Filesystem operations: `op` = `read`, `write`, `sync`, `create`, `remove`; `kind` = `table`, `wal`, `manifest`, `ingest`, `other` |
| `pebble_fs_bytes_total{op,kind}` | counter | Bytes read and written by file kind |

File kinds are classified by name: `NNNNNN.sst` is a Pebble table, `.log` the
WAL, `MANIFEST-*` the manifest, other `.sst` names go-carbon's staged
conversion (ingest) tables, and everything else (directories, markers,
options) is `other`. Pebble v1.1 does not tell compaction reads from point
reads, so **table reads cover compactions and user/persister reads**, and
**table writes cover flushes and compactions**; compare with
`pebble_flush_bytes_total` and `pebble_compaction_{read,write}_bytes_total` to
split them. A `sync` is a full sync (fsync/fdatasync, including those
batched by `store-parallel-table-syncs`); partial writeback requests are not
counted. Directory syncs, which precede every manifest commit, are syncs of
kind `other`. Counting costs about 2.4 ns per operation
(`BenchmarkCountingFileRead`), negligible beside the system call.

## Dashboard rows

`SEL` below stands for the dashboard's host selector, for example
`job="go_carbon", host_name=~"$host.*"`. Rates use `$__rate_interval`.
Panels are per host (`sum by (host_name)`).

```json
[
  {"row": "pebble: by host", "title": "updates", "unit": "ops",
   "expr": "sum by (host_name) (rate(persister_stage_calls_total{SEL, stage=\"total\"}[$__rate_interval]))",
   "description": "Persister writeouts per second."},
  {"row": "persister timings", "title": "total", "unit": "short",
   "expr": "sum by (host_name) (rate(persister_stage_seconds_total{SEL, stage=\"total\"}[$__rate_interval]))",
   "description": "Workers busy in updates on average: the persister is saturated when this reaches the configured worker count."},
  {"row": "pebble: by host", "title": "update duration", "unit": "s",
   "exprs": {
     "max": "max by (host_name) (persister_update_seconds_max{SEL})",
     "p99": "histogram_quantile(0.99, sum by (host_name, le) (rate(persister_update_duration_seconds_bucket{SEL}[$__rate_interval])))",
     "mean": "sum by (host_name) (rate(persister_update_duration_seconds_sum{SEL}[$__rate_interval])) / sum by (host_name) (rate(persister_update_duration_seconds_count{SEL}[$__rate_interval]))",
     "p50": "histogram_quantile(0.5, sum by (host_name, le) (rate(persister_update_duration_seconds_bucket{SEL}[$__rate_interval])))"},
   "description": "Spread of update wall time: the slowest update per scrape, quantiles and mean."},
  {"row": "persister timings", "repeat_stages": ["lock_wait", "route", "load", "decode", "tail_read", "encode", "ooo_merge", "commit", "original_write"],
   "panels": [
     {"title": "<stage>", "unit": "s",
      "expr": "sum by (host_name) (rate(persister_stage_seconds_total{SEL, stage=\"<stage>\"}[$__rate_interval]))",
      "description": "Worker-seconds per second spent in <stage>, summed across workers (sampled stages are scaled)."}]},

  {"row": "pebble I/O", "title": "compactions by kind", "unit": "ops", "stack": true,
   "expr": "sum by (kind) (rate(pebble_compactions_total{SEL, kind!=\"multi_level\"}[$__rate_interval]))",
   "description": "Compactions per second by kind. move and delete_only cost almost no I/O; default rewrites data; read compactions are triggered by read amplification."},
  {"row": "pebble I/O", "title": "table write IOPS", "unit": "iops",
   "expr": "sum by (host_name) (rate(pebble_fs_ops_total{SEL, op=\"write\", kind=\"table\"}[$__rate_interval]))",
   "description": "Write calls to Pebble tables: flushes plus compactions (buffered writes, not device IOPS)."},
  {"row": "pebble I/O", "title": "table read IOPS", "unit": "iops",
   "expr": "sum by (host_name) (rate(pebble_fs_ops_total{SEL, op=\"read\", kind=\"table\"}[$__rate_interval]))",
   "description": "Read calls to Pebble tables: compaction reads plus point and iterator reads that miss the block cache. Pebble v1.1 does not tell them apart."},
  {"row": "pebble I/O", "title": "syncs", "unit": "ops",
   "expr": "sum by (kind) (rate(pebble_fs_ops_total{SEL, op=\"sync\"}[$__rate_interval]))",
   "description": "Full syncs per second by file kind: table syncs (one per flush/compaction output file), WAL syncs, manifest and directory (other) syncs."},
  {"row": "pebble I/O", "title": "bytes by kind", "unit": "Bps",
   "exprs": {
     "read": "sum by (kind) (rate(pebble_fs_bytes_total{SEL, op=\"read\"}[$__rate_interval]))",
     "write": "sum by (kind) (rate(pebble_fs_bytes_total{SEL, op=\"write\"}[$__rate_interval]))"},
   "description": "Bytes read and written per second by file kind (table, WAL, manifest, ingest, other)."},
  {"row": "pebble I/O", "title": "creates / removes", "unit": "ops",
   "exprs": {
     "create": "sum by (kind) (rate(pebble_fs_ops_total{SEL, op=\"create\"}[$__rate_interval]))",
     "remove": "sum by (kind) (rate(pebble_fs_ops_total{SEL, op=\"remove\"}[$__rate_interval]))"},
   "description": "Files created and removed per second by kind: table churn from flushes and compactions, WAL rotation."},

  {"row": "runtime CPU", "title": "process CPU", "unit": "short",
   "expr": "sum by (host_name) (rate(process_cpu_seconds_total{SEL}[$__rate_interval]))",
   "description": "Cores used by the go-carbon process."},
  {"row": "runtime CPU", "title": "Go CPU classes", "unit": "short", "stack": true,
   "exprs": {
     "user": "sum by (host_name) (rate(go_cpu_classes_user_cpu_seconds_total{SEL}[$__rate_interval]))",
     "gc mark assist": "sum by (host_name) (rate(go_cpu_classes_gc_mark_assist_cpu_seconds_total{SEL}[$__rate_interval]))",
     "gc mark dedicated": "sum by (host_name) (rate(go_cpu_classes_gc_mark_dedicated_cpu_seconds_total{SEL}[$__rate_interval]))",
     "gc mark idle": "sum by (host_name) (rate(go_cpu_classes_gc_mark_idle_cpu_seconds_total{SEL}[$__rate_interval]))",
     "gc pause": "sum by (host_name) (rate(go_cpu_classes_gc_pause_cpu_seconds_total{SEL}[$__rate_interval]))",
     "scavenge": "sum by (host_name) (rate(go_cpu_classes_scavenge_total_cpu_seconds_total{SEL}[$__rate_interval]))"},
   "description": "Estimated CPU by class from the Go runtime: user code, GC phases, scavenger. Excludes idle."},
  {"row": "runtime CPU", "title": "GC CPU fraction", "unit": "percentunit",
   "expr": "sum by (host_name) (rate(go_cpu_classes_gc_total_cpu_seconds_total{SEL}[$__rate_interval])) / sum by (host_name) (rate(go_cpu_classes_total_cpu_seconds_total{SEL}[$__rate_interval]) - rate(go_cpu_classes_idle_cpu_seconds_total{SEL}[$__rate_interval]))",
   "description": "Share of busy Go CPU spent in garbage collection. Mark assist steals time from request goroutines."},
  {"row": "runtime CPU", "title": "GC cycles", "unit": "ops",
   "expr": "sum by (host_name) (rate(go_gc_cycles_total_gc_cycles_total{SEL}[$__rate_interval]))",
   "description": "Completed GC cycles per second."},
  {"row": "runtime CPU", "title": "goroutines / threads", "unit": "short",
   "exprs": {"goroutines": "max by (host_name) (go_goroutines{SEL})", "threads": "max by (host_name) (go_threads{SEL})"},
   "description": "Goroutines and OS threads. Threads far above cores usually mean goroutines blocked in syscalls (disk I/O)."},

  {"row": "memory", "title": "RSS", "unit": "bytes",
   "expr": "max by (host_name) (process_resident_memory_bytes{SEL})",
   "description": "Resident memory of the process."},
  {"row": "memory", "title": "Go memory classes", "unit": "bytes", "stack": true,
   "exprs": {
     "heap objects": "max by (host_name) (go_memory_classes_heap_objects_bytes{SEL})",
     "heap unused": "max by (host_name) (go_memory_classes_heap_unused_bytes{SEL})",
     "heap free": "max by (host_name) (go_memory_classes_heap_free_bytes{SEL})",
     "stacks": "max by (host_name) (go_memory_classes_heap_stacks_bytes{SEL} + go_memory_classes_os_stacks_bytes{SEL})",
     "metadata": "max by (host_name) (go_memory_classes_metadata_mcache_inuse_bytes{SEL} + go_memory_classes_metadata_mspan_inuse_bytes{SEL} + go_memory_classes_metadata_other_bytes{SEL})",
     "other": "max by (host_name) (go_memory_classes_other_bytes{SEL} + go_memory_classes_profiling_buckets_bytes{SEL})"},
   "description": "Memory the Go runtime maps, by class. Released heap is excluded: it no longer counts toward RSS."},
  {"row": "memory", "title": "store block cache", "unit": "bytes",
   "expr": "max by (host_name) (pebble_block_cache_bytes{SEL})",
   "description": "Pebble block cache in use; part of heap objects in pure-Go builds."},
  {"row": "memory", "title": "memtables", "unit": "bytes",
   "expr": "max by (host_name) (pebble_memtable_bytes{SEL})",
   "description": "Pebble memtables, including those queued for flush."},
  {"row": "memory", "title": "write cache", "unit": "bytes",
   "exprs": {"estimated bytes": "max by (host_name) (cache_estimated_bytes{SEL})"},
   "description": "Estimated memory of go-carbon's write cache (16 B per point plus about 160 B per metric). Points and metrics: cache_points, cache_metrics."},
  {"row": "memory", "title": "open tables", "unit": "short",
   "expr": "max by (host_name) (pebble_table_cache_entries{SEL})",
   "description": "Open Pebble table readers. Fewer than the store's table count means tables are reopened on reads."},
  {"row": "memory", "title": "conversion working set", "unit": "bytes",
   "expr": "max by (host_name) (store_migration_working_bytes_reserved{SEL})",
   "description": "Memory reserved by in-flight conversions."}
]
```
