# Experimental v2 operator metrics and catalog scaling

The v2 runtime registers these metrics through the existing application
registry only when `[prometheus] enabled = true`. Existing constant labels
apply. No name, owner, vshard, partition or path labels are added. Metrics are
diagnostics, never replay frontiers or placement authority.

| Signal | Metric | Meaning |
| --- | --- | --- |
| Ingest rate | `rate(v2_ingest_points_total[5m])` | Points accepted by the TCP receiver |
| Commit rate | `rate(v2_committed_points_total[5m])` | Points sealed by this process, including recovery replay |
| Seal/rollup lag | `v2_seal_lag_seconds`, `v2_rollup_lag_seconds` | Age of the oldest nonempty owner head; both stages commit at the same boundary |
| Queued work | `v2_buffered_points`, `v2_dispatch_bytes` | Owner-head points and bytes held by bounded producer dispatch leases |
| Parts by level | `v2_parts{level="0"}` through `level="6"` | Physical Pebble SST files, not logical archives or portable part manifests |
| Merge debt | `v2_compaction_debt_bytes` | Pebble's estimated bytes of remaining compaction work |
| Merge work | `v2_compacted_bytes_total` | Bytes written by LSM compactions during this process |
| Disk occupancy | `v2_live_sst_bytes`, `v2_obsolete_bytes` | Live SST bytes and obsolete bytes eligible for deletion |
| Seal work | `v2_seals_total`, `v2_seal_seconds_total` | Seal attempts and time across owners; attempts include failures |
| Reads | `v2_read_duration_seconds` | Histogram per FetchMany, including errors, cancellation and the consumer callback |
| Catalog | `v2_catalog_names` | Current discovery names, including the classic union |
| Write mode | `v2_dual_write_enabled` | Zero for the currently supported sole-writer v2 runtime |

Lag excludes receiver/journal/dispatch queue time. Use the lag gauges with
the queue gauges and ingest/commit rates; do not treat an empty owner head as
proof that upstream queues have drained. Rollups execute during sealing, so
there is currently no independent asynchronous rollup queue. Recovery commit
rates cannot be compared directly with new TCP ingest rates.

Physical lifecycle GC reclaimed bytes and live move progress require telemetry
from their respective lifecycle/copy owners. These collectors do not yet expose
those signals. Their absence does not mean zero work. Obsolete bytes exclude
Pebble zombie tables retained by readers and do not measure reclaimed disk bytes.

VictoriaMetrics uses the same distinction between pending rows, current parts,
merge counters and bounded cache bytes; see
[vmstorage metrics](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vmstorage/main.go)
and [storage caches](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/lib/storage/storage.go).
The go-carbon metrics retain the existing component prefix and Prometheus
`_total`, `_bytes` and `_seconds` conventions.

## Repeatable producer catalog measurements

`TestNameCatalogScale` is opt-in. Compile once, then run create and recover in
separate processes at 1M, 5M and 20M names. The test never opens a daemon data
root or starts a network listener. Use a fresh absolute scratch directory for
each count and set `GOMEMLIMIT=24GiB` (the Go heap limit is a soft runtime limit).

```sh
GOTOOLCHAIN=go1.27.0 CGO_ENABLED=1 go test -mod=vendor -c \
  -o /tmp/catalog-scale.test ./internal/partlog
PARTLOG_SCALE_NAMES=1000000 PARTLOG_SCALE_DIR=/tmp/catalog-scale-1m \
  PARTLOG_SCALE_PHASE=create GOMEMLIMIT=24GiB /tmp/catalog-scale.test \
  -test.run '^TestNameCatalogScale$' -test.v -test.timeout=12m
PARTLOG_SCALE_NAMES=1000000 PARTLOG_SCALE_DIR=/tmp/catalog-scale-1m \
  PARTLOG_SCALE_PHASE=recover GOMEMLIMIT=24GiB /tmp/catalog-scale.test \
  -test.run '^TestNameCatalogScale$' -test.v -test.timeout=12m
```

Each phase writes JSON and a heap profile to that scratch directory. JSON
includes CET time, phase duration, live heap per name, heap objects and peak
process RSS. Linux also includes current RSS from `/proc/self/status`.
Creation measures group-compressed WAL ingestion and sync; recovery measures
opening that durable WAL. The fixture has evenly mixed 64/96/128/192-byte names
and average/sum/last policies with one/two/three archives. Each definition
contains its actual name, matching v2 application policy encoding.

This isolates the producer name catalog. It excludes the core rollup state,
occupied archive slots, read-side metadata, discovery and quota indexes,
receiver queues and daemon startup orchestration. It cannot establish full
daemon memory or a production restart deadline. These are synthetic assumptions,
not sampled production name or schema distributions. Recovery runs on the same
machine may benefit from the filesystem cache.

`BenchmarkCatalogRefreshCompare` compares the previous discovery equality
check with `Snapshot.EqualNames`. The latter avoids materializing a second
string-header array on each refresh and preserves pinned immutable views.
