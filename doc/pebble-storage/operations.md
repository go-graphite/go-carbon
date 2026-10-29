# Shared-store operations

The [operator tools](../../tool/ops/README.md) cover canaries, migration/reclaim
status, fleet comparisons, capacity estimates, input logging, stale artifacts,
SMART collection and expiration-policy generation. They operate on explicit
inputs, with local fakes for their tests. They do not require a particular fleet
layout or monitoring provider.

## Dashboard

Import [operations-dashboard.json](operations-dashboard.json) into Grafana using
its dashboard import UI. Select a Prometheus datasource, the go-carbon scrape job,
the target label (default `instance`), and the targets to compare. The datasource
is a variable; the file has no fixed datasource or dashboard identity. Target-label
names must be valid Prometheus label identifiers. A relabeled installation can use
`host`, for example. All panels follow that label, including the phase CPU legend.
Avoid selecting jobs whose target identifiers refer to unrelated services.

The dashboard includes process/HTTP health, incremental migration and reclaim,
persister stage cost, Go GC/heap and metadata-cache memory, continuous input-log
coverage and stage costs, Pebble compaction/ingestion I/O, sampled phase CPU,
heartbeat failures, and receiver/read restart gaps. Values have explicit units;
missing samples remain gaps. Hover descriptions explain counter scope and
measurement limits.

Feature prerequisites are U04 (phase CPU), U05 (restart/heartbeat), U08 (I/O),
U10/U11/U12 (migration and reclaim), U15/U16 (cache accounting) and U21 (input log),
with U29 required for the complete persister stage instrumentation. These are
feature-slice identifiers from the submission stack, not release or GitHub PR
numbers. Earlier builds can use the available panels, with missing capabilities
left visibly empty. Scraping must be enabled. Phase sampling and heartbeat/input
logging must be configured before their optional panels have data.

Process CPU per point includes read traffic, GC, migration and compaction. The
stage series measure worker elapsed time and may exceed process CPU. The dashboard
uses Pebble point/update counters for those panels; use the CLI's `--backend
whisper` for an original-file comparison or adjust those dashboard queries.
During mixed-backend/mirror conversion, stage time is not exclusively attributable
to one backend. Compare completed stores and equivalent traffic windows. Never
average histogram bucket quantiles to claim a fleet p99.

This is an importable static artifact; no dashboard publishing script or live
Grafana write is needed. Offline checks verify panel IDs, layout and variable
references. Live query and visual rendering checks still belong to the importing
operator because no monitoring endpoint is assumed.

## Reclaim, free space and discard

After migration, distinguish logical ownership, original-file reclamation,
empty-directory pruning, filesystem free space and SSD discard. They are separate
steps with different evidence:

1. Use `status.py` to inspect the complete receipt. Require shared phase, no active
   conversion, no skipped originals and no error before source-tree pruning.
2. Follow reclaim queue/completion counters and record free bytes and used inodes
   for the source mount. Use U28's `store-prune-empty-dirs` with its own readiness,
   lock, exclusion and no-follow safeguards. Complete read-only inventories before
   and after can confirm remaining originals/empty directories; truncated samples
   cannot prove completion.
3. Check compaction debt, L0 pressure and write stalls. File deletion frees
   filesystem blocks; Pebble key deletion can retain space until compaction.
   Do not remove live SST, WAL, lock, checkpoint or archive files by hand.
4. On SSD storage, inspect discard support through every filesystem, device-mapper
   and RAID layer. A filesystem reporting free space does not prove the SSD has
   received discard for it. Run trim only after checking support and headroom.

## fstrim guidance

On Linux, inspect the selected mount and device topology locally:

```sh
findmnt --target /var/lib/carbon
lsblk --discard
systemctl status fstrim.timer
```

Choose your actual data mount rather than assuming it shares a device with `/`.
Record write/read latency, compaction throughput/debt, stalls and device utilization
before trimming. Test one host from one failure domain first, and watch its peers'
traffic/latency. Preserve enough serving replicas while conversion or maintenance
changes read availability. For an immediate local run on a supported filesystem:

```sh
sudo fstrim --verbose --minimum 524288 /var/lib/carbon
```

This example requests a minimum contiguous discard range of 512 KiB. Kernel,
filesystem and device support determine the actual behavior. Trim can contend
with foreground I/O; avoid overlapping trims on replicas or already saturated
storage. Use a detached local service for long operations if your terminal is
unreliable, and inspect its exit status and journal before advancing to another
host. Do not launch a fleet-wide trim from a guessed host list.

Prefer the distribution's `fstrim.timer` when it meets your mount/scheduling needs.
If a mount-specific schedule is needed, install neutral, administrator-owned
units through your configuration management. For example:

```ini
# go-carbon-fstrim.service
[Unit]
Description=Discard unused blocks on the go-carbon data mount
RequiresMountsFor=/var/lib/carbon

[Service]
Type=oneshot
ExecStart=/usr/sbin/fstrim --verbose --minimum 524288 /var/lib/carbon
```

```ini
# go-carbon-fstrim.timer
[Unit]
Description=Periodic discard for the go-carbon data mount

[Timer]
OnCalendar=weekly
RandomizedDelaySec=6h
Persistent=true
Unit=go-carbon-fstrim.service

[Install]
WantedBy=timers.target
```

Verify the local `fstrim` executable path before installation. Select a frequency
from workload/device measurements; a sample weekly schedule is not a recommendation
for every SSD. Do not also enable a second overlapping schedule for the same mount.
Randomization reduces coincidence; it does not enforce replica coordination, so
schedule related replicas in explicit separate maintenance windows. Afterward,
compare the same I/O/latency counters and SMART write/error measurements. A trim's
reported byte count may include already-discarded ranges and is not a direct
measurement of recovered capacity or NAND wear reduction.

## Metric expiration

`store-expiration-delete-workers` defaults to 8. A scan feeds bounded batches of
up to 512 candidates to these workers. The delete rate is shared by every worker;
the successful-delete cap includes in-flight reservations, with slots returned
for conflicts and pending cache writes. Raising workers alone does not raise the
configured rate or per-sweep cap.

Each worker checks pressure after pacing and queueing. L0, debt or a write stall
pauses further deletes for the rest of that sweep; already started batches may
finish. Pending points are checked again before a batch enters the store, and
metadata identity/generation/revision are validated under writer stripes. New
arrivals after that check stay queued for persistence and recreate the metric if
deletion wins. Shutdown and reload join every worker before closing the store.

Expiration uses atomic NoSync WAL batches. Catalog, revision, data tombstones and
sidecar inventory recover together. A crash may lose an expiration; the next sweep
retries it. A later durable write also syncs preceding deletes, and each sweep
starts an asynchronous flush after joining its deletions, including partial or
cancelled sweeps. Administrative single-metric deletes retain their sync policy.

Expiration does not invalidate render caches. A cached reply may include an expired
name for up to the normal 60-second query-cache TTL, or until the cache is cleared,
as with ordinary ingestion. This keeps unrelated cached reads warm without extra
work on cache hits. The catalog change journal and index refresher still remove
expired names from discovery. Administrative mutations retain their base cache
invalidation behavior.

Local scale probes use `TestPrepareExpirationScaleStore` in bounded one-million
name increments. Set `EXPIRATION_SCALE_DIR` and `EXPIRATION_SCALE_ADD`; preparation
compacts the exact-format fixture. Run destructive benchmarks on independent
copies with `-benchtime=1x`: `BenchmarkExpirationScaleDelete` accepts
`EXPIRATION_SCALE_BATCH`, `EXPIRATION_SCALE_WORKERS`, `EXPIRATION_SCALE_DELETES`
and `EXPIRATION_SCALE_SYNC=1`. `BenchmarkExpirationScaleSweep` measures the full
scan and deletion path. Both report CPU per delete and settled compaction bytes;
compaction settling is outside the deletion timer.
