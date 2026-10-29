# Incremental CWhisper → shared exact storage

> Design overview, data layout, write and read paths, tuning and production
> lessons: [Pebble store design notes](pebble-storage/README.md).

Shared exact storage is opt-in. CWhisper, including its OOO sidecar,
remains authoritative until conversion has verified every timestamp and value.
Performance and whole-service resource use require independent qualification
with representative traffic, restart recovery and current-write rollback.

## Online conversion and resume

```text
CWhisper authoritative
  → capture main + OOO under existing writer fence
  → stage archives and exact writer state, invisible to readers
  → independently verify timestamps, float bits and policy
  → drain original readers; sync data + ownership + cleanup record
  → shared-only reads/writes
  → archive if configured, then unlink originals/sidecars/locks; sync parent
  → finish cleanup record and continue
```

Incoming writes stay in the normal cache while their metric converts. Other
metrics continue; only ownership publication drains that metric's original
readers. Before publication CWhisper remains authoritative; afterwards shared
storage remains authoritative even if cleanup fails or the process crashes.
New metrics start directly in shared storage.

Resume drains durable cleanup and walks remaining originals. It never reimports
completed metrics or repeats a full-store audit. Catalog IDs and source-directory
identity survive restart. Keep the original directory and incremental flag after
completion. A completed source walk is not performance acceptance.

A bounded 4,096-name write-fed priority queue gives hot metrics earlier service;
after three priority jobs workers check cold scan work to avoid starvation.
Hints are nonblocking and best effort. Both paths share all worker, memory, disk,
verification and publication guards; there is no guard-exempt conversion budget.

Find/info/render merge original and shared-owned names during migration using
the compact mapped FST. Completed migrations also build this mapped index from
the committed catalog, with bounded disk sorting and no original-tree walk.
The catalog change journal extends a mutable overlay for new names; deletions,
packed-policy changes, journal resets and an overlay exceeding 100,000 metrics
replace the mapped generation from the catalog. Per-metric details are retained
only when internal stats are enabled. Graceful shutdown reuses this generation
and saves only its mutable overlay, including pending cache names.

Configurations without a v2 file-list cache retain the compatibility heap-based
builder; its full-node startup/RSS needs separate qualification before disabling
the compatibility index path.

## Configuration and operation

Use a fresh empty destination physically outside the source tree:

```toml
[whisper]
storage-backend = "pebble-chunk"
store-encoding = "exact"
data-dir = "/var/lib/graphite/whisper"
store-dir = "/var/lib/graphite/shared-exact"
store-incremental-migration = true
store-mirror-files = false
store-live-migration = false
flock = true
store-migration-workers = 16
store-migration-control-file = "/var/lib/graphite/migration-control.json"
store-migration-bytes-in-flight = 268435456
store-migration-min-free-bytes = 8589934592

[carbonserver]
enabled = true
trie-index = true
file-list-cache = "/var/lib/graphite/files.flc"
file-list-cache-version = 2

[tags]
enabled = false
[buckyd]
enabled = true
bind = "0.0.0.0:4242"
auth-jwt-secret-file = "/etc/go-carbon/buckyd.secret"
```

The embedded Buckyd API uses the same open shared store as the persister. Stop
the standalone filesystem Buckyd before assigning its port to go-carbon, and
retain the existing JWT secret and namespace/operation grants. Keep incremental
mode enabled: it selects the v6/v7 reader and preserves the migration marker.
An unconverted source remains outside the shared management API (HTTP 404).
Deleting a converted metric whose original-file cleanup is still pending returns
HTTP 409; do not bypass that ownership check. Once cleanup is durably complete,
deletion removes the shared metric and queues a combined source/catalog index
scan. Changes are coalesced at 30-second intervals; unconverted files remain
visible. A shared metric's reported physical size of zero does not mean that
the metric is an empty or missing Whisper file. Flattened PUT/POST transfers
remain unsupported for continuation state and return HTTP 409 before upload.

The persister must be enabled; schema migration is disabled. Physical quota
restrictions apply. Only go-carbon may write the source tree during conversion:
coordinate original-file expiry, OOO merge, transfer and rebalancing jobs. Keep
cross-process file locks; process-local locks cannot fence cron or overlapping
handoff workers. On 1202, local-time quiet windows cover the actual 01:00 and
03:00 maintenance schedules. On 1201 they also cover its 06:00 OOO merge,
from 05:55 to 08:10. Process detection extends a pause if jobs overrun.

`tool/store-migration-control` renews the version-1 worker lease. `workers: 0`
pauses admissions; raising the live operator cap resumes without losing progress
or restarting go-carbon. Admission grows through 1/2/4/8/16/24/32/48/64 workers,
checking end-to-end request tails, CPU/RSS, backlog, real storage stalls, physical
I/O and free-space reserve. Internal disk-wait phases differ by backend and are
diagnostic, not a cross-backend latency gate. Actual request regressions remain
a gate. Lease expiry falls back to one worker. Pending cleanup remains eligible
with zero workers or low admission headroom. Nineteen policy tests pass.

Status is available through `/admin/info?scopes=cache,storage` and
`store-dir/INCREMENTAL-STORE-MIGRATION.json`. Counters describe the current migration run and can reset on config reload
or restart; ownership and reclaimed sources are durable checkpoints. Peak extra
space includes in-flight capture/staging, index sort runs, WAL and ordinary
compaction. Byte reservations are not a total RSS cap; measure physical free
space separately. Diagnostic reflinks retain old extents and must be cleaned up.

With `store-ooo-direct-writes = true`, the final check waits in phase `sidecars`
until the sidecar inventory is ready, logging its progress every ten seconds.
An empty source tree does not prove that a legacy store has no sidecars. Complete
an applied scan through `POST /admin/store/sidecars?action=scan&limit=1000`,
passing each returned `after` value to the next request until `done` is true.
The scan must start without `after`; a dry run cannot establish readiness.
Migration then resumes automatically. Remaining sidecars still fail the check;
inspect and fold them through the sidecar API before retrying. A migration that
already failed the readiness check on an older build can be retried with a config
reload (`SIGHUP`) after the scan completes; the store stays open and the source
walk resumes from the remaining files. Installing this fix requires the usual
binary restart, but recovery itself does not require one.

`storage.migration.skipped_metrics` maps each rejected metric name to its last
conversion error; the same map is saved in the migration receipt. Individual
rejections also log the metric name and keep its original file authoritative.
A run with any skipped sources leaves `source_complete` false, retaining mixed
discovery and incremental writes. Repair those source files, then reload the
configuration (`SIGHUP`) or restart to retry them. Each new run walks the remaining
originals again and resets its skipped counters and names; durable completed
owners are retained. Do not clear completion by editing the receipt or deleting
authoritative source files.

Both pilots now install `tool/store-migration-control/volume_binding.py` as a
systemd `ExecStartPre`. A root-owned proof records the actual XFS UUID and all
source/store/archive root inodes while bindings are valid. A later device-number
change can update only those binding fields, under the engine's fcntl record
lock; changed UUIDs, paths or inodes fail. An unchanged same-boot start only
validates, preserving handoff behavior. Thirty policy/helper tests and native XFS
checks pass, including live-engine exclusion and interrupted repair. Installation
restarted neither app. See the [controller README](../tool/store-migration-control/README.md#reboot-stable-volume-binding).

V6 imports physical writer state; flattened replace/fill/window corrections are
rejected. V4/V5 stores are never silently relabelled V6. Once originals are
reclaimed, rollback requires a compatible V6 executable or restoration/backfill
from a replica/backup. Archived originals are cutover snapshots and lack later
shared-only writes. Keep compatible binaries ready.

Shared expiration is currently disabled on the pilots. Legacy expiry jobs do not
expire converted metrics; matching their retention/activity rules is required
before fleet acceptance. Conversion preserves source mtime as activity; later
shared writes refresh it.

## Reproduce the paired storage diagnostic

```sh
go build -mod=vendor -o /tmp/continuation-replay ./tool/continuation-replay
/tmp/continuation-replay -plan /private/corpus/plan.json \
  -operations /private/corpus/operations.jsonl \
  -work-dir /private/new-paired-run -procs 4 -profile
```

Version-1 plans record the sampling population, allocated main/OOO bytes,
inclusion probability, pre-trace capture time/hashes, engine budgets and warmup.
JSONL preserves actual batches, arrival times, float bits and read windows.
Missing/changed inputs fail without resampling. CPU includes background engine
work; conversion and final forced-fold verification are outside measurement.
Final read hashes, writer state, reopens and source hashes must match.

`-prepare-ingress` generates diagnostic batches from recorded ingress using the
8-point/2-s cache policy; these are not captured production flushes. `-unpaced`
is a functional diagnostic. The single-writer tool always reports
`performance_qualified=false` and does not replace full application/find testing.
