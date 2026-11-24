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

## Go memory policy

With v2 enabled, startup defaults to `GOGC=75` to reduce garbage retained
between collections. An explicit `GOGC` value (including `off`) wins. The
Go runtime still honors an explicit `GOMEMLIMIT`; this growth target does not
bound RSS. File mappings, native allocations and filesystem page cache need separate
accounting. Classic startup retains the runtime's GC growth setting.

## Part identities

`[v2] id-keys` defaults to `true`. Seals use vshard-local numeric IDs from
the store's name dictionary, reducing repeated names in immutable parts.
The dictionary remains the source of truth; requests carry their existing
name through part routing and cursor selection without a global name cache.
Existing name-keyed parts remain readable alongside ID-keyed parts.

Set `id-keys = false` before creating a store to retain name-keyed seals.
Once a store contains ID-keyed parts, keep ID keys enabled when reopening it.
This setting is applied at process startup.

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

Sealed-part retirement also exports `v2_part_gc_pending_parts`,
`v2_part_gc_removed_parts_total`, and `v2_part_gc_unlinked_bytes_total`.
The byte counter measures removed original links, not physical disk reclaimed:
hard-link copies and reader pins can retain those blocks. Move progress and
physical allocation reclamation remain outside this metric contract.

### Visibility and seal latency

`/admin/info?scopes=v2-write` exposes process-lifetime, point-weighted
`append_to_visible` p50/p99 and operation-weighted `seal_total`, `rollup_plan`,
and `publication` p50/p99. It also returns TCP receiver counters and the active
producer/shard counts. This measures the real daemon path, including rollups;
it does not require an in-process shadow dispatcher.

The append clock is recorded when a receiver owner appends decoded points.
Network, sender and receiver queue delays before that call are excluded. Compare
`receiver.Accepted` with an independent arrival counter to detect upstream
backlog. Visibility is observed only after successful reader publication;
republishing the same owner head does not count its points again.

The append clock has one-second resolution. Returned finite quantiles are
conservative upper bounds with up to 1.01s of rounding for append-to-visible,
and 10ms for stage durations. `overflow` counts observations at or above
120.01s; a quantile in that bucket is `null`, since it has no finite upper bound.
Seal total includes rollup planning and publication. These nested stage
quantiles have different weighting from visibility and cannot be added together.
The observer keeps bounded histograms and producer frontiers per owner and
creates no input queue or publication barrier.

## Adopt a copied range

The experimental unplaced L3 daemon exposes move control on its existing
carbonserver HTTP listener. Use a retained coherent snapshot served by the
existing whole-file transport (`storectl sync --parts --snapshot-dir <snapshot>
--part-listen 127.0.0.1:8090`). Copy and adopt with:

```sh
storectl move --url http://127.0.0.1:8080 --action copy \
  --from http://127.0.0.1:8090
# The first JSON line contains ID, before any file is transferred.
# A replacement client resumes against the same retained donor snapshot:
storectl move --url http://127.0.0.1:8080 --action copy \
  --from http://127.0.0.1:8090 --id <ID>
storectl move --url http://127.0.0.1:8080 --action status --id <ID>
```

The client streams `/snapshot.json` and `/parts/<checksum>` from the existing
whole-file server into `POST /admin/v2/move/begin` and
`PUT /admin/v2/move/part?id=<ID>&part=<checksum>` on the receiver. It holds no
progress state. For a token-protected donor, pass `--part-token-file <file>`;
the token is sent only to the donor and is never stored in the move record. `GET /admin/v2/move/status?id=<ID>` reports the phase, each file's
name/size/checksum/state, per-owner states and staged/active parts and bytes,
verified parts/bytes done and total, average decimal MB/s since move creation,
ETA for remaining transport bytes, transferred bytes, verification count and
failures with reasons. ETA is zero when throughput is unknown or copy is done;
it does not estimate boundary-repair/activation time. Failures retain a total
count and the latest 16 reasons.

The receiver owns one fsynced, atomically renamed
`<v2.path>/moves/<ID>/record.json`. Files advance through
`pending → received → verified → staged → active` (staging/activation also have
per-owner receipts). `received` means the whole file and its transport checksum
are durable; `verified` additionally certifies descriptor/rollup semantics.
An interrupted file can be resent. A received file waits for its descriptor
without being resent; a verified file is neither fetched nor verified again.
After restart the receiving owner catalog reconciles activation that committed
just before its receipt. Already active owners are skipped. The same ID keeps
the original adoption clock; an attempt to change it fails. Progress decreases
are refused, logged at ERROR and counted; the client exits non-zero on errors.

`POST /admin/v2/move/adopt?id=<ID>` can retry activation independently. `copy`
with a finished ID needs no donor and is a no-op. After all owners activate, the
receiver syncs a small done receipt, then releases transport links and the
preparation read index. Startup repeats interrupted cleanup. The done receipt
retains totals, owner completion states, throughput and failure history.
Discovery includes adopted names on its normal one-second refresh.

The local-directory compatibility path remains available:

```sh
storectl move --url http://127.0.0.1:8080 --action prepare \
  --parts-dir /absolute/path/to/received-snapshot
storectl move --url http://127.0.0.1:8080 --action adopt --id <ID>
```

Preparation verifies and pins the files before acknowledging; source links may
then be removed. Existing legacy prepared requests import their verified
certificate without rereading SSTs. The first move upgrades the root's restart
format to version 2, so older daemons refuse it before opening stores. Unknown
move-record versions likewise prevent writable startup and administrative
changes. This is a forward-only format change, including after completion.

With Prometheus enabled, `v2_move_progress{move,measure}` exposes `phase`,
`parts_done`, `parts_total`, `bytes_done`, `bytes_total`,
`megabytes_per_second`, `eta_seconds`, `failures`, `bytes_transferred` and
`verifications`. `v2_move_owner_state{move,owner}` reports each owner;
`v2_move_regressions_total` counts rejected regressions in this process.
Phase/state numbers are pending=0, received=1, verified=2, staged=3, active=4,
done=5. The existing Graphite collector exports the same observations under
`v2.move.<ID>.*`, with `owner.<index>.state` and `v2.move.regressions`.
Failure reasons remain in status, avoiding unbounded metric labels. With the
local metric endpoint, the collector feeds the ordinary v2 TCP intake; remote
metric endpoints continue to use the existing sender.

Names use the receiving daemon's producer catalog and owner placement, which
may differ from the donor's geometry. Each receiving owner atomically activates
its bindings, receiving identities and boundary repairs. Other owners can
continue intake. Visibility across different owners is gradual until `Done`;
this endpoint alone does not switch external routing or retire the donor.

Copied SST bytes stay unchanged. Mature coarse history needs no raw coverage:
the ordinary newest-timestamp slot rule selects between copied and target
coarse slots, with receiving live class breaking equal timestamps. Only coarse
windows touched by retained receiving raw are recomputed from resolved raw
slots. If such a window crosses a fine retention cutoff, its older neighbors
must still be present; otherwise adoption fails before activation instead of
installing a partial aggregate. This check does not apply to untouched mature
windows. The catalog stores repairs rather than duplicating old history.
Version-1 move baselines remain readable; older binaries reject the new
version-2 authority, so do not downgrade a root after adoption.

The endpoint currently requires unplaced L3 intake. Source producers must own
disjoint canonical names; competing donor streams for the same name, moving
previously adopted stores, and lifecycle-reset reconciliation are outside this
protocol. Incomplete state and links remain under `<v2.path>/moves/<ID>` for
retry; completed moves retain only their small done receipt.

### Seal threshold

`[v2] seal-interval = "10s"` sets the restart-only maximum time between
owner seals (zero selects the same default; negative values are rejected).
`seal-max-rows = 131072` is the independent restart-only size threshold
(zero selects this default; negative values are rejected). It limits synchronous
seal work and retained pending samples. An original application remains intact,
so a packet can cross this threshold; the existing 2,097,152-row hard limit still
bounds staging to 64MiB per owner, plus detached original batches, policies and
bounded dispatch queues. The usual 131,072-row threshold is 4MiB of row storage
per owner before original-batch and packet overhead. No merge policy changes. Existing explicit control barriers still force seals.
When inactivity expiry is configured, its metadata check retains a separate
one-second cadence; new metadata and due fenced expiry may force an earlier seal.

Pending heads publish independently every 500 ms, including idle tails. The
last unpublished tail is also published before synchronous seal work starts. Fine
and coarse reads combine pending original samples with sealed data; rollups
are still computed exactly from fine samples at seal. Each publication writes
its new L0 rows before making them reader-visible; the production L0 path has
no producer journal. Restart recovers those runs until their seal is committed.
The shorter publication cadence reduces visibility delay independently of the
seal interval, at the cost of more frequent L0 publications. Measure CPU, device
writes and restart time when qualifying that cadence.
Changing this threshold does not change the on-disk format.
