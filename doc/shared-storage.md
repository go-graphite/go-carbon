# Pebble-chunk shared storage and embedded buckyd

`pebble-chunk` is an opt-in experimental backend owned by go-carbon. The classic
file backend remains the default. One go-carbon process owns a Pebble database
containing the metric catalog and compressed archive slots. Persistence,
carbonserver and the embedded buckytools transfer API use that same handle. Do
not open the database from a standalone buckyd or another process. The engine
passes the CPU and disk
[integration gate](#qualification-results); deployment qualification remains
required.

## Configuration

```toml
[whisper]
storage-backend = "pebble-chunk"
store-dir = "/var/lib/graphite/shared"
store-cache-size = 268435456
store-memtable-size = 67108864
store-sync-interval = "1s"
# Existing schemas-file, aggregation-file and worker settings still apply.

[buckyd]
enabled = true
bind = "127.0.0.1:4242"
auth-jwt-secret-file = "/etc/go-carbon/buckyd.secret"
node = "carbon-01"
nodes = ["carbon-01:2003=a", "carbon-02:2003=a"]
hash = "carbon"
replicas = 1
tmpdir = "/var/lib/graphite/transfer-tmp"
max_transfers = 4
max_body_bytes = 167772160
```

Pebble reserves each active memtable from `store-cache-size`, so the configured
cache budget needs headroom beyond active memtables for block-cache reads. It
does not limit the process RSS.

The process must be able to write the store and temporary directories. An empty
`store-dir` selects `whisper.data-dir + "/.store-chunks"`. Backend, store memory,
path, sync interval and buckyd settings require restart; ordinary schema changes
apply to new metrics. Existing policies are preserved. Online policy migration
is rejected.

Writes may arrive out of order; archive updates use the shared WAL and require
no per-metric sidecars. `store-sync-interval` defaults to `"1s"`: point updates
return before WAL sync, and a background worker syncs pending updates once per
interval. A process or machine crash can lose updates since the last successful
sync, including points already confirmed out of the cache. Sync delays can
extend that loss window. Idle stores do not issue periodic WAL syncs. Graceful
store shutdown syncs remaining updates.

Set `store-sync-interval = "0s"` to wait for WAL sync on every update batch.
Negative intervals are rejected. Metric creation, deletion and snapshot
imports/replacements always wait for WAL sync, regardless of this setting.
Failed writes are requeued. Receiver acknowledgement still precedes persistence
of an in-memory cache write.

Carbonserver builds trie/trigram indexes from the catalog and uses shared-store
reads for render/info. The normal periodic scan discovers newly persisted
metrics; realtime/cache discovery options retain their existing behavior.
Transfer mutations schedule a catalog refresh at most once every 30 seconds;
the normal `scan-frequency` remains active. Imports complete after a synced
store commit, and the transfer temporary file is then removed. New metrics may
take up to the next batched scan to appear in find/glob results. Shared mode
bypasses response caches so deletion and replacement cannot leave cached
results from an earlier generation. This affects performance and should be
measured on representative workloads before rollout.

Metric count, data-point and logical-size quotas use classic Whisper capacity,
including headers. Namespace physical-size quotas are rejected: compressed
tables and the WAL are shared across metrics. `storage.diskBytes`,
`storage.walBytes`, `storage.memTableBytes` and `storage.cacheBytes` report
store-wide accounting. `storage.cacheHits`, `storage.cacheMisses`,
`storage.chunkMaterializations` and `storage.chunkOperands` expose cache and
chunk-write behavior as counts since the previous stats flush, like every
other go-carbon counter.
Per-metric physical size and file modification time have no shared equivalent;
the transfer metadata reports classic export size, mode `0644`, and mtime `0`.

## Mapping standalone buckyd options

| Standalone option | go-carbon `[buckyd]` setting |
| --- | --- |
| `--bind`, `-b` | `bind` |
| `--tmpdir`, `-t` | `tmpdir` |
| `--node`, `-n` | `node` (empty selects hostname) |
| positional ring members | `nodes` array, same `HOST[:PORT][=INSTANCE]` syntax |
| `--hash`, `--replicas` | `hash`, `replicas` |
| `--auth-jwt-secret-file` | `auth-jwt-secret-file` |
| `--pprof` | `pprof` (empty disables the dedicated listener) |
| `--pyroscope` | `pyroscope` |
| `--timeout` | `timeout`; accepted legacy cache TTL, unnecessary for catalog reads |
| `--prefix`, `-p`, `--cache_path` | `prefix`, `cache_path`; nonempty values are rejected |
| `--sparse`, `--compressed`, `--mtime` | `sparse`, `compressed`, `mtime`; true is rejected |

Filesystem options do not select shared compression or a per-metric directory.
Shared compression is provided by Pebble; network transfers independently
negotiate Snappy. The embedded API provides `/metrics`, `/metrics/{name}` and
`/hashring`, including listing/filtering, HEAD/GET, POST fill, PUT replacement,
DELETE and offloaded copying. Fill preserves existing destination values and
rejects different retention/aggregation/XFF policies with HTTP 409. Replacement
publishes a complete archive generation atomically.

JWT tokens use the existing `X-Buckyd-Authorization` header, HMAC shared secret,
namespace patterns and `read`, `update`, `replace`, `delete` operation grants.
An empty secret-file setting disables authentication, matching standalone
buckyd. Offloaded transfers mint a short-lived token granting `read` for only
the requested source metric; source and destination must share a secret.
Offload sources are independent of `nodes`, which describes the ingestion
hashring using its original ports and instances. Cross-ring `bucky copy
-offload` works with `nodes` omitted; source redirects are rejected. Missing
source metrics return HTTP 404 so the client's `-ignore404` option applies.
pprof runs on its separate configured listener. Both listeners use a 10-second
HTTP header-read timeout.

## Migration and client usage

Use the updated **bucky client** with the existing standalone buckyd on file
nodes and embedded buckyd on shared nodes. The standalone daemon is unchanged.
Metric bodies remain classic Whisper exports, preserving all stored archive
points rather than resampling through render. Compressed file sources remain
supported through go-whisper's snapshot importer; compressed `Mix` policies are
rejected.

`storage-backend = "pebble"` is a retired prototype setting and is rejected.
Migrate it offline through buckyd. Preserve the old binary and legacy store,
quiesce ingestion and drain its cache, then keep the old binary's buckyd
endpoint serving as the source. Start the new binary with an empty separate
`.store-chunks` destination, copy and verify every metric, then switch routing.
Delete the legacy source only after the new destination serves the verified
data. The separate default directory prevents accidental reuse of `.store`.

```sh
bucky copy -src old-node:4242 -dst shared-node:4242 -workers 4 \
  -api-token-file /etc/graphite/bucky.token
# After routing/cutover is ready, normal rebalance/copy -delete can retire sources.
```

GET supplies a revision token; offloaded client moves with deletion obtain one via HEAD.
The updated client sends that token with source DELETE. A write, replacement or
delete/recreate during transfer causes HTTP 409 and retains the source. Failed
copy/delete jobs remain retryable in the client job log. File daemons without
tokens keep their existing deletion behavior. Revision checks detect concurrent
writes but do not implement dual-write routing or merge the receiver cache.

The API uses bounded concurrent temporary exports/imports. Size `tmpdir` for
the classic export capacity of the largest metrics and transfer concurrency;
Snappy exports may also need a second temporary file. `max_body_bytes` bounds
both encoded and decoded imports; increase it deliberately for larger archives.

## Engine and transfer boundary

`internal/chunkstore` owns storage policy and persistence. It does not import
go-whisper; the retired `go-whisper/store` dependency is removed. A chunk contains
up to 128 circular archive slots, with an explicit presence bitmap, timestamp
deltas, and lossless float bits. Snappy is used when it reduces the encoded size.
Keys identify the metric, generation, archive and chunk; the number of live slots
is bounded by the configured retentions.

Updates preserve classic Whisper routing, correction order, aggregation and
XFF behavior. Fine points and their rollups commit in one atomic Pebble
batch. Merge operands use last-mutation precedence, including when an older
timestamp overwrites a circular slot. The format permits fewer than 32 pending
operands; the engine writes a full chunk after four mutations to
reduce repeated merge work. No clock or mutable global state is used by the
merger. Metadata and the mutable revision are separate records.

Fetches and transfer snapshots observe metadata, revision and chunks from one
database snapshot. Replace publishes a new generation atomically. Fill retains
existing non-NaN destination values and rejects policy mismatches. Conditional
delete checks metric identity, generation and revision.

`internal/whisperio` handles classic Whisper import/export and compressed OOO
snapshot imports. Transfers preserve physical archives instead of resampling
render responses. The buckyd integration retains its HTTP, Snappy,
authentication, offload and revision-token protocol. go-carbon retains the root
go-whisper library for classic files and transfer format helpers. Buckytools is a
separate client executable; go-carbon does not import or require that module.

The format marker is `CHUNKSTORE`, containing `go-carbon-chunks-v1`. Opening a
legacy or unknown nonempty directory is rejected before opening Pebble. A
migration must use separate stores and buckyd; there is no in-place conversion.
New store directories, their newly created ancestors and the marker are synced.
Pebble v1.1.5 terminates the process on WAL-sync failure. The test suite checks
this fail-stop behavior for synchronous and periodic sync, plus recovery of
previously synced writes. Periodic-sync tests also discard unsynced filesystem
state to verify the loss window and the final sync on graceful shutdown.

## Qualification gate

For matched work, CPU per committed input point must be at most 1.5 times
cwhisper-OOO, and allocated disk after maintenance at most 50 percent. Reads,
memory, setup cost and durability differences must also be reported. File
engines use their ordinary unsynced write/close lifecycle. The benchmark harness
uses synchronous chunk commits (`SyncInterval: 0`), matching
`store-sync-interval = "0s"`; the application now defaults to periodic sync.
Receiver acknowledgement still precedes persistence.

Repeated Linux VM measurements are authorized for code integration. Deployment
requires qualification on the intended native Linux storage and a representative
production replay. The suggested development host resolves to an Amazon KVM
guest running Linux/amd64 on XFS, with 8 CPUs and 32 GiB RAM. It is useful
additional evidence, not physical-device power-loss qualification.

Qualification builds use Go 1.27.1 and `CGO_ENABLED=0`, matching release builds.
Early local VM screening used CGO and is excluded from the release-build gate.
The local Linux/arm64 VM has 2 CPUs and 2 GiB RAM; its scratch data uses overlayfs.

`TestStorageScale` is opt-in and runs one engine per process. Its synthetic
schema is `1s:10m,10s:1h,60s:6h`, average, XFF 0.5, with 120 seed points, batches
of eight, and one read for every ten writes. Values repeat across metrics, so
these compression ratios are not a production forecast. The timed loop excludes
creation and final maintenance; both are reported separately. Every metric is
checked against classic Whisper after reopening, and a sample of all archives
is checked before and after maintenance. Pebble receives a full compaction;
file OOO merges run only for the 253 sampled metrics. File disk figures include
remaining sidecars, especially for the late-write case; they do not represent
a fully merged minimum footprint. A separate XFS probe of the same late trace
merged all 100 file metrics and used 8 KiB per metric (819,200 bytes total).
Since every metric receives identical values, extrapolating that file size gives
781 MiB for 100k metrics, still above twice the measured 87 MiB chunk footprint.
This is an extrapolation, not a full 100k-metric merge measurement.

```sh
CGO_ENABLED=0 go test -mod=vendor -c -o /tmp/storage.test ./persister
GOMAXPROCS=2 STORAGE_SCALE_ENGINE=pebble-chunk \
  STORAGE_SCALE_METRICS=100000 STORAGE_SCALE_ROUNDS=80 \
  STORAGE_SCALE_WORKERS=32 STORAGE_SCALE_MEMORY_MIB=64 \
  STORAGE_SCALE_CACHE_MIB=256 \
  /tmp/storage.test -test.run '^TestStorageScale$' -test.v -test.timeout 1h
```

Run the identical command with `STORAGE_SCALE_ENGINE=cwhisper-ooo` as the control.
`STORAGE_SCALE_LATE=1` enables alternating late hole-filling batches.
`STORAGE_SCALE_CPU_PROFILE=/tmp/storage.cpu` profiles only the measured loop.
`STORAGE_SCALE_CACHE_MIB` overrides the configured cache budget without changing
memtable size. Pebble deducts memtable reservations from that cache budget, so
equal cache and memtable settings can leave no room for cached blocks. The
setting is not an RSS limit. Chunk runs report cache hits/misses and write counts.

## Qualification results

The accepted configuration materializes after four mutations and uses a 256 MiB
cache budget with 64 MiB memtables. All measured workloads pass the 1.5 CPU-ratio
and 0.5 disk-ratio limits. CPU includes the accompanying read workload and is
normalized by successfully committed input points. Disk uses allocated file
blocks after maintenance and close, excluding directories.

| Workload | CPU µs/point, chunks / OOO | CPU ratio | Allocated disk, chunks / OOO | Peak RSS, chunks / OOO |
| --- | ---: | ---: | ---: | ---: |
| Development host, 100k metrics, 8 rounds, median of 3 | 21.98 / 23.50 | 0.94 | 23 / 781 MiB | 718 / 73 MiB |
| Local VM, 100k metrics, 80 rounds | 4.91 / 6.11 | 0.80 | 87 / 781 MiB | 898 / 169 MiB |
| Local VM, 100k metrics, 80 rounds, late writes | 4.67 / 14.93 | 0.31 | 87 / 1,950 MiB | 935 / 188 MiB |
| Local VM, 1M metrics, 8 rounds | 5.37 / 9.11 | 0.59 | 231 / 7,812 MiB | 973 / 225 MiB |

These measurements establish the integration gate, not a production speedup.
The workload uses short retentions and repeated values. Chunk memory usage is
substantially higher. Development-host timed wall duration was 100–108 seconds
for chunks versus 43–52 seconds for OOO despite similar CPU, with different
durability boundaries. Creation/setup also costs more: 328–341 versus 32–37
seconds there, and 548 versus 67 seconds in the million-metric VM run. Setup is
excluded from the timed CPU figures. The local VM timed durations were 262/301
seconds for sustained ordered writes, 252/773 for late writes, and 277/513 for
the million-metric workload (chunks/OOO).

[Raw logs and environment metadata](chunk-storage-2026-10-04/raw/README.md),
[machine-readable results](chunk-storage-2026-10-04/summary.csv), and an
[exact-field verifier](chunk-storage-2026-10-04/verify.py) accompany this report.
Run `python3 doc/chunk-storage-2026-10-04/verify.py` to check all 12 recorded rows.
Archived measurements retain the pre-rename engine label `pebble-chunks`; the
current configuration and benchmark selector is `pebble-chunk`.

All six aggregation methods and three XFF settings pass the classic differential
suite, including 4,096-batch late-write retention wraps. Targeted race tests,
codec and merger fuzzing, subprocess crash/WAL recovery, malformed chunk bounds,
format rejection, epoch-crossing reads and archive-transfer tests pass. The
timestamp-bound checks also execute on Linux/386. A live local HTTP round-trip
between the preserved legacy Pebble service and the new go-carbon binary verifies
legacy-to-chunks direct transfer and Snappy-offloaded transfers both ways, all
three physical archives, NaN payloads, negative zero, replacement and stale/valid
conditional deletion.
No production data was migrated.

The final integrated source passes Linux `go test ./...` and `go vet ./...`.
The full race run passes every package except the unchanged helper throttle
timing test (895 events versus its 900 minimum). An isolated HEAD baseline
reproduced that failure twice in three runs; the current focused race test
passed all three reruns. No race was reported. See the
[validation logs](chunk-storage-2026-10-04/validation/) and
[source/environment metadata](chunk-storage-2026-10-04/metadata.json).

## Rejected configurations

The original 32-operand prototype failed the development-host CPU gate in three
100k-metric runs of eight rounds: median 46.43 microseconds/point versus 23.34 for
cwhisper-OOO. Allocated disk was about 23 MiB versus 781 MiB. Materializing after
four updates helped the smaller 10k-metric sustained case, but the 100k-metric
development-host result remained 44.87 microseconds/point.

In the release-build local VM run at 100k metrics and 80 rounds, the four-update
candidate used 10.59 microseconds/point versus 6.11 for cwhisper-OOO, and about
87 MiB versus 781 MiB allocated disk. This also fails the CPU ceiling.

CPU profiles put about 59 percent of sampled CPU under Pebble point reads in the
development-host sustained probe. Enabling Bloom filters regressed the
100k-metric development-host result to 72.96 microseconds/point; that experiment
was reverted.

The retained cache-budget change leaves room for actual cached blocks after
Pebble's memtable reservations. These measurements used synchronous WAL
commits; no durability relaxation was used to meet the gate.
