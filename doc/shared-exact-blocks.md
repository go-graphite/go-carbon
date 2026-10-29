# Shared exact-block storage: implementation and conversion

> Design overview, data layout, write and read paths, tuning and production
> lessons: [Pebble store design notes](pebble-storage/README.md).

This is an experimental integration. The production qualification gates have not passed. The
earlier prototype measurements do not apply to this Pebble integration. Measurements below
identify the backend, maintenance schedule and environment used for each run.

## Storage layout and exactness

Set `[whisper] storage-backend = "pebble-chunk"` and `store-encoding = "exact"`.
Each metric is a catalog record and indexed chunks inside shared Pebble SST files and a shared WAL.
There is no metric file or OOO sidecar in the destination. The old Whisper tree is retained separately
for comparison. After cutover it becomes stale unless a live-write rollback bridge is maintained. New stores use the v4 marker and a distinct merger name. Existing v1/v2
stores cannot be opened as exact stores, and older-format mode cannot open v4 stores. The earlier experimental v3 fixtures are also rejected; they remain paired with their original evaluation binary.

## WAL rotation and write latency

Exact stores enable the local Pebble asynchronous rotation patch. Previously a
memtable flush sealed and fsynced the old WAL under the global commit mutex,
stalling ordinary writes. Rotation now installs the next in-memory WAL buffer
and performs the disk handoff in the background. The next file is created only
after the old EOF is durable, then its directory is synced before buffered IO
starts. Flushes cannot reclaim the old WAL before that handoff finishes.

The configured durability policy remains in effect: synchronous mutations wait
for persistence; interval-synced mutations may lose writes since the last sync.
Flush, checkpoint and shutdown wait for their required handoff. Only one handoff
may be outstanding, and a second rotation applies backpressure if disk IO cannot
keep up. This removes the fsync wait from ordinary writes during one rotation;
it does not promise bounded latency during a sustained disk outage. The v4 format
and strict WAL recovery rules are unchanged. Native matched replay now passes the
bounded write-tail and correctness checks described below. Whole-node resource
and migration qualification remains outstanding.

Chunks currently cover 128 sparse circular slots. Present timestamps use signed deltas. Values use
signed integer deltas only when reconstruction to Float64 is bitwise exact within +/-2^53; all other
chunks use byte-stream-split Float64 bits. This preserves signed zero, NaN payloads, infinities and
subnormals. Zstd is applied only if it reduces the encoded body. CRC32 protects the decoded body and
its integer/float interpretation. The existing batch, catalog, revision and snapshot machinery remains
in use. Late writes and corrections update the same shared chunks and propagate rollups atomically.

## Bulk conversion

Build with `go build -mod=vendor -o store-convert ./tool/store-convert`.
For an immutable source tree:

```sh
./store-convert -source /data/frozen-whisper -source-quiesced \
  -source-id host-and-snapshot-id -destination /data/exact-store \
  -workers 2 -cpu 2
```

The converter imports complete archives, rather than replaying historical points through the live
write path. It preserves retentions, aggregation, XFF, OOO corrections and the compressed buffered
rollups that Fetch computes. Physical archive snapshots alone omit these rollups; a differential
six-aggregation test reproduced that loss and now checks migration against Fetch.

The committed catalog is the per-metric checkpoint. Resuming requires the same source identity and
paths. Verification is on by default: every archive timestamp and every Float64 bit is compared with
the imported result, including resumed metrics. Source/destination/copy trees must be disjoint; path
traversal, source symlinks escaping the tree, and tagged filenames without a name manifest
are rejected. A JSONL inventory (`{"Metric":"a.b","Path":"a/b.wsp"}` per line) avoids filesystem
walks and supplies exact names for tagged files. An inventory can be incomplete: use a full
inventory or a final full-tree reconciliation before cutover.

Workers, GOMAXPROCS, block cache and memtable budgets are explicit. Progress includes elapsed time,
metrics, points, resumed checkpoints and errors. Errors retain successful checkpoints and prevent
completion. Compaction is a separately reported phase. The source is never rewritten.

For a live baseline use `-source-live -copy /data/original-copy`. On Linux/XFS, the default
`-require-reflink=true` clones a main file and its OOO sidecar under the same stable path and inode
locks used by the production writer. Parsing, private OOO merging, syncing and encoding happen after
those locks are released. This avoids reading an entire large metric while blocking its writer.
Unsupported reflinks fail; a full read-copy fallback requires the explicit `-require-reflink=false`.
Copies must live on the same filesystem for reflinks. Private OOO snapshots use owned scratch space beside the destination; Linux copies preserve sparse holes and use reflinks when available. Interrupted private snapshots are cleaned on resume. Capture can still wait for source locks or dirty-extent synchronization; it avoids reading the full metric under its writer lock.

Files confirmed deleted during a live scan are audited as vanished and removed from the unserved destination. Missing files in an immutable inventory remain errors. This handles expiration racing a large conversion; final catch-up must reconcile deletions too.

Each metric has its own capture time. This is a live baseline, not a globally consistent database
snapshot. **Live-write catch-up is not implemented yet.** The converter leaves `NEEDS-CATCHUP`, and
go-carbon refuses to serve that destination. `CONVERSION-IN-PROGRESS` similarly blocks serving an
interrupted conversion. Neither marker is an operator bypass: catch-up, exact API comparisons and
performance qualification must be completed before selecting the destination.

## Measurements so far (2026-10-18)

| Case | Current cwhisper | Exact blocks | Result |
| --- | ---: | ---: | --- |
| Short replay CPU seconds, 942 archive writes | 0.489 | 1.863 | Fails CPU gate |
| Short replay final allocated MB | 922.4 | 222.4 | Whole fixture, not byte-weighted fleet estimate |
| Short replay read p99 ms | 11.49 | 15.98 | Insufficient sample for production SLA claim |
| Short replay write p99 ms | 25.41 | 20.61 | 134 requests; no SLA claim |
| Long stress replay CPU seconds | 9.732 | 16.583 | 70,650 archive writes; 25 forced full compactions |
| Long replay CPU seconds, exact incremental compaction | 9.732 | 11.554 | Local control uses forced cwhisper maintenance; native matched maintenance is a separate experiment |
| Long incremental replay peak RSS MB | 24.78 | 42.41 | Process high-water RSS; no full-node memory claim |
| Corpus conversion + full verification | — | 14.2 seconds | 1,310 metrics / 54,320,145 visible archive points / two CPU workers |
| Interrupted conversion | — | 431 resumed, 879 imported | All 1,310 metrics verified; no errors |

The replay oracle reported zero mismatches for both short runs and the exact long stress run.
Codec/application race tests passed; randomized, retention-wrap, crash-recovery and storage-contract
checks include exact mode. Round-trip fuzzing completed 1,429,901 executions without a failure.
The source snapshot/rollup and converter tests pass. Full production/API correctness and native
CPU/memory/latency qualification are still outstanding.

The long CPU profile attributes about 40% of samples to full-store compaction. The original stress
schedule forces a rewrite of the whole cold store every 15 seconds. A separate production-maintenance
replay uses flushes plus Pebble's normal incremental compaction, measures background jobs and waits
for active compactions to settle. Both experiments are retained; changing maintenance cannot turn the
stress result into a pass. Native tests must include compaction debt, disk growth and sustained traffic.

Configured HTTP and streaming render caches are retained. A process-local store token invalidates replies after restore, fill, archive mutation or deletion; ordinary ingestion keeps the same configured TTL as file storage. Differential tests verify cached response bytes and invalidation.

All Go 1.27 packages passed, including the full race suite and vet. A default-ten-minute run timed out in the fsync-heavy persister package; rerunning it with the repository-standard 30-minute deadline passed (690 seconds normal, 645.5 seconds with race instrumentation). Changed storage, cache, converter and application paths also passed focused checks. Native conversion initially imported and verified 97 current metrics / 1,070,020 points in 0.667 seconds; one sampled metric had disappeared since inventory capture. The converter now audits that live deletion rather than treating a stale inventory as corrupt data. Native qualification is in progress.

No production service has been switched to this format.

The compact catalog in format v4 keeps metric names in catalog keys and stores policy, identity and generation in a binary value. Revision/activity records and transaction boundaries are unchanged. The measured decoder uses 48 bytes / one allocation per lookup versus 208 bytes / four allocations for JSON on the three-retention fixture. Local timing is about 21 ns versus 952 ns; this is a microbenchmark, not an end-to-end CPU reduction claim. The previous native v3 binaries and results remain frozen separately.

Fault-injection recovery now runs in both legacy and exact modes: synchronous writes, periodic WAL sync, loss of unsynced writes, and shutdown sync. The strict filesystem discards unsynced OS buffers. Sealed compressed-point corrections after conversion also match cwhisper.

## Dependency regeneration

Run `go mod tidy && make vendor`. The vendor target reapplies `patches/go-whisper-visible-snapshots.patch` without fuzz, alongside the existing Vellum patch. Regeneration was checked against the tested source hashes. The upstream module version is unchanged; remove this maintained patch when an upstream version includes the snapshot APIs and sparse-copy behavior.

## Previous native implementation result (2026-10-19)

| Matched native replay | File storage | Exact v4 |
| --- | ---: | ---: |
| CPU seconds | 11.61 | 5.58 |
| Process high-water RSS MB | 24.59 | 43.06 |
| Final allocated MB | 1,075.52 | 235.82 |
| Read p99 ms | 1.516 | 1.973 |
| Scheduled write p99 ms | 14.698 | 105.476 |
| Replay oracle errors | 0 | 0 |

Each run replayed 70,650 archive mutations and 3,000 reads over 365 seconds, with
write-triggered cwhisper OOO compaction and exact-mode flushes every 15 seconds plus
normal background compaction. The file control preserves any existing classic
files. These are native Linux storage-API results, not full-application latency
qualification or a byte-weighted fleet savings estimate. V3 repeats also showed
lower CPU and a worse scheduled write tail. The fixture shrank 4.56x; this is not
proof of 10x fleet reduction.

All 86,860,585 final points in the exact stores passed timestamp/Float64-bit checks
before and after full compaction/reopen. The real application comparison passed
620 binary reply checks across CarbonAPI v2 and v3 on 97 captured metrics, including
find/info/render, new TCP writes, late holes, duplicate corrections and replies
after restart. Linux reflink and sparse-fallback content tests passed.

An execution trace attributes 94.5% of syscall wait to fdatasync. Most forced WAL
rotation time was spent syncing/closing the previous WAL, blocking overlapping
writes. Compact metadata reduced allocations but did not fix that write-tail cost.
The memory/read/write gates have not passed; the original production service and
format remain selected. Full-node copy, live-write catch-up, expiry/activity
reconciliation and a current rollback bridge remain outstanding.

## Native WAL fix follow-up (2026-10-19)

The same frozen production corpus and scheduled maintenance were replayed on
Linux/amd64 with two CPUs and a 4-GiB resource envelope. Ordinary writes buffer
during the WAL handoff; fsync counts and durability policy are preserved.

| Measurement | cwhisper control | Synchronous exact control | Fixed exact | Fixed repeat |
| --- | ---: | ---: | ---: | ---: |
| CPU seconds | 11.65 | 5.54 | 5.75 | 5.50 |
| Process high-water RSS MB | 25.28 | 41.98 | 42.56 | 42.00 |
| Final allocated MB | 1,075.519 | 235.819 | 235.876 | 235.819 |
| Read p99 ms | 1.872 | 24.437 | 1.577 | 1.564 |
| Scheduled write p99 ms | 14.500 | 132.069 | 1.554 | 1.515 |
| Replay oracle errors | 0 | 0 | 0 | 0 |

The final synchronous control ran after all artifact staging. An initial
diagnostic synchronous control had a 111.936-ms write p99 and 1.787-ms read p99;
the control read tails vary. These sequential bounded runs establish the WAL
write-tail improvement, without a fleet-wide latency or memory guarantee.
The fixed store still uses about 17 MB more RSS than cwhisper on this fixture.

Four exact cases each passed two complete 86,860,585-point checks, including full
compaction/reopen with the earlier v4 reader. The application passed 638 exact
CarbonAPI v2/v3 comparisons, including find/info/render, OOO corrections,
graceful restart, and three new corrections surviving SIGKILL/reopen. Strict
filesystem tests separately cover power loss and required sync ordering. The
full race suite, vet, CI-pinned linter and byte-identical vendor regeneration pass.

The 4.56x fixture ratio remains an unweighted physical measurement. The earlier
SQLite prototype's raw fixture was 4.17x, while its weighted indexed-file estimate
was 11.74x; these are different estimands. A read-only larger-block audit preserves
every timestamp/value bit and reduces chunk payload from 181.450 MB to 128.511 MB
(1.412x), excluding hot correction state, WAL and SST overhead. This does not
prove 10x fleet savings. Full-node copy, live catch-up and current-write rollback
remain prerequisites for selecting the new format.

The strict-filesystem WAL rotation tests in `internal/chunkstore/wal_rotation_test.go`
exercise persistence ordering, checkpoints and shutdown.

## Encoder scratch reuse

Exact encoding reuses a fixed 2,581-byte body buffer for an encoding operation.
The returned encoded value owns a separate byte slice, so returning scratch to
the pool cannot change a previously encoded block. The integer/float selection,
compression options, checksum and v4 layout are unchanged. An output-ownership
test retains raw and compressed results across concurrent encoding calls and
checks their decoded timestamp and Float64 bits.

The frozen production-value oracle checks the encoder against its earlier
implementation on 334 dense/sparse chunks across 30 family/value strata. The
local allocation experiment reduced its median from 3,783 to 810 bytes per
encoding. A matched Linux/arm64 VM replay measured lower CPU with scratch reuse,
but only a modest RSS reduction; the RSS gap versus cwhisper remains. The Zstd
low-memory option is still experimental because it increased CPU relative to
scratch reuse alone in that sequence. These diagnostics do not qualify native
CPU, memory, read/find latency or full-node migration. Encoder ownership and
allocation tests cover the local change; native resource qualification remains required.

## Pending writes and cache accounting

A pending write retains one current chunk and a 128-bit changed-slot mask.
The merge operand is constructed from those slots at commit, preserving the
last value for duplicate updates and excluding unchanged pre-existing points.
This replaces a second complete chunk array: writeChunk shrinks from 4,152 to
2,096 bytes on the evaluated 64-bit builds. Materialization thresholds, atomic
batch boundaries, exact integer/float encoding, format v4 and synchronization
are unchanged. The regression compares complete operands and their serialized
bytes with the previous representation for both legacy and exact encodings.

The matched production-corpus Linux/arm64 VM replay reduced cumulative process
allocation from 801.76 to 609.09 MB. Peak serving RSS stayed approximately 41 MB;
this allocation reduction did not meet the original memory parity target. The later GC25 diagnostic tradeoff was accepted for a bounded canary.

Pebble subtracts memtable accounting reservations from its cache budget. A
budget too small to accommodate those reservations can leave no effective
block cache, increasing misses and allocation despite lowering process RSS.
Cache and GC experiments are explicit research settings; product defaults
remain unchanged. Native production-store and full-service resource qualification,
live conversion catch-up and rollback preserving new writes remain required.

## Decoder allocation candidate

Exact decoding borrows a bounded 2,583-byte buffer until checksum validation
and copying into the chunk's scalar arrays finish. Decoded output owns its
values; the caller's encoded bytes are unchanged. Precomputed CRC32 header
states produce the original checksum without allocating a two-byte prefix.
Encoding, compression, format v4 and synchronization remain unchanged.

The captured-value oracle compares the old decoder/checksum on real chunks
and corrupted inputs; retained-output/concurrency tests cover raw/compressed
blocks, negative zero and NaN bits. Five local steady-state allocation trials
reduce decoding from approximately 2,265 bytes/two allocations to zero. In the
matched VM process, total post-replay allocation falls only about 5% beyond
the changed-slot mask, and its CPU/RSS results remain unqualified.

These changes require native evaluation. Decoder ownership and corruption
tests cover exact values, checksum failures and independent retained output.
Cache/GC defaults are unchanged. The accepted GC25 diagnostic does not block the one-node canary; the full copy, traffic-preserving catch-up, exactness and current-write rollback remain required.


## Bounded canary and current-write rollback

An opt-in `[whisper] store-mirror-files = true` writes each batch through the
original file writer before committing and confirming it in the shared store.
Keep `data-dir` on the original tree and set an explicit, physically disjoint
`store-dir`, `storage-backend = "pebble-chunk"` and `store-encoding = "exact"`.
Buckyd administration and shared expiration must be disabled: administrative
deletions are not mirrored. Keep the original compression, flock and OOO settings.
This temporary mode retains file writes and adds work; shared-only replay resource
results do not describe its performance.

The application durably creates `MIRROR-RECONCILE-REQUIRED` before serving. It
leaves the guard on both clean and unclean shutdown, since stock may subsequently
accept more writes. A new shared-store session refuses that directory until it
is reconciled. Returning to the original file backend uses the original current
tree. Preserve and restore the cache dump as required by the normal shutdown
contract; acknowledged storage batches and points still buffered at ingress are
different durability boundaries. Process-crash tests are not power-loss proofs.

Failed mirrored batches are requeued without confirmation. Their repair state
survives persister replacement on config reload. A retry after a failure or an uncertain
source rewrite repairs the shared metric from a coherent original snapshot before
confirmation. Routine admission-clock crossings use authoritative archive-window
corrections. Private
repair files live outside the original metric tree, so a crash cannot expose
scratch metrics during stock discovery.

`store-convert -reconcile -source-quiesced -source ... -destination ...
-source-id ... -copy ...` finalizes an existing conversion with the identical
source/copy/dataset identity. It refreshes copies, replaces and bit-verifies every
source archive, checks the paged catalog for deletions, compacts/closes, then
retires serving guards. Verification is required. The finalizer rejects partial
manifests, tagged/noncanonical paths and empty source trees. It deliberately
rescans the whole source; it is not yet an efficient online dirty-set catch-up.
The operator must keep the source quiesced and retain incoming traffic until the
new writer takes over. The flag does not establish that operational fence.

The accepted GC25 diagnostic (31.05 MB / 14.81 s) is not a memory blocker for the
one-node canary. The complete node copy, traffic-preserving final handoff, actual
deployed-binary comparisons and native canary measurements are still required.


## Stable JSON wire formatting

Native comparison against the deployed executable found identical protobuf
replies but different spaces in JSON: upstream protojson seeds its comma spacing
from the executable's hash. The vendor patch pins single-line comma spacing to
one space, matching the deployed wire format across rebuilds. Escaping, field
order, number spelling and protobuf serialization remain unchanged. It adds no
second serialization pass. The patch is reapplied by `make vendor`; the golden
covers nested arrays, a comma inside an escaped string, negative zero and float
fallback values. Native JSON and protobuf comparisons must pass before the
one-node format switch.

### Bounded rollback-mirror OOO maintenance

The mirrored writer uses source-provided archive windows for successful explicit
OOO compaction. The windows cover all populated sidecar slots, including expired
records removed by the merge, lower-archive buckets, and virtual rollup buffers
before and after the merge. Input, compaction and retired-block corrections are
read from the successful original writer and committed in one atomic shared
patch. Ordinary file-only writes retain their existing compaction API and budget.

A rewrite revision local to the open source handle detects implicit OOO folding
inside UpdateMany without extra filesystem calls. A rewrite with an existing
sidecar, a merge error, an uncertain retired span, or a previously failed mirror
retains authoritative full-source repair before confirming the batch. The
revision does not replace durable migration guards or source verification.

The full native mirror replay defaults to the production 64 KiB OOO threshold
at 500 checks/second and reports the threshold and actual compaction counts.
GRAPHITE_FULLMIRROR_OOO_THRESHOLD permits an explicitly labelled diagnostic
control. The differential unit oracle covers live and expired sidecars across
all six aggregation methods and three XFF values, including implicit rewrites,
exact complete archives, shared compaction and reopen. These controls do not
substitute for an actual sustained full-application mirrored queue replay.
