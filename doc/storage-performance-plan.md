# Storage performance baseline and proposed improvements

Status: approved and implemented; library commits published to upstream master. The OOO correctness regression was
fixed first. See [implementation results](storage-performance-results.md) for
accepted changes, measurements, publication and remaining deployment checks.
The sections below preserve the original measured plan and acceptance targets.

## What was measured

- go-carbon `7029c88034a73c0d0d09b0a7042a29b2d5c588b9`; root Whisper and
  store `d84403499e32`; Pebble `v1.1.5`.
- Go 1.27.1, Linux/arm64 in the local Docker VM, 2 vCPUs, approximately 2 GiB
  RAM, `GOMAXPROCS=2`, overlayfs scratch storage. Host: Apple M3 Max, 36 GiB RAM.
- All 72 existing benchmark cases, ten repetitions each, run sequentially.
  Writes, reads and concurrent rounds used `4096x`; maintenance `64x`;
  reopen `256x`. Each benchmark retains its classic-oracle correctness checks.
- Seven supplemental read cases and seven three-archive write cases, five
  repetitions each. The OOO late-write/rollup case fails and has no accepted
  timing. The other 13 supplemental cases pass.
- CPU profiles for eight selected engine/workload pairs, allocation deltas for
  the same pairs, and a separate Pebble blocking profile. CPU capture covers
  only the measured loop; allocation deltas subtract the warmed setup profile.
  Allocation profiles use sampling and include some profile-writer overhead;
  `B/op` from the unprofiled benchmarks is the allocation baseline.

Profiles were collected separately because diagnostic instruments can interfere
with each other; CPU profiles alone do not measure time waiting for I/O.
See [Go diagnostics](https://go.dev/doc/diagnostics).

This is a local synthetic comparison, not a production sizing result. Data and
metadata are warm; the largest existing write case has 128 metrics. In that
case 4096 operations mean only 32 batches per metric, although the separate
preflight covers multiple wraps. Single-metric and supplemental write runs
cover many timed wraps. Query fixtures contain holes: the coarse read requests
a six-hour grid with only the recent 20 minutes initially populated.

Pebble synchronizes its WAL on commit. File engines use ordinary unsynced
write/close. These are different durability contracts, and VM storage timings
are not physical-device fsync qualification. No race instrumentation was used
during timing. No changes to GOGC, compression, WAL sync or storage semantics.

## Results

Medians; smaller is better. Write columns are microseconds per submitted,
persisted point; read columns are microseconds per query.

| Engine | Ordered write, batch 8, 128 metrics, 1 archive | Ordered write, batch 8, 1 metric, 3 archives | Recent read | Full fine read | Coarse read |
| --- | ---: | ---: | ---: | ---: | ---: |
| Classic | 1.206 | 3.299 | 9.066 | 22.917 | 11.196 |
| cwhisper | 1.477 | 1.574 | 15.335 | 82.928 | 32.048 |
| cwhisper OOO | 1.489 | 1.619 | 15.375 | 81.955 | 32.237 |
| Pebble | 24.488 | 26.157 | 8.234 | 45.584 | 5.208 |

The three-archive probe uses `1s:10m,10s:1h,60s:6h`, average, XFF 0.5.
Compressed engines are faster in that ordered-write fixture while paying
more on reads. A single overall engine ranking would hide that trade-off.

| Engine | Ordered batch 1, us/point | Ordered batch 64, us/point | Batch 8 allocation, B/point | Batch 8 with 32 workers, us/point |
| --- | ---: | ---: | ---: | ---: |
| Classic | 9.510 | 0.218 | 237 | 0.813 |
| cwhisper | 11.292 | 0.283 | 390 | 0.973 |
| cwhisper OOO | 11.285 | 0.300 | 392 | 0.980 |
| Pebble | 198.763 | 2.654 | 119 | 1.469 |

Sequential write columns use 128 metrics. Concurrent cases use one distinct
metric per worker and a barrier per eight-point round. They include scheduling
and expose Pebble's opportunity to group WAL syncs; they do not establish that
32 workers is the right production setting. The maximum/minimum range across
repetitions is wide for some sequential Pebble writes: batch 8 spans
24.204–37.284 us/point. Use the saved distributions, not small timing deltas.

Late writes in the single-archive, batch-8/128-metric workload cost
1.201 us/point for classic, 3.014 for OOO and 24.400 for Pebble. Plain cwhisper
does not support that workload. In the supplemental pending-correction read,
classic takes 10.543 us/query, OOO 78.262, and Pebble 5.054. OOO allocates
150,832 B/query versus approximately 45,173 for the matched clean OOO probe.

Maintenance medians are 0.185 ms for OOO merge and 1.835 ms for Pebble
flush/full-keyspace compaction. These are different operations on a bounded
corpus, not interchangeable per-point costs. Reopen plus first fetch is
9.934 us for classic, 17.564 us for cwhisper, 17.003 us for OOO, and 1.636 ms
for the shared Pebble database. Pebble reopen is a database lifecycle event.

Apparent single-archive size after maintenance is 7,228 B/metric for classic,
4,921 for compressed engines, and about 6,758 for Pebble in the batch-8,
128-metric case. These are logical lengths including metadata and holes, not
allocated blocks, peak disk use, write amplification, or steady-state RSS.

## Required first step: OOO correctness regression

The expanded late-write/rollup benchmark fails consistently. Its timing is
excluded. A separate serial reproducer locates the first failure at round 76
(77 batches), before explicit compaction:

```text
schema: 1s:10m,10s:1h,60s:6h; average; XFF=0.5
initial now: 1700006400
seed: storagePoints(now-600, 600, 1), then compact
each round: storageWriteBatch(batch of 8, round, late=true)
clock: the end returned by storageWriteBatch
round 76: now=1700007024
query: [1700003424,1700007024]
timestamp 1700006410, step 10: classic=68.5, OOO=66.5
```

The long replay also differs after compaction: at timestamp 1700038990,
classic=25.5 and OOO=26.5. The original 72-case benchmark matrix remains green;
it did not cover this combination of sustained late writes, multiple archives
and fine-retention expiry.

Proposed C0: promote the reproducer into a permanent regression; trace the
first retention wrap through collision replay, sidecar expiry, buffered
rollups and compaction. A plausible mechanism is recomputing an existing
coarse bucket from an incomplete surviving fine window, but the root cause
has not yet been proven. Fix that behavior, exercise all aggregation methods
and XFF settings, and require identical values before/after reopen, merge and
further writes. Rebaseline OOO after the fix. Do not weaken the oracle or hide
the failing case behind a performance exemption.

## Per-engine implementation proposals

Targets below are acceptance thresholds for experiments, not predicted gains.
Each item should be a separate change with an unchanged baseline for comparison.

### Classic Whisper

1. **Evaluate the existing bounded writeout batching.** Small ordered-write
   profiles place about 78% of sampled CPU under `openWithOptions`, including
   about 33% under `acquirePathLock`. Batches of eight reduce per-point time
   about 7.9x in the measured workload. The batching settings already exist;
   first evaluate `writeout-min-points=8` and a bounded arrival deadline such as
   `2s` in a representative replay. Keep current pressure, retry, restored-data
   and shutdown bypasses. Measure queue age, cache residency and crash-loss
   exposure before selecting a deployment value. Acceptance: at least 15%
   lower CPU per committed point with an agreed write-delay budget.
2. **Reduce classic read allocation.** `readSeries` and `unpackDataPoints`
   account for about 78% of sampled allocation in the coarse-read probe.
   First allocate exact output capacity and fill one buffer across ring wrap;
   then assess decoding directly into the result if it remains useful.
   Preserve timestamps, NaNs, signed zero and error handling. Target at least
   25% fewer B/query with no material latency regression. Compare optimized
   classic against the frozen baseline revision, so the oracle cannot change
   alongside the candidate unnoticed.

Source: [readSeries and classic fetch](../vendor/github.com/go-graphite/go-whisper/whisper.go),
[existing batching](../cache/batching.go),
[configuration trade-offs](../go-carbon.conf.example).

### cwhisper

1. **Reduce repeated work in compressed reads.** `fetchCompressed` accounts
   for about 55% of sampled CPU on the coarse-read probe; sorting about 18%.
   `storedPoints` and `filterCompressedSlots` also allocate substantial scratch
   data. Add a proven no-collision fast path using complete interval bounds,
   avoid decoding blocks that cannot overlap, and reuse request-owned decoded
   points across live rollup and slot filtering. Keep future-slot invalidation,
   XFF and partial-window behavior explicit. Target at least 20% less read CPU
   and 25% fewer B/query on coarse/full-fine fixtures.
2. **Reduce header and block scratch allocation.** Ordered-write profiles
   identify `readHeaderCompressed` and `WriteHeaderCompressed` as allocation
   owners. Pre-size or reuse scratch within one operation/handle, then measure.
   Apply the existing batching experiment here too. Keep header CRC checks,
   growth-failure behavior and rewrite publication intact. Avoid introducing
   a shared mutable Whisper-handle cache in this first pass.

Source: [compressed reads and headers](../vendor/github.com/go-graphite/go-whisper/compress.go),
[storedPoints](../vendor/github.com/go-graphite/go-whisper/ooo.go).

### cwhisper OOO

1. **Complete C0 before performance changes.** The new multi-archive late-write
   fixture must become an ordinary correctness gate and pass in full.
2. **Make collision checks cheaper without changing precedence.** In the valid
   single-archive late-write profile, `compressedBatchOverlaps` accounts for
   about 33% of sampled CPU and classic replay for about 53% of sampled
   allocation. Audit whether expired sidecar aliases unnecessarily trigger
   replay; bypass only cases proven irrelevant to all retained aggregates.
   Coalesce raw sidecar-slot reads into bounded contiguous reads where safe,
   rather than one read per input slot. Preserve exceptional future/correction
   write ordering. Target at least 15% less CPU/point and 25% fewer B/point on
   sustained late writes after C0 passes.
3. **Bound pending-correction read work.** `mergeOutOfOrderValues` accounts for
   about 46% of sampled CPU and 70% of sampled allocation in the corrected
   coarse-read probe; `readArchivePointsAt` alone owns about 29% of allocation.
   Read only required sidecar ranges and complete aggregation windows, reuse
   per-request scratch, and avoid repeated decoding. Start without persistent
   result caches or format changes. Target at least 20% less read CPU and
   30% fewer B/query, with corrections still visible immediately.
4. **Evaluate compaction cadence after those changes.** Measure combined
   ingestion, read and merge costs with realistic lateness. The small merge
   fixture is insufficient evidence to prioritize a new compaction design.

Source: [collision checks and replay](../vendor/github.com/go-graphite/go-whisper/compressed_batch.go),
[sidecar reads and recomputation](../vendor/github.com/go-graphite/go-whisper/ooo.go).

### Pebble

1. **Amortize synchronous commits using existing batching/concurrency.** The
   ordered writer profile consumes 2.51 CPU-seconds over 9.36 wall-seconds;
   its separate block profile puts approximately 1.93 seconds of a 2.15-second
   run in commit publication waits. The concurrent benchmark improves from
   roughly 25 us/point with one worker to 1.47 with 32 workers. Sweep bounded
   batches and concurrency on the intended filesystem; retain `pebble.Sync`
   and acknowledgement-after-commit. Target at least 15% lower end-to-end
   cost per committed point within the accepted latency budget. Consider a
   new group-commit API only if existing batching cannot meet that target;
   such an API requires a separate design review.
2. **Remove the unnecessary archive-existence read.** `Fetch` always calls
   `hasArchivePoint`, but only uses its result when aligned `from == until`.
   That helper accounts for about 23% of CPU in the corrected coarse-read
   probe. Restrict it to that condition while retaining the same snapshot and
   empty/populated zero-width behavior. Target at least 10% lower CPU/query
   for nonzero queries with no new state or cache.
3. **Then reassess metadata and rollup work.** JSON metadata decoding costs
   about 18% of CPU in the read probe, while output values dominate allocation.
   After the smaller change, profile production-sized multi-archive writes
   before choosing an immutable metadata cache, alternate serialization or
   bulk range reads for propagation. Any cache must preserve snapshot,
   revision and delete/recreate generation consistency and have a fixed memory
   bound. Do not change the on-disk format based on this small fixture alone.

Historical source: [synchronous commits, Fetch, hasArchivePoint and rollup](https://github.com/go-graphite/go-whisper/blob/16b07882e95e65a1eb2c3c8df22712e795622bde/store/store.go).
The replacement is documented in [chunk storage qualification](shared-storage.md#qualification-results).

## Proposed order and acceptance gates

1. C0 regression and fix; rebaseline OOO.
2. Small independent changes: Pebble's conditional existence check and classic
   read allocation. Keep their commits separate.
3. Shared compressed read work, then OOO-specific collision and correction-read
   work, each measured independently.
4. Evaluate existing batching and writer concurrency in a representative replay;
   a deployment canary is a separate approval, after reviewing concrete results.

Every candidate must pass the full classic differential matrix, the new late
rollup regression, persistence/recovery/transfer checks, targeted upstream
tests, `go test -race ./...`, and Linux filesystem checks. Do not drop required
CRC, locking or sync operations to meet a timing target. If classic itself is
optimized, use the frozen baseline engine as the reference.

Compare unchanged and candidate revisions with matched work and at least ten
interleaved repetitions, using benchstat and per-point/per-query allocations.
On the intended Linux filesystem, add representative retention and lateness
distributions, at least 100k metrics (then the deployment's actual scale),
cold/warm reads, concurrent same-metric read/write, mixed ingestion/query load,
and sufficient duration for WAL flush, SST compaction and OOO maintenance to
reach a repeatable cycle. Measure CPU/committed point, queue age and p95/p99,
live heap/RSS, allocated/peak disk and write amplification separately.

Acceptance targets must improve the relevant workload without more than a 5%
regression in an unrelated case outside measurement noise; otherwise revert or
revise the experiment. Cumulative profile percentages overlap and cannot be
added. For example, halving the compressed fetch path's measured 55% CPU share
could save at most about 27% of this probe's total CPU, not 50% of the service.
No production speedup is claimed by this report.

## Evidence and reproduction

- [All 720 baseline measurements, benchstat format](storage-performance-2026-10-03/baseline.txt)
  and [CSV](storage-performance-2026-10-03/baseline.csv).
- [Supplemental results, including failures](storage-performance-2026-10-03/probes.txt)
  and [CSV with explicit pass/fail](storage-performance-2026-10-03/probes.csv).
- [CPU, allocation and blocking profile summaries](storage-performance-2026-10-03/profiles.txt).
- [Environment, versions, image digest and source hashes](storage-performance-2026-10-03/metadata.json).
- [Measurement-only Go overlay](storage-performance-2026-10-03/measurement_probe_test.go.txt)
  and [reduced OOO failure output](storage-performance-2026-10-03/ooo-rollup-reproducer.txt).

The baseline binary was built with `go test -mod=vendor -p 2 -c ./persister`.
Each of ten rounds ran these patterns sequentially under `GOMAXPROCS=2`,
with `-test.run='^$' -test.benchmem -test.count=1`:

```text
^BenchmarkStorage(Write|Read|ConcurrentWrite)$  -test.benchtime=4096x
^BenchmarkStorageMaintenance$                 -test.benchtime=64x
^BenchmarkStorageReopen$                      -test.benchtime=256x
```

To reproduce the additional test without editing source, map the virtual file
`<checkout>/persister/profile_probe_test.go` to the saved
`measurement_probe_test.go.txt` using a Go overlay JSON `Replace` map, then run:

```sh
go test -mod=vendor -overlay=/tmp/storage-perf-overlay.json ./persister \
  -run '^TestStorageProbeLateRollup$' -count=1
```

Raw profiles, binaries and runner scripts are retained locally in
`/tmp/go-carbon-storage-perf-d_p19n10/`. Benchmark data directories and the
isolated container are removed after collection. This plan and its measurement
artifacts are left uncommitted for review.
