# Storage performance implementation (2026-10-03)

These are historical measurements, including the retired per-point Pebble
prototype. The current tests branch runs only the Whisper file backends and pins
root go-whisper `a1d8f4cdfbff`, which retains the file-library fixes below.

Published go-whisper master: `16b07882e95e65a1eb2c3c8df22712e795622bde`.
At measurement time, go-carbon pinned both the root and nested store modules to
`v0.0.0-20261003193447-16b07882e95e`; vendor matches the published source.
Both modules resolve through the public Go proxy and checksum database from a
fresh cache. No local module replacement is used.

## Changes

All library changes were made in the upstream go-whisper checkout on `master`:

| Commit | Change |
| --- | --- |
| `73e62a1` | Preserve OOO rollups across fine-retention expiry and partial-window rewrites |
| `7addb59` | Decode classic fetch results directly from one circular read buffer |
| `564a471` | Check Pebble archive existence only for zero-width queries |
| `1b90631` | Skip compressed slot bookkeeping when complete bounds exclude collisions; avoid unnecessary decoding scratch |
| `e232958` | Reuse bounded header scratch and pre-size decoded blocks |
| `16b0788` | Bound OOO correction reads and scratch; skip expired single-archive alias replay |

The new regression identified three related failures: discarding fine sidecar
inputs while their coarse aggregates remained retained; flushing a rewritten
partial window without its encoded prefix; and demoting a newer buffered coarse
replacement to a gap-filling sidecar value. The fix retains those inputs and
preserves replacement precedence before overwriting their slots. It uses the
existing file format and exceptional classic replay path.

The upstream regression runs 4,096 alternating late-write batches for all six
aggregation methods and XFF 0/0.5/1, with reopen, compaction and idle expiry.
The first 128 batches are checked individually; later cycles every 32 batches
and at lifecycle boundaries. Classic read changes have a frozen reader from
`d84403499e32` as their oracle, including float bits, holes and ring wraps.

`BenchmarkStorageRollup` and `TestStorageLateRollupRetentionWrap` promote the
previous measurement-only workload into go-carbon's ordinary storage gates.

## Measured changes

Linux/arm64, Go 1.27.1, a 2-vCPU/2-GiB Docker VM, `GOMAXPROCS=2`, overlayfs.
Ten repetitions alternate before/after order. Benchmarks use non-race binaries;
read experiments use 8,192 operations, rollup probes 4,096. Later measurements
shared the host with separate macOS correctness tests. Use the stable allocation
reductions and balanced repetitions as local evidence; these are not production
CPU or throughput claims.

The final complete comparison contains 72 cases x 10 repetitions x 2 revisions
(1,440 measurements), all passing. It uses 4,096 operations for write/read/
concurrency cases, 64 for maintenance and 256 for reopen. Benchstat found no
statistically significant timing regression above 5%. Synchronous Pebble write
timings remain noisy; a lack of significance is not proof of identical latency.

| Final candidate versus published baseline | Time change |
| --- | ---: |
| Classic full fine read | -36.09% |
| cwhisper full fine read | -40.36% |
| cwhisper OOO full fine read | -40.82% |
| Pebble coarse read | -20.38% |
| OOO late writes, batch 8, one metric | -20.21% |
| OOO late writes, batch 64, one metric | -49.96% |
| OOO late writes, batch 64, 128 metrics | -42.59% |

See [complete comparison](storage-performance-2026-10-03/implementation/matrix-benchstat.txt).

The first comparison isolates classic/Pebble and shared compressed reads. The
second compares the remaining refinements against that shared-read candidate;
its percentages must not be added to the first comparison.

| Change / workload | Time change | Allocated bytes change |
| --- | ---: | ---: |
| Classic full fine read | -38.65% | about -59% |
| Classic coarse read | -12.93% | about -53% |
| Pebble coarse read | -20.87% | about -6% |
| Shared compressed full fine read | -39.28% | about -37% |
| Shared compressed coarse read | -28.48% | about -48% |
| Further OOO corrected coarse read | -15.02% | -35.58% |
| Further OOO late write, batch 8, one metric | -20.38% | -57.55% |
| Header reuse, ordered batch 8, 128 metrics | no significant change | about -11% |

The additional OOO corrected-read time gain falls below the proposed 20% target;
its 35.58% allocation reduction exceeds the 30% allocation target. It is retained
for that measured reduction and lower read time. Multi-archive late writes now
pass; the former failing benchmark has no valid before-fix speed comparison.
The 128-metric late-write case does not reach the same per-metric retention wraps
and shows no timing gain. Multi-archive alias checks remain conservative to
preserve retained aggregates. Further coalescing of sidecar slot reads was not
needed to meet the single-archive write target.

The initial OOO correction-read experiment cut only 7.2% of time and was refined
before acceptance. Header pooling is an allocation improvement, not a demonstrated
write-latency improvement. No WAL sync, CRC, locking, retention or XFF check was
removed; no format, public API, dependency, handle cache or deployment setting was
introduced.

Separate timed-loop CPU profiles support the direction of the improvements:

| Workload | Baseline sampled CPU us/unit | Candidate sampled CPU us/unit |
| --- | ---: | ---: |
| cwhisper coarse query | 39.47 | 26.22 |
| OOO corrected coarse query | 102.83 | 66.67 |
| Pebble coarse query | 6.11 | 5.13 |
| OOO late write, per point | 3.74 | 2.81 |

These are single CPU captures per revision, with approximately three seconds of
measured work each, normalized by completed queries or submitted/persisted points.
They are sampling estimates, separate from the ten-repetition uninstrumented
acceptance comparisons. Profile summaries and iteration counts are saved.

## 100,000-metric qualification

One candidate-only run per engine, 32 workers, `1s:10m,10s:1h,60s:6h`, average,
XFF 0.5, 120 seeded points per metric, then three ordered batches of eight.
Each run commits 2.4 million new points and performs 30,000 mixed-in recent
reads. CPU and allocation figures include those reads, normalized by committed
points. Every metric is compared with classic after reopen. Coarse resolutions
are sampled for 252 metrics before maintenance; fine results are checked for the
same sample afterwards. Pebble maintenance compacts the shared database; file
maintenance visits only that sample. The reported maintenance duration also
includes its post-maintenance validation and filesystem walk.

| Engine | Mixed CPU us/point | Mixed allocated B/point | Mixed batch p95 / p99, ms | Allocated disk after maintenance, MiB | Peak process RSS, MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Classic | 6.962 | 968.4 | 0.234 / 27.48 | 1562.5 | 41.0 |
| cwhisper | 7.915 | 1865.3 | 0.194 / 31.07 | 781.3 | 37.4 |
| cwhisper OOO | 7.246 | 1864.6 | 0.166 / 24.96 | 781.3 | 36.9 |
| Pebble | 37.300 | 1141.5 | 21.112 / 44.22 | 177.8 | 44.7 |

All four runs passed. This is a correctness/scale check, not an A/B performance
measurement. RSS is the process lifetime peak, including setup; it is not
steady-state service RSS or filesystem cache. The run triggers Pebble flush and
compaction but does not establish a long-running equilibrium. Pebble uses the
suite's small 8-MiB cache and 4-MiB memtable. Its multi-archive write cost remains
a performance priority; profile representative storage/cache settings before
choosing metadata caching or propagation changes. The 2-vCPU VM's latency tails
and synchronous Pebble commits are not an equal-durability device benchmark.

## Validation and remaining scope

- Upstream full ordinary tests pass. A full race run of the candidate before the
  last local allocation refinements passed in 779.7 seconds with a 30-minute
  timeout. Final affected-path race validation passed in 646.3 seconds; it
  includes the final allocation refinements and independent range-reader oracle.
- Nested store full race tests and go-carbon full race tests against the final
  upstream source pass. The consumer's batching deadline, pressure, restore,
  retry, dump and concurrency tests remain enabled.
- Linux storage parity, recovery, import/export and late-rollup tests pass.
- All 1,440 measurements in the complete matched benchmark comparison pass.
  Linux's full persister race suite passes, including physical sparse-sidecar
  validation. On macOS only that physical-allocation assertion is capability-
  skipped after the independent sparse-file control fails.
- Public module checksums and the regenerated vendor source match upstream.
  Final consumer full race tests pass using the published pin without an
  overlay. All 86 permanent benchmark smoke cases pass. `go vet ./...`,
  `go mod verify` and `git diff --check` pass.

The existing batch-size and concurrency sweeps demonstrate storage cost
amortization; they do not choose a receiver-to-disk delay budget. Batching stays
disabled by default. Production replay, queue-age/crash-loss budgeting, explicit
cold-cache runs, actual deployment cardinality, steady-state compaction/write
amplification, and a production canary remain deployment qualification. No
production configuration was changed. New group-commit APIs, metadata caches or
on-disk formats remain separate design decisions.

## Evidence

The [original plan](storage-performance-plan.md) and its frozen baseline remain
available. [Implementation metadata](storage-performance-2026-10-03/implementation/metadata.json)
records the image, sources and experiment stages. Raw measurements and benchstat
comparisons are in [the evidence directory](storage-performance-2026-10-03/implementation/).
The saved scale probe is a temporary Go test overlay and cleans all engine data
through `testing.TempDir`; its text source is included for reproduction. Benchmark
scratch data is separate from retained reports and build/module caches.
