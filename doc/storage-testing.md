# Storage correctness and performance

The `persister/storage_*_test.go` suite exercises the vendored Whisper libraries
and go-carbon's `internal/chunkstore`. Classic Go Whisper is the behavioral oracle
for cwhisper, cwhisper OOO and `pebble-chunk`. No external service or fixture
download is needed.
The tests create temporary storage and freeze the clock; they do not touch a
configured data directory.

The retired per-point Pebble backend is no longer in the matrix. See
[chunk storage qualification](shared-storage.md#qualification-results) for its
replacement, the opt-in large-cardinality workload, crash/fault tests and current
measurements. The dated sections below preserve results from before that replacement.

## Correctness gate

```sh
make test-storage
# Without race instrumentation:
go test -mod=vendor ./persister -run '^TestStorage' -count=1
# Machine-readable results, including exact failing subtests:
go test -mod=vendor ./persister -run '^TestStorage' -count=1 -json > storage-tests.jsonl
```

The normal `go test ./...` / CI suite includes these tests. They are strict tests,
not an opt-in report that tolerates newly discovered discrepancies. Fixes to an
engine or a dependency pin must make the relevant oracle cases pass. Do not
record candidate output as a replacement expected result merely to make CI green.

| Coverage | Checks |
| --- | --- |
| `TestStorageParity` | 864 engine/policy/trace combinations: six aggregation methods, XFF 0/0.5/1, ordered and shuffled batches, same-timestamp and same-slot duplicates, sparse rollups, late hole fills and corrections with/without rollups, retries, historical corrections, retention boundaries, full/partial block ring wrap, expiry/future admission and special float values |
| `TestStorageFetchEdges` | Empty/populated files, zero/sub-step/exact-step queries, archive-selection boundaries and clipping (432 cases) |
| `TestStorageRandomizedParity` | Three fixed seeds across six aggregation methods for OOO and Pebble; mixed-age batches, duplicates, clock advances, periodic compaction and reopen; failures include the seed, batch and clock |
| `TestStorageLateRollupRetentionWrap` | 4,096 alternating late-write batches across three archives; checks the first fine-retention wrap on every update, later cycles periodically, and final compaction/reopen |
| `TestStorageCircularSlotWriteOrder` | Fine/coarse circular-slot collisions between future points and historical corrections in both write orders; checks every resolution before/after time advances, compaction and reopen |
| `TestStorageCWhisperLateLimitation` | Explicitly verifies plain cwhisper loses a late point, while classic, OOO and Pebble retain it; verifies an actual sidecar is created and removed by merge |
| `TestStorageCrashRecovery` | A child exits after acknowledged writes without cleanup; checks four metrics after recovery, additional writes, compaction and another restart; Pebble covers WAL-only and SST plus newer WAL data |
| `TestStorageConcurrentMetrics` | Eight writers with distinct values and interleaved reads; checks metric isolation and restart, including race detection |
| `TestStoragePersisterRoundTrip` | Real `Whisper.store` configuration, confirmation counts, no stranded in-flight points, and data read after reopening |
| `TestStorageTransferRoundTrip` | Classic/compressed/OOO import to Pebble, source main/sidecar hashes unchanged, restart, classic-readable export |
| `TestStoragePebbleArchivePersistence` | Every physical archive and metadata survive import/flush/compaction/restart/export; delete/recreate cannot resurrect old generations |

Oracle comparisons include series presence, timestamps, step, length, NaN holes,
finite float bits (including signed zero), aggregation, XFF and retention metadata.
Each write and both reopen/compaction stages are checked independently, so a
mismatch at one stage does not prevent the later persistence checks. Every engine
gets its own copy of the original batch order; Whisper sorts its input in place.

Plain cwhisper has 90 explicitly skipped late-write scenarios and a separate
test of that limitation. **OOO and Pebble have no parity exemptions.** The crash
writer itself is a subprocess-only helper and skips when invoked normally.
The clock is global in Whisper: do not add `t.Parallel` to these tests.

## Temporary files and cleanup

Go's `TempDir` cleanup removes the databases, sidecars, exports and engine logs
after each test or benchmark, including ordinary failures. The Make targets also
wrap each run in its own temporary directory and remove it when the command
exits, preserving the test's exit status.

For leftovers from forcibly terminated runs, first stop the corresponding runs,
then use:

```sh
make test-storage-clean
make bench-storage-clean
```

These remove only the respective scratch directory under
`${TMPDIR:-/tmp}/go-carbon-storage/`. If you supplied `TMPDIR` when running a
target, supply the same value when cleaning. Test and benchmark cleanup are
independent. Go build/module caches and explicitly saved reports (such as
`before.txt`) are retained. Direct `go test` commands use Go's normal temporary
directory handling; these Make cleanup targets only manage runs made through
the Make targets.

## Benchmarks

```sh
make bench-storage
# A small smoke run checks all paths; these timings are not performance evidence:
make bench-storage STORAGE_BENCH_FLAGS='-benchtime=1x -count=1'
# Equal work across backends and revisions:
go test -mod=vendor ./persister -run '^$' \
  -bench '^BenchmarkStorageWrite/(ordered|late-holes)/batch=8/metrics=1$/' \
  -benchmem -benchtime=4096x -count=5 > before.txt
# Repeat the same command after a change, writing after.txt; if available:
benchstat before.txt after.txt
```

Use `TMPDIR` to select the filesystem under test. Record the commit, Go version,
OS/architecture, filesystem, CPU, `GOMAXPROCS`, benchmark command and storage
dependency pins alongside results. Run timing experiments on an otherwise idle
machine, separately from race tests or other benchmarks. Compare matching
workloads and batch sizes; use allocations and repeated measurements, not a
single `ns/op` sample. Run the correctness gate before accepting an optimization.

| Benchmark | Timed work |
| --- | --- |
| `BenchmarkStorageWrite` | One batch of 1/8/64 points into 1/128 metrics, ordered or filling holes from previous batches; file open/lock/update/close versus shared Pebble commit |
| `BenchmarkStorageRead` | Open/fetch/close for files or shared-store fetch; recent/full fine resolution and historical coarse reads with a warm filesystem cache |
| `BenchmarkStorageConcurrentWrite` | Rounds of eight-point writes from 1/8/32 workers to distinct metrics, including scheduling and Pebble's opportunity to group WAL syncs; all metrics checked against a bounded classic replay afterwards |
| `BenchmarkStorageRollup` | Three-archive ordered/late writes and coarse reads with/without pending corrections; classic comparison before and after timed work, compaction and reopen |
| `BenchmarkStorageMaintenance` | OOO merge or Pebble flush plus full-keyspace compaction, after reseeding a bounded 1,024-point workload outside the timer |
| `BenchmarkStorageReopen` | Reopen plus first fetch for a one-metric store; Pebble closes/reopens the database, files open/close their metric |

Write benchmarks first validate their workload against classic, including two
ring wraps and compaction. They seed full archives before timing, then advance
time with each round so repeated iterations cannot simply rewrite/drop the same
timestamps. They compare the entire live window for every metric against a
bounded classic replay after compaction and reopen. Plain cwhisper's late-write benchmark is skipped because dropping
points is not useful throughput. Reads check their exact query against classic
before timing; maintenance compares values before and after every iteration.

Outputs include `ns/op`, `B/op`, `allocs/op`, `ns/point`, `points/s`, `points/op`,
read `ns/value` and `values/op`, file count and `logical-B/metric`. Divide B/op
and allocs/op by points/op for per-point allocation costs. Disk figures include
WAL/metadata/sidecars and apparent sparse lengths; they are **not allocated disk
blocks or peak disk use**. Store memory limits are fixed at an 8 MiB cache and
4 MiB memtable. Engine logs are captured during benchmarks so output remains
parseable by benchstat, and printed if a benchmark fails.

**Durability is different:** Pebble commits sync the WAL; file backends use their
normal unsynced write/close path. These compare current application behavior,
not equal power-loss guarantees. Explicit final compaction is outside the write
timer (ordinary Pebble background work still runs); use the maintenance benchmark
and an intended compaction cadence to account for that cost. Fixed operation
counts help compare the same number of submitted points. No timing threshold is
enforced in CI.

## Correctness fixes

Classic Whisper remains the reference; its storage behavior is unchanged.

- cwhisper grows capacity before block rotation can overwrite retained samples,
  applies classic XFF and query-grid rules to live rollups, preserves circular-slot
  overwrite semantics, and materializes buffered history before policy changes.
- OOO exposes corrected aggregates immediately, including cascading rollups and
  explicit NaN corrections. Compaction preserves corrected buffers. Exceptional
  circular-slot collisions replay classic write order in an in-memory scratch
  archive before publishing the compressed replacement; ordinary updates retain
  the incremental path.
- The chunk engine preserves mixed-age retention routing and propagation past
  partial XFF windows, checked against classic Whisper. WAL synchronization is
  unchanged.

The file-library fixes are published on go-whisper's `master`, with separate
compressed/OOO commits and library regressions. go-carbon pins and vendors the
root module without a local `replace`. The nested store module has been removed
from its dependencies; go-carbon owns the chunk engine.

## Performance implementation (2026-10-03)

At this stage both modules pinned `v0.0.0-20261003193447-16b07882e95e`. The additional OOO
retention-wrap fix, classic/compressed/OOO/Pebble read optimizations, and OOO
single-archive replay optimization are published upstream. The full consumer
race suite and all 86 benchmark smoke cases pass with that public pin. See
[results and scope](storage-performance-results.md) for the 1,440 matched
measurements, Linux validation and 100,000-metric qualification.

## Earlier correctness validation (2026-10-03)

This records the initial correctness pin. See
[performance implementation results](storage-performance-results.md) for the
subsequent retention-wrap fix, optimizations, current pin and validation.

Validated with Go 1.27.1 on macOS/arm64 and Linux/arm64. Root Whisper is pinned
to `d84403499e32` (shared compressed fix `fbae74965bf0`, followed by the OOO fix).
The nested store is pinned to the same revision; its code is unchanged from the
previously validated `5f2e38dab385` store revision.

| Engine | Parity passed | Fetch edges passed | Random traces passed | Slot-order cases passed |
| --- | ---: | ---: | ---: | ---: |
| cwhisper | 198 | 144 | Unsupported late writes | 2 |
| cwhisper OOO | 288 | 144 | 18 | 6 |
| Pebble | 288 | 144 | 18 | 6 |

The 90 known plain-cwhisper late-write cases remain explicitly skipped. OOO and
Pebble have no parity skips. Recovery, isolation, persister acknowledgements,
transfer and archive-persistence tests also pass.

- Full go-carbon suite: `go test -mod=vendor -race -count=1 ./...` passes.
- Linux: all persister tests pass with `-race -count=1` in
  `golang:1.27.1-bookworm`, including physical sparse-sidecar allocation.
  On this macOS filesystem, an independent Go sparse-file control loses holes
  after close, so only that physical-allocation assertion is capability-skipped.
- All 72 benchmark cases pass a one-iteration smoke run. These timings were
  collected alongside tests and are not performance evidence.
- The upstream library's full ordinary suite and targeted race tests for
  compressed/OOO/rewrite paths pass. Its full race suite exceeded the 10-minute
  timeout in the existing CPU-heavy `TestFillCompressedMix`; no race was reported
  before the timeout. The later performance validation completed a full library
  race run with a 30-minute timeout.
- `go vet ./...`, `go mod verify`, and `git diff --check` pass.

## Historical baseline before fixes

The first run on macOS/arm64, Go 1.27.1, found existing strict-parity failures:

- cwhisper and OOO differ from classic on recent coarse rollup values, some sparse
  XFF behavior and zero/sub-step query grids. These are reported even when a later
  compaction or elapsed time might make a particular query agree.
- Filling a 600-point compressed archive loses some still-live values near the
  oldest edge in the tested workload, already before the eight-point clock
  advance. Reopen and compaction do not recover them. The write benchmarks'
  full-window oracle catches this too and rejects those performance results.
- OOO differs on the mixed-age/future retention-boundary trace.
- Pebble differs on the mixed-age retention-boundary trace and randomized traces.
  `go.mod` pins root Whisper to `5f2e38dab385`, but the nested store to
  `32ae757b63b8`. The vendored root `extractPoints` splits at `i`, while the store
  still splits at `i-1`; this is one concrete cause to investigate/fix upstream.

Small reproductions:

```sh
go test -mod=vendor ./persister -run '^TestStorageParity/cwhisper-ooo/average/xff=0.5/ordered$' -count=1
go test -mod=vendor ./persister -run '^TestStorageParity/pebble/average/xff=0.5/retention-boundaries$' -count=1
go test -mod=vendor ./persister -run '^TestStorageFetchEdges/cwhisper-ooo/populated=true/age=0/width=0$' -count=1
go test -mod=vendor ./persister -run '^TestStorageRandomizedParity/pebble/seed=1/average$' -count=1
go test -mod=vendor ./persister -run '^TestStorageParity/cwhisper-ooo/average/xff=0.5/ring-wrap-partial-block$' -count=1
```

Recovery, concurrent metric isolation, persister acknowledgements, imports and
exports passed in the initial run. Read/maintenance/reopen benchmark smoke runs
passed; write benchmark cases reject compressed-storage window discrepancies.
These were failures in the original test-only change. Keep the strict oracle
checks as regression gates for the fixes described above.

The suite is synthetic, with bounded cardinality. It does not simulate power
loss, disk-full/failed-fsync/torn-WAL faults, crashes *during* a write or rename,
multi-process replacement races, cold-cache reads, production RSS, latency
percentiles or end-to-end receiver/cache throughput. A passing suite is a
regression gate for these cases, not a proof against every form of corruption.
Production filesystem qualification and a representative production canary remain
separate checks before deploying storage changes.
