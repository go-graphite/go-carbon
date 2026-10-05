# Storage correctness and performance

The `persister/storage_*_test.go` suite compares classic Whisper, cwhisper and
cwhisper with out-of-order support. Classic Whisper is the behavioral oracle.
Tests use temporary files and a controlled clock; they do not touch a configured
data directory or require an external service. The separate Pebble integration
adds its backend to the same workloads.

## Correctness gate

```sh
make test-storage
# Without race instrumentation:
go test -mod=vendor ./persister -run '^TestStorage' -count=1
# Machine-readable results:
go test -mod=vendor ./persister -run '^TestStorage' -count=1 -json > storage-tests.jsonl
```

The ordinary test suite includes these checks. Comparisons cover presence,
timestamps, query grids, NaN holes, finite float bits (including signed zero),
aggregation, XFF and retention metadata. Every engine receives the original batch
order because Whisper sorts its input in place. Do not add `t.Parallel`: the
library's clock is process-wide.

| Test | Coverage |
| --- | --- |
| `TestStorageParity` | Six aggregations, XFF 0/0.5/1, ordered/shuffled writes, duplicate slots, sparse rollups, late corrections, retries, retention boundaries, ring wrap, expiry and special floats |
| `TestStorageFetchEdges` | Empty/populated archives, zero/sub-step queries, clipping and archive selection |
| `TestStorageRandomizedParity` | Three reproducible seeds across six aggregations for OOO, with mixed-age writes, duplicates, compaction and reopen |
| `TestStorageLateRollupRetentionWrap` | 4,096 alternating late-write batches through fine-retention wraps, compaction, reopen and idle expiry |
| `TestStorageCircularSlotWriteOrder` | Fine/coarse collisions between future points and historical corrections in both orders |
| `TestStorageCWhisperLateLimitation` | Documents plain cwhisper's late-write limitation and verifies OOO sidecar creation/removal |
| `TestStorageCrashRecovery` | Child exit after successful writes, recovery, subsequent writes, compaction and reopen |
| `TestStorageConcurrentMetrics` | Eight writers with interleaved reads, isolation and persistence |
| `TestStoragePersisterRoundTrip` | Real persister configuration, confirmations, no stranded in-flight points and post-reopen reads |

Plain cwhisper has 90 explicitly skipped late-write cases and a separate test of
that limitation. OOO has no parity exemptions. The subprocess-only crash writer
and opt-in scale test skip during ordinary runs. Sparse allocation is asserted
only on filesystems that preserve holes after close, verified with a control file.

## Benchmarks

```sh
make bench-storage
# Correctness smoke run; these timings are not performance evidence:
make bench-storage STORAGE_BENCH_FLAGS='-benchtime=1x -count=1'
go test -mod=vendor ./persister -run '^$' \
  -bench '^BenchmarkStorageWrite/(ordered|late-holes)/batch=8/metrics=1$/' \
  -benchmem -benchtime=4096x -count=5 > before.txt
```

The matrix covers write batches of 1/8/64 points, 1/128 metrics, ordered and
late-hole workloads, fine/coarse reads, 1/8/32 concurrent writers, multi-archive
rollups, OOO maintenance and reopen. Each workload checks results against classic
Whisper outside the timer, including after maintenance and reopen. Write
benchmarks advance time and validate full live windows through ring wraps.
Plain cwhisper's unsupported late-write workloads are excluded.

Report per-point CPU/time and allocation alongside throughput, memory, disk and
read latency. Record revision, Go version, filesystem, CPU and `GOMAXPROCS`; run
matched repeated measurements on an idle host. Benchmarks preserve the file
persister's ordinary unsynced open/write/close lifecycle. Final maintenance is
outside write timing. No timing threshold is enforced in CI.

`BenchmarkStorage*` reports logical sizes, including sidecars and apparent sparse
lengths, rather than allocated blocks or peak disk use. Engine logs are captured
and printed on failure so benchmark output remains parseable by benchstat.

## Scale workload

`TestStorageScale` runs one backend per process and reports process CPU per input
point, allocation, RSS, read/write latency and allocated file blocks. Creation
and final maintenance are reported separately. All metrics are checked after
reopen; every archive is checked for a sample before and after maintenance.
Values repeat across metrics, so compression figures are not production forecasts.

```sh
GOMAXPROCS=2 STORAGE_SCALE_ENGINE=cwhisper-ooo \
  STORAGE_SCALE_METRICS=100000 STORAGE_SCALE_ROUNDS=80 \
  STORAGE_SCALE_WORKERS=32 \
  go test -mod=vendor ./persister -run '^TestStorageScale$' -v -count=1 -timeout 1h
```

Use `classic`, `cwhisper`, or `cwhisper-ooo` as the selector. Set
`STORAGE_SCALE_LATE=1` for late-hole filling, and
`STORAGE_SCALE_CPU_PROFILE=/tmp/storage.cpu` to profile the measured loop.

## Temporary files and cleanup

Go removes temporary files after tests. The Make targets additionally isolate
scratch directories below `${TMPDIR:-/tmp}/go-carbon-storage/`, preserve exit
status and clean on exit. For forcibly stopped runs, stop the process before
running `make test-storage-clean` or `make bench-storage-clean` with the same
`TMPDIR`. These targets retain build/module caches and saved reports.

## Correctness fixes and historical measurements

The pinned root go-whisper revision includes published compressed/OOO fixes for
retained samples, XFF, query grids, historical corrections and circular-slot write
order. No local module replacement is used. Classic Whisper remains the oracle.

[The correctness investigation](storage-correctness-plan.md),
[performance plan](storage-performance-plan.md), and
[measured implementation results](storage-performance-results.md) preserve the
2026-10-03 evidence. Those historical reports also include the retired per-point
Pebble prototype; it is not a backend in this branch. Production filesystem
qualification and a representative replay remain separate deployment gates.
