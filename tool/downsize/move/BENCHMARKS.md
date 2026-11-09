# Local resize benchmarks

Build the application, resize command and loopback laboratory with the same Go
toolchain used by CI. All targets and temporary directories must be explicit.

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o /tmp/go-carbon ./
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o /tmp/resize ./tool/downsize
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o /tmp/resize-lab ./tool/downsize/move/lab
/tmp/resize-lab -help
```

Use a new temporary directory for each run. Record fixture dimensions, source
and target encoding, transferred raw/stored bytes, wall time, CPU time and peak
RSS. Compare alternating runs with identical fixture dimensions and cache state.
The lab validates transferred point values and exposes profiling options. Keep
cold/warm cache results separate. Loopback throughput does not establish device,
network or foreground workload capacity on another machine.

Source restarts invalidate an existing whole-store source session. Start a new
baseline after that event. Worker/controller resumption does not imply source
session persistence. Shared CPU and I/O governors are ceilings; validate their
interaction with the actual foreground workload before selecting limits.

## Transfer phases and foreground load

Use `--mode source` to measure export and binary framing to a discard sink.
`--mode target` prepares source wire files outside measurement, then measures
decode, SST ingestion, overlap merges, publication and receipt verification.
Both modes share the three-App process, its garbage collector and background
work. They do not model separate hosts or physical network traffic. `--cpuprofile`
records the measured phase; `--memprofile <prefix>` writes allocation profiles
before and after it. Counters report allocation bytes, objects and GC cycles.

For foreground measurements, add these flags to a normal bulk run:

```sh
--ingest 10000 --ingest-source --reads 8 --read-source --read-qps 1000 \
--delete=false --measured-health
```

The ingest sender catches up to scheduled rates in bounded batches using a
reusable buffer. Read workers retain scheduled deadlines and fetch the fixture's
stored time range. Per-host offered points, committed points, nonempty reads,
read-p99 buckets and write errors distinguish sender slowdown from backend
capacity. Load and counters stop before profiler output or forced GC.
`--mode idle --duration 20s` supplies a matched foreground baseline. `--auto`
enables governor ramping; otherwise the run starts at its available ceiling.
Both retain a 1000 MB/s per-direction cap and storage guards.

Measured health uses shared-process CPU, per-App cache backlog, write errors
and recent read latency. Disk, NIC and memory capacity remain explicit fixture
assumptions. This mode does not probe physical disk utilization or NIC saturation.
Removing reads is a local capacity experiment; it does not validate external
replica routing or replace the runbook's readiness gates.

## Receipt and target-cache comparisons

`BenchmarkTransferReceiptEncoding` compares the original `json.Marshal` plus
SHA-256 path with a reusable standard JSON encoder and buffer. The encoder's
final newline is excluded from the hash. Its byte-parity oracle covers escaping,
invalid UTF-8, long names, integer extremes and nil versus empty counts.

Overlap merges capture committed target metadata and continuation state under
writer locks before mutation invalidates it. Unavailable entries use the database
path. The cached/uncached oracle covers exact, v6 and v7 stores, a newer target
value, target-wins overlap and receipt verification after a later foreground write.
Independent staged SST read-back and final merged-result verification remain.

Prior interleaved local measurements used one million synthetic names, 333,140
moved metrics, 373,401,893 raw bytes, 1% target overlap, four shared backend CPUs,
eight streams and identical cache budgets. Receipt encoding reduced allocation
from 608 bytes and three allocations per proof to zero after warmup. End-to-end
CPU medians were 30.57 to 29.54 CPU-s/raw GB for receipt reuse and 30.22 to 28.96
for target-cache reuse. These are separate matched comparisons; do not multiply
their ratios. A final warm run measured 29.63 CPU-s/raw GB and 78.83 raw MB/s.
Those observations do not demonstrate the 20 CPU-s/raw GB target, sustained
1 GB/s sparse transfers or production foreground safety.

## Sparse transfer and retirement reconciliation (2026-11-02)

A later local experiment used one million synthetic names, 333,140 moved metrics,
589,777,822 raw history bytes, two nonempty archives (10s:1d and 1m:30d),
128 input points per metric, and approximately 1 KiB of cached state per metric.
It ran on a shared 16-logical-CPU ARM64 machine with Go 1.27.0 and vendored
modules. Each embedded App shares that process's CPU budget and garbage collector.
This is a sparse synthetic fixture, not a captured production corpus.

Three runs with eight backend CPUs, eight streams, 256 MiB block caches and
128 MiB metadata caches, without export prewarming, had median baseline rates
of 162.74 before and 224.10 raw MB/s after. Individual rates were 124.56–169.92
and 167.08–226.17 MB/s. CPU medians were 16.81 and 16.08 CPU-s/raw GB;
allocations fell from 25.96M to 23.69M objects. OS cache state and other machine
load were uncontrolled, so these short-run wall times are not a production
capacity claim. The earlier first profiled run measured 98.51 MB/s and
28.78 CPU-s/raw GB, illustrating that variability.

Changes coalesce wire hashing, verify catalog names and receipt IDs in their
respective sorted orders, avoid rediscovering an unpublished staged ID's keys
for an overlap merge, remove redundant temporary catalog writes, and publish
in groups of 16 names to reduce writer-stripe intersections. Full staged SST
read-back, archive checks, receipt bytes, target-wins merges and durability
barriers remain. Partitioning publications into stripe lanes and eliminating
SST sorting by growing per-namespace slices did not pay and were reverted.

A warmed single-pair sweep with 16 shared backend CPUs and 8,192-name batches
measured the following without profiling. Scaling was sublinear beyond four
streams. A separate profiled 14-stream run measured 138.19 MB/s; profiling and
machine load must not be confused with concurrency effects.

| Requested streams | Raw MB/s |
| --- | ---: |
| 1 | 51.36 |
| 2 | 100.04 |
| 4 | 148.86 |
| 8 | 193.73 |
| 14 | 223.16 |

An earlier 64-name-publication variant, using a simultaneous file barrier and
four CPUs per App process, measured 124.47, 180.28 and 259.81 aggregate raw MB/s
for one, two and four independent pairs. All processes shared the same physical
machine and disk. This was 1.45x and 2.09x aggregate scaling, not near-linear
scaling across separate hosts. The 500 raw MB/s per-pair goal was **not met**.
The warmed source export/framing stage reached 1,148.78 MB/s; the isolated
target decode/SST-ingest/publication/verification stage reached 247.40 MB/s.
These isolated phases exclude network, planning and the final reconciliation.

The larger end-to-end win was retirement reconciliation. With 1,024 histories
changed after baseline staging, the old controller took 62.65 seconds including
catalog inventory, plan generation, copying, delta reconciliation, retirement
checks and independent sampled history parity. The final controller took
4.43 seconds with the same fixture and updated server implementation. Its copy-
through-retirement phase took 3.17 seconds versus 61.06 seconds. An intermediate
version took 6.38 seconds before version markers were removed from the
--delete-rate budget. That cap governs actual deletion; markers use copy admission. The retirement source ended empty and sampled target histories, including the
injected new values, matched. The new --reconcile mode took 4.47 seconds including
inventory/planning and final parity while retaining all 333,140 source histories;
it gates on the writer-drain barrier and performs the same full coverage check
without data deletion.
The old path repeatedly scanned pending catalog pages and performed one transfer
per changed name; the new capability checks bounded plan batches and transfers
only their changed names through the shared admission workers. Final locked
catalog/version checks and source fencing remain required.

A controlled foreground comparison offered 10,000 points/s per App on a small
hot set. Committed rates were 10.03–10.04k points/s when idle, 10.03–10.04k
with paced copy, and 9.90–10.11k during an uncapped short copy. All three Apps'
backlogs drained and no write/load errors occurred. This duplicate-heavy hot-set
workload does not establish foreground safety at a large host's physical write
rate. Read traffic was disabled; sampled history parity is not replica-routing
or intact-site latency validation.

`--pair-target 0` restricts a lab run to one source/target pair. `--start-file`
waits after prewarming for a local file barrier, allowing independent lab
processes to start their measured phases together. `--mutexprofile` records
contention. Source and target phase modes now honor `--streams` with private
per-worker decoder buffers. `--delta-metrics 1024 --delete=true` changes source
histories after baseline staging and exercises final reconciliation on the
private fixture. Use both targets for that experiment. Output includes inventory,
plan, measured transfer and final parity times plus process peak RSS. Use reused
fixtures for copy timing; initial fixture construction is reported separately.

A separate warmed one-pair CPU-budget sweep (eight requested streams) measured
64.08 / 119.71 / 197.43 / 212.92 MB/s at 1 / 2 / 4 / 8 shared backend CPUs.
The controller was a separate process. This was near-linear through two CPUs,
about 3.08x at four, and then limited by the local target/storage path. These
CPU and stream sweeps do not establish throughput on different host hardware.
For source retention plus final reconciliation, use --reconcile --delete=false
in the private lab. Source version bookkeeping is bounded by the ordinary copy
admission; --delete-rate must not impose an implicit sparse-copy ceiling.

A final alternating-order publication-group check used three runs per setting,
14 requested streams, 16 backend CPUs and one pair. Medians for 16 / 8 / 4-name
groups were 228.62 / 226.92 / 224.72 raw MB/s and 17.56 / 18.16 / 18.07
CPU-s/raw GB. Ranges overlapped and smaller groups did not consistently improve
throughput, so 16 remains the setting. A high-stream mutex profile still showed
85.6% of aggregate sampled delay at writer-stripe acquisition. This is a
remaining parallelism limit, not a fraction of total wall time; correctness and
foreground writer locking remain required.
