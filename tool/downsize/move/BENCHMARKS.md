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
