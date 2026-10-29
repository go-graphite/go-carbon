# Whole-store copy measurements

Whole-store replacement copies a native Pebble checkpoint and retained input-log
files. Its source reads immutable stored bytes in file ranges. An empty target
opens those same SSTs and metadata after verification, then replays the log gap.
There is no per-metric decode/re-encode, cache refcount loop, merge write or SST
reconstruction in this baseline. Metric-range moves use a different path; their
78.8 MB/s observation does not predict whole-store restoration time.

## Local comparison

These are warm operating-system page-cache, loopback measurements on a shared
Mac, with separate source and target processes and four source CPUs. They do not
establish physical NIC/NVMe bandwidth or production foreground safety. Decimal
MB/GB are used throughout. Checksums, resume verification and file syncs remain
enabled. Every completed offline copy matched all source file SHA-256 hashes.

| Measurement | Before | After |
| --- | ---: | ---: |
| 2 MiB source range, allocation/request | 264,838 B | 4,110 B |
| Source range time | 812 us | 790 us |
| Sparse whole checkpoint, median MB/s | 509 | 612 |
| Sparse whole checkpoint, median CPU-s/stored GB | 1.392 | 1.329 |
| Dense whole checkpoint, median CPU-s/stored GB | 1.211 | 1.115 |
| Dense target CPU, source batching held constant | 1.918 s | 1.804 s |

Sparse means one million names with 48 points each (454 MB, 75 files); dense
means one million names with 384 points each (3.067 GB, 470 files). Each offline
column uses three copies. Wall times vary substantially on the shared machine;
the allocation reduction and bounded write sizes are more stable evidence.
The buffer pool removes repeated allocation/clearing and increases source file
read size from 256 KiB to 1 MiB. Targets fill a reused worker buffer before writing
so short socket reads do not cause small disk writes. At most 64 active streams
per role hold 64 MiB of scratch; verified files skipped on resume need no buffer.

Separate original source/target profiles attributed 92%/88% of sampled CPU to
kernel I/O and 6%/8% to SHA-256. After batching, kernel I/O still dominates. The
whole-copy path has no point compression or codec CPU to remove. Filesystem and
network copies, mandatory checksums, durability syncs and protocol handling remain;
the profiles do not prove all remaining kernel cost is unavoidable.

With continuous 10,000 points/s and 1,000 reads/s on 1,000 hot names, the final
30-second comparison (copies finish before measurement stops) was:

| Whole-copy phase | Before | After |
| --- | ---: | ---: |
| Stored MB/s, 1 GB/s ceiling | 524 | 787 |
| CPU-s/stored GB, including foreground | 1.903 | 1.593 |
| CPU-s/stored GB, subtracting matched idle CPU | 1.302 | 1.152 |
| Offered / committed points | 350,650 / 350,628 | 310,993 / 311,223 |
| Read p99 upper bound, idle / copy | 4.096 / 8.192 ms | 4.096 / 8.192 ms |
| Nonempty reads / completed reads | 35,072 / 35,072 | 31,106 / 31,106 |

Both runs had zero read/write errors and passed independent target chunk
read-back. Checkpoint creation took 307/256 ms and continued serving nonempty
reads. The steady-state commit budget was 98%; the accepted read budget was
20 ms (the after run also passed 10 ms). An earlier baseline attempt exceeded
10 ms during idle load before copying, with a 16.384 ms histogram bound. These
are synthetic hot-read workloads on shared hardware, not a production capacity
claim. Other repeated loaded runs ranged from 635–703 MB/s before and 787–819 MB/s
after; the exact wall-time improvement is not stable enough to extrapolate.

## Live source benchmark

The laboratory starts a real application with TCP ingestion, durable input logging,
the shared store, catalog index, HTTP reads and the whole-store API, all bound to
loopback. The driver clones a synthetic fixture, runs paced ingestion and reads,
then repeats full checkpoint copies while load continues. It reports source CPU
separately from target process CPU, committed/offered points, cache backlog,
nonempty reads and a read-p99 histogram upper bound. A short checkpoint phase is
reported separately. Source profile collection uses a separate endpoint so the
application server's request timeout cannot truncate a long profile.

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o /tmp/resize ./tool/downsize
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o /tmp/copy-source ./tool/downsize/copy/lab
/tmp/resize snapshot --action fixture --destination /tmp/copy-fixture \
  --metrics 1000000 --points 384 --apply
python3 tool/downsize/copy/live_benchmark.py \
  --source-binary /tmp/copy-source --copy-binary /tmp/resize \
  --fixture /tmp/copy-fixture --output /tmp/copy-load-1 \
  --duration 30 --ingest 10000 --reads 1000 --workers 8 \
  --rate 1000000000 --read-p99-ms 20 --profile
```

Use a fresh output directory and canonical paths; the driver resolves paths before
starting a pull. A filesystem clone is used where available. The last staging copy
is retained and every received chunk is independently read back against its source
digest. Source/target profiles and JSON counters remain in the output directory.
Do not use the fixture command on an existing store.

The default loopback driver holds the 1 GB/s bandwidth ceiling fixed and bypasses
host preflight; it does not invent Linux disk or NIC telemetry. `--lease FILE` can
add a real controller's expiring admission lease. The unified governor's ramp,
resource binding, durability guards and lease expiry have separate tests; this
Mac experiment does not validate physical host telemetry. Compare before/after
builds with the same driver, fixture and workload. CPU/stored GB includes foreground
CPU unless explicitly adjusted using the idle phase. Short phases do not enforce a
commit-rate ratio; steady phases require at least 98% of the offered point rate.
They also require at least 95% of the requested read rate and no empty/error reads.

## Projected baseline times

At a sustained 500 MB/s, a 1/3/5/6 TB stored checkpoint needs about
33/100/167/200 minutes. At 1 GB/s NIC speed the corresponding lower bounds are
17/50/83/100 minutes. A 2 GB/s disk-write limit alone gives 8/25/42/50 minutes;
the binding bound is the maximum, rather than the sum. Foreground reservations,
source reads, checksum verification, retained logs, replay, startup and readiness
add time. Shared replicas and network links require per-host/direction budgeting;
these figures are not a guarantee that many concurrent hosts achieve them.
