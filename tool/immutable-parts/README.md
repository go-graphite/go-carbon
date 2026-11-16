# Immutable parts experiment

This standalone command tests the cost of receiving already decoded metric-ID
records, sorting bounded private owner buffers, and publishing immutable SSTs to
fresh Pebble databases. It does not integrate with the daemon or define a
supported storage format. Never point it at an existing database directory.

```
go build -o immutable-parts ./tool/immutable-parts
./immutable-parts -mode prepare -journal sealed-copy.log -fixture records -points 450000000
./immutable-parts -fixture records -out trial-16 -shards 16 -steps 1
./immutable-parts -fixture records -out private-16 -shards 16 -steps 1 -db-per-owner
./immutable-parts -fixture records -out trial-64-rollup -shards 64 -steps 1,60,3600 -readers 8
./immutable-parts -mode check -fixture records -out differential
```

Preparation reads a copied sealed journal, checks its frames through the existing
reader, canonicalizes names, assigns IDs and writes 192 fixed-record buckets.
Its throughput is reported separately. Timed runs include reading these buckets,
batch transfer, sorting, block encoding, SST sync/ingest, automatic Pebble
compaction, final flush and database close. Input records have metric ID,
timestamp, IEEE value bits and original sequence (32 bytes total). This is a
ready-record sink measurement; journal decoding/name admission and arbitrary
producer routing are outside its timing. Repeated passes move timestamps and
sequences forward and must be identified when reporting results.

Each metric has one owner. One producer per owner reads its prepartitioned input
and transfers batches through two atomic slots. A batch is at most 1 MiB; the
queue, one producer allocation and owner buffer impose byte bounds because rows
have a fixed size. `-max-rows` bounds each owner's mutable buffer, which seals at
that size or on the first received batch after one second. Sorting is by metric,
time and source sequence. No application data mutex or read-before-write is used.
Pebble SST ingestion, manifests, snapshots, cache and compaction retain their
internal synchronization. This is not a completely lock-free system.

`-db-per-owner` isolates each owner in its own fresh database and publishes one
snapshot per owner. It limits each database to one background compactor and one
table-cache shard; this comparison includes those deliberate settings. The
default uses one database with up to 16 compactors.

Raw blocks preserve every original sample. Optional schema/aggregation config
files use the existing Graphite parsers. Resolution ladders are computed at
sealing; the summaries are provisional because other parts can complete a window
or supersede a sample. Final rendering merges raw records by source sequence,
then aligns/deduplicates fine slots and computes each retention level in order.
Raw retention expiry and a background correction-aware logical compactor are not
implemented. Pebble compacts physical KV blocks only. The experiment must retain
raw blocks for correctness; it cannot estimate production steady-state retention
space or fully settled write amplification.

Concurrent readers acquire an atomic, refcounted published Pebble snapshot.
Each metric owner publishes its own reference serially, including in the shared
database layout, so concurrent publishers cannot move a reference backward. The
last reference closes it, so a reader never uses a closed snapshot. They request
random metric IDs over one-hour and seven-day ranges, merge raw parts and compute
rollups. The mutable head is not query-visible until its SST is published. A
production implementation must atomically publish a frozen head together with
its part set to eliminate this visibility gap. A lock-free writer does not make
Pebble block loading or I/O lock-free. Reader latency fields are logarithmic
histogram bucket lower bounds (within a factor of two), combining both ranges;
they include empty results during cold population and exclude metadata lookup.

`-mode check` samples eight actual fixture metric IDs, writes and reopens their
SSTs and compares raw IEEE bits/source order plus 1s/60s/3600s render slots with
unmodified go-whisper. The test suite also checks ordered late corrections,
NaN payloads, signed zero, sparse occupancy and average/sum/min/max/last. This is
not proof for arbitrary streaming history: late points outside the fine window,
rollup invalidation, retention edges, schema changes, NaN aggregation and update
batch order require a broader compatibility state machine.

Native Linux output includes process `/proc/self/io` write bytes, CPU ticks
(100Hz Linux USER_HZ) and RSS high water. Report disk MB/s from those bytes over
the complete run and write amplification relative to 32 input bytes/point;
physical device traffic from other processes and filesystem metadata are outside
that attribution. SST bytes are also reported separately. Any compaction debt at
close means these are finite-run costs, not steady-state endurance numbers.

## Durable owner experiment

`-mode core-bench` measures a separate owner-local sink with independently
configurable `-shards` (Pebble instances), `-core-producers`, CPU affinity and
`GOMAXPROCS`. It generates synthetic preidentified rows; it does not measure
network intake, canonical-name admission, rollups or existing daemon APIs.

```sh
immutable-parts -mode core-bench -out trial -shards 16 \
  -core-producers 16 -core-duration 30m -core-rate 25000000 \
  -max-rows 2097152 -core-settle 60s
```

Each producer owns a single-producer atomic queue per shard with a 256 KiB
accounted payload quota. Ready owners receive packets before a producer waits
for full queues. One owner per shard collects bounded heads, sorts at one second or the byte bound, and publishes
immutable Snappy SST blocks without reading existing data. Data and the producer
frontier vector use separate, disjoint SSTs, ingested in one atomic manifest edit.
Putting the global frontier key in each data SST would make their ranges overlap
and cause unnecessary compaction. Blocks and replay position commit together.
`-core-blind-batch` compares the same ordered puts in a synced blind WAL batch.
An optional snapshot hook publishes a detached head and Pebble snapshot; callers
must release every pin before closing the engine. No application data mutex is
shared with readers or producers. Pebble has its own internal synchronization.

The offline core has three opt-in filesystem/CPU experiments. None changes the
persisted configuration, keys, apply groups or final durable-ingest boundary:

* `CORE_SST_PREALLOCATE=1` accumulates one encoded SST per owner, preallocates
  its exact final length through the VFS, writes it, and performs the original
  final sync before ingestion. The owner's encoded-buffer capacity is reused;
  budget it in addition to its mutable head and admission queues. Platforms
  whose VFS treats preallocation as a no-op still write and sync normally.
* `CORE_SST_DIRECT=1` implies preallocation and writes a private 4096-byte-aligned
  copy using Linux `O_DIRECT`. Padding is truncated to the exact SST length
  before the original final fsync. The retained aligned copy adds another
  encoded SST buffer per owner. Unsupported filesystems return an error; there
  is no silent fallback. Readers use independent buffered descriptors.
* `CORE_BUCKET_SORT=1` groups the highest varying metric-ID byte before sorting
  each group with the original comparator. A private scratch slice adds 32
  bytes per head row. Mixed time partitions and ambiguous equal sort keys use
  the original sort, including its exact value-bit tie behavior. The scratch
  capacity is reused across seals.

All are disabled by default pending native stability/CPU measurements. Use
fresh private directories and compare each option separately before combining:

```sh
python3 tool/immutable-parts/io_sweep.py --out /scratch/new-io-sweep
CORE_SST_PREALLOCATE=1 python3 tool/immutable-parts/numa_bench.py \
  --binary /absolute/path/immutable-parts --out /scratch/new-preallocate-trial \
  --nodes '0:0-47,96-143' --producers 48 --owners 48 --dbs 48 \
  --duration 120 --profiles
```

Adapt CPU IDs to actual topology and preflight/monitor colocated services. The
fio sweep writes only fresh files below its output directory; a `STOP` file in
that directory terminates its active fio process. Its end-of-stage mode syncs
each file at the end; it does not issue a filesystem-wide sync or establish a
single-sync seal protocol. CPU profiles are saved separately for each NUMA
pipeline. Start records identify all experiment flags. Lease-credit wait
seconds/count and the live waiting-producer gauge are distinct from edge-queue
waits. Phase, preallocation and file-write seconds are overlapping wall times,
not additive CPU costs; use CPU profiles for per-stage CPU attribution.

Keys contain a time-partition prefix followed by canonical numeric metric ID,
archive, block start, immutable part and block ordinal. IDs must come from an
exact canonical-name catalogue: routing hashes IDs but does not replace identity
with a hash. Raw archive blocks preserve timestamps, IEEE value bits and original
source sequence. The stopped-owner expiry primitive deletes complete logical
partition ranges and atomically persists an expiry floor. Pebble compaction
reclaims physical bytes later. Automatic retention and authoritative rollups
are not integrated, so enabling expiry is not safe for a multi-archive store.

`-core-append-parts` instead orders the numeric experimental keys by time
partition, immutable part, metric, archive and block. This keeps successive part
ranges disjoint within a partition. Single-partition heads use an equivalent
sort comparator that avoids signed partition division on every comparison.
These numeric keys are not the proposed portable format in
`internal/partsformat`: portable identities, virtual-shard file boundaries and
retained copy files remain separate integration work. A data SST can still span
time partitions, so logical range deletion does not imply immediate whole-file
physical reclamation.

`-mode core-replay -journal segment.log -out recovered` decodes an immutable
single-segment input journal through the same producer and owner path. Restart
reconstructs the name catalogue by decoding the dictionary prefix and resumes
from the minimum durable owner frontier. Owners ahead of that minimum skip their
already committed prefixes. Header identity and engine configuration are
validated before reuse; staging SSTs left before atomic ingest are removed.
This does not implement live multi-segment journal retirement, dictionary
checkpointing or a production durability acknowledgement contract.

Samples report committed and submitted counts, process CPU/IO/RSS, estimated
compaction debt, L0 files and write stalls. The final seal checks that every
submitted row committed; scheduled points and `target_met` expose backpressure.
A finite run, nonzero debt at close, or a shortfall against the offered schedule
cannot qualify sustained throughput. `-cpuprofile`, `-mutexprofile` and
`-blockprofile` write profiles for diagnosing a plateau with unused hardware.

Correctness tests include independent producers, raw bit parity, restart replay,
SIGKILL between owner publications, SIGKILL during actual journal replay, idle
one-second sealing, frozen-head immutability and pinned partition-expiry views.
These raw-archive tests do not establish render/rollup equivalence.

Original apply batches can use `submitBatches` with `openCoreWithHooks` and a
pure `PlanSeal` callback. The callback receives original clocks, order, opaque
policy bytes and metric boundaries. It returns sorted, disjoint extension SSTs
and a nonfailing postcommit publication callback. Raw data, extensions and owner
frontier ingest atomically; the flat submit path rejects a configured planner.
This is an offline numeric engine hook, not an importable production backend.
`durableFrontiers` safely snapshots committed owner vectors during intake; log
retirement also requires the log's own synced frontier.

For CPU topology experiments, set `CORE_OWNER_CPUS` and `CORE_PRODUCER_CPUS` to
comma-separated explicit Linux CPU IDs. Each list assigns goroutines round robin
to locked OS threads. Empty lists use runtime scheduling. Consult actual topology
first; affinity does not make cross-node routing or Pebble background work local.
Use an offered rate well above measured throughput when looking for a ceiling;
`-core-rate 30000000` itself caps a scaling curve at 30M points/s. Per-owner seal
sort/encode/ingest wall seconds and aggregate producer queue wait are reported,
alongside current live SST bytes and process counters. Queue wait sums can exceed
wall time and `/proc/self/io` syscall counts are not physical-device IOPS.

Producer dispatch counts rows per owner before allocating one exact backing
slice, then gives each owner a disjoint region with a fixed capacity. This
avoids repeated geometric allocations while preserving caller-owned input:
callers can reuse their rows immediately after `submit` returns. The owners
still copy those private packets into their own heads before sorting.

### NUMA-local pipelines and bounded dispatch

`core-bench` reuses producer routing buffers under an 8 MiB in-flight payload
quota per producer. Queue pointer capacity follows the smallest packet charge;
lease capacity follows producer metadata charges, rather than a fixed packet
count. An indivisible packet larger than a quota is admitted only when that
queue or producer is empty. Retained backing capacity and owner heads are
separate from these in-flight accounting limits. A lease lasts until all
database consumers finish copying its disjoint regions. Producers can reuse
their input immediately after submission. Apply-batch policies, order and points
remain detached with their original boundaries. SST key/value scratch is reused
after `Set` returns.

`-core-owners` sets execution-owner count independently of `-shards` (database
count) and `-core-producers`. An execution owner can poll several databases;
routing, snapshots and durable frontiers remain per database. The execution-owner
count can change on restart without changing persisted routing.

For a Linux machine with two NUMA nodes, launch separate runtimes with CPU and
memory binding for the entire pipeline, including Pebble background threads:

```sh
python3 tool/immutable-parts/numa_bench.py --binary /absolute/path/immutable-parts \
  --out /scratch/new-trial --nodes '0:0-47,96-143;1:48-95,144-191' \
  --producers 48 --owners 48 --dbs 48 --duration 120
```

Use CPU/node IDs discovered on the actual host. Counts are per node; offered
rate is aggregate. Synthetic metric IDs are split before pipeline entry into
disjoint subsets by `-core-pipeline`/`-core-pipelines`. There is no cross-node
producer-to-owner queue. Each process has its own allocator, scheduler, head
buffers and databases. This launcher exercises the offline numeric engine; it
does not implement portable admission, log routing or cross-node live recovery.
Per-second heap/GC, memtable, flush, L0 and phase counters accompany process I/O.
The launcher retains measurement JSON and deletes its synthetic databases.

Core samples distinguish cumulative external SST durability-sync seconds and
maximum individual sync time from total encode time. Runtime counters include
GC CPU, mark-assist CPU and scannable heap bytes; completed GC counts alone do
not measure concurrent collector work. These measurements complement the L0,
memtable, flush, compaction-debt and owner-phase counters. External stage files
are created outside Pebble's object provider, so its `BytesPerSync` option does
not apply to them.

External SST output reuses a 1 MiB write buffer per owner. It drains before the
original final durability sync and propagates both flush and sync failures. This
batches small table writes without changing SST bytes or manifest publication.
Queue tests cover ready-owner progress, FIFO, original apply groups, indexed
seal acknowledgements, byte-credit refunds and indivisible oversized packets.

### Endurance qualification status

A complete 1800-second trial used 24 independent processes, each with eight
selected CPUs, four producers and one database. All 680,392,982,528 submitted
numeric points committed, every process closed successfully, and write stalls
were zero. Both stability gates fail; keep the PR Draft.

| Selected CPUs | Intake seconds | Mean M points/s | Worst sliding 10s M/s | Worst / raw mean | CPU ns/point | Written B/point | Gate |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
|192|1800.336|377.925459|309.181923|81.8103%|77.5300|1.900716|Raw FAIL|
|192|1800.336|377.925459|331.215529|87.6404%|77.5300|1.900716|Declared-filter FAIL|

The gates require worst-ten-second throughput at least 90% of the same raw mean
and CPU cost at most 120ns/point. Mean and per-point costs include final sealing;
close, the two-second observation interval, reporting and temporary database
deletion are excluded. Start skew was 41.112ms and mean busy CPU count
was 29.301; selecting 192 affinity CPUs does not mean 192 busy CPUs.

The factor-two external-writer rule was declared before measurement. The fresh
60.002-second baseline accounted for 273.900MB/s of service writes;
windows above 547.799MB/s were excluded only from the separate diagnostic.
Eligible sliding-window coverage was 89.8107%, with
161/180 eligible aligned windows averaging 381.653235M points/s.
The raw trough coincided with 1121.394MB/s of service-accounted writes.
The filtered gate still fails, so external writes do not explain away instability.

Unfiltered estimated debt over the final 600 seconds had a fitted slope of
-0.916206B/s, ranging from 13731.7 to 49184.8 bytes;
interpolated endpoints were 26737.9 and 34829.0 bytes.
The post-close total was 29763 bytes. A downward fitted slope does not
mean every debt sample decreased. Default and explicit legacy exact and actual
SIGKILL journal-replay suites passed on native Linux with CGO and Go 1.27,
alongside build, vet and the complete touched-package tests.

A new trial needs its own booked window and fresh 60-second baseline. Wait for
cache to fall only when it exceeds 2B points. Stop only benchmark IO on write
errors/drops, service-incarnation change, cache above 3B points, or cache growth
above 2B from baseline. Retry a missing scrape three times; do not turn missing
counters into zero. Input-log age and short-window commit/intake ratios are not
health signals for this burst-writing workload. Report commit/intake over windows
at least as long as the service's configured maximum writeout delay. The earlier
120-second ratio condition is withdrawn. A guard stop ends the attempt; do not
retry within the same booked window. This trial had no guard trip, counter reset
or process-IO permission gaps.

For the separate diagnostic, match PID and process start time, reject permission
gaps, use the raw mean denominator for both gates, show eligible coverage, and
retain every debt sample:

```sh
python3 tool/immutable-parts/endurance_report.py \
  --trial /scratch/trial --system /scratch/system.jsonl \
  --baseline /scratch/baseline.json --factor 2 --out /scratch/endurance.json
```

This is the unchanged offline numeric engine: input-journal/network/name admission,
authoritative rollups and retention remain outside its timed path. This trial
establishes neither complete-engine throughput nor hardware-only scaling, portable
copies, native-engine superiority or production readiness. No engine or persisted
format changed in this documentation update.
