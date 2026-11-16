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

Journal admission can use `partlog.OpenAdmission` before opening the owner
storage. It reserves a new monotonic cursor epoch with a synced fence and opens
each fresh append file on its first append, without scanning historical payloads. `WaitReady(ctx)` joins
historical validation; `Replay`, `Follow` and truncation wait for that boundary.
`RecoveredTails(ctx)` returns the fixed pre-admission cursor vector, excluding
early live appends and empty epoch headers, for independent recovery timing.
Live records enter the new epoch, and ordered followers consume old epochs
first. A validation failure surfaces on `Errors`, fails readiness and prevents
further acceptance. Close joins recovery before releasing directory ownership.

Frames and opaque payloads keep their encoding. New CRC-protected epoch fences
and retained-frontier metadata are part of the journal's recovery state and must
be copied with it. Cursors are monotonic opaque positions, not record counts;
each epoch reserves 48 low bits for stream records. Fences are retained, and an
exhausted epoch space fails closed. Older binaries cannot read fenced gaps; do
not roll back to a reader without epoch support after enabling this experiment.

The `replay-live` benchmark opens admission and starts live journal appends
before opening Pebble. Its first-accept receipt uses the wrapper exec timestamp
separately from recovery and storage-open receipts. `TestLogAdmissionExecBenchmark`
is a journal-only startup probe: set `PARTLOG_ADMISSION_BENCH_ROOT`,
`PARTLOG_BENCH_PRODUCERS` and `PARTLOG_BENCH_EXEC_UNIX_NS`. It deliberately holds
historical validation until a successful append. This probe does not establish
production listener latency, downstream visibility or sustained replay speed.

### Isolating journal replay stages

The log backlog harness also accepts `PARTLOG_BENCH_PHASE=read` (framed
read and CRC validation) and `decode` (the same work plus numeric-row decode).
Run these against a freshly generated journal before adding concurrent live
records, with the same producer count and expected point total. They reject
incomplete coverage. Their `read_MB_per_second` counts payload bytes and excludes
open/recovery and final close; these are stage ceilings, not storage throughput.
`PARTLOG_BENCH_READAHEAD` selects the per-stream read buffer in bytes (default
4MiB, maximum32MiB); each stream overlaps one sequential read with consumption.
The replay-live settled rate includes journal open, historical replay, accepted
live suffix draining, compaction waiting and final flush/close. Compaction debt
is sampled immediately before close, while the databases remain open.

For a bounded replay-live CPU capture, set `PARTLOG_BENCH_PROFILE` to an output
path and optionally `PARTLOG_BENCH_PROFILE_SECONDS` (1–120, default20). Do not
combine it with `-test.cpuprofile`. It completes the capture even while a slow
replay continues, preserving a usable profile if the external IO guard later
stops the process. Fixture extension via `PARTLOG_BENCH_OFFSET` preserves the
original apply clock of the generated rows.


The journal harness imports `internal/v2core`; it does not retain its own copy
of the owner engine. Five-second samples cover historical replay, accepted live
suffix drain, compaction settlement and final close. The close phase reports
process progress and the last preclose debt snapshot, because closed databases
cannot be sampled. Settlement requires five consecutive 200ms observations with
zero estimated compaction debt, active compactions and active flushes, after
all stream durability barriers complete. The settled rate includes close.

`TestLogSIGKILLRandomReplay` kills a child at three distinct seeded journal-record
offsets and verifies every recovered persisted row for coverage, uniqueness,
timestamp and float64 bits. Set `PARTLOG_RANDOM_POINTS` to raise its default
262147-point fixture on a benchmark host; it must be at least131073. These raw-row
checks do not establish rendered-query parity or production backend integration.

### Replayed render and crash qualification

`go test -tags=sealrollup_integration ./tool/immutable-parts -run '^TestReplayedRenderDifferential$'`
checks 1,680 renders (70 schemas, six aggregation methods, four query widths)
against classic Whisper, native exact blocks, and continuation. Before replay,
the journal records the canonical name, archive policy, original clock, order,
and original apply boundary in each batch envelope. The offline adapter reads
persisted numeric rows, checks coverage and exact bits against those envelopes,
then runs `sealrollup` and publishes immutable archive blocks through `partsread`.
This is a reconstruction oracle; it does not qualify production sealing or add
missing policies to the older numeric throughput corpus.

The opt-in `TestFullScaleReplayCrashes` takes `PARTLOG_CRASH_ROOT`, whose
`fixture/log` contains a historical-only benchmark journal, plus
`PARTLOG_BENCH_POINTS`, `PARTLOG_BENCH_PRODUCERS`, and `PARTLOG_BENCH_OWNERS`.
On Linux it clones that journal into three fresh trial directories, uses seeded
committed-row prefixes to SIGKILL real replay+live child processes, reopens the
retained journal, finishes replay and checks every persisted raw row. It reports
the retained live prefix separately: one-second group commit permits losing an
unsynced tail. It does not claim a rendered oracle for the numeric corpus.

Replay receipts include journal logical file bytes at start/end, generated raw
and frontier SST file bytes, Pebble compaction-output file bytes, and the process
`write_bytes` delta divided by historical plus accepted live points. File sizes
and process block writes are distinct counters; their difference includes
allocation, rounding, manifest and other write effects. Final estimated debt is
reported verbatim; a nonzero residual still fails the strict settlement gate.
