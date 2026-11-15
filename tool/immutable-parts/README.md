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

Each producer owns a two-slot atomic queue per shard. One owner per shard
collects bounded heads, sorts at one second or the byte bound, and publishes
immutable Snappy SST blocks without reading existing data. Data and the producer
frontier vector use separate, disjoint SSTs, ingested in one atomic manifest edit.
Putting the global frontier key in each data SST would make their ranges overlap
and cause unnecessary compaction. Blocks and replay position commit together.
`-core-blind-batch` compares the same ordered puts in a synced blind WAL batch.
An optional snapshot hook publishes a detached head and Pebble snapshot; callers
must release every pin before closing the engine. No application data mutex is
shared with readers or producers. Pebble has its own internal synchronization.

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
