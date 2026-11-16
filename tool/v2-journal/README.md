# Exact journal codec comparison

`go run ./tool/v2-journal -input /path/to/input-log -partition 0 -segments 7 -compare-series-clocks`

This offline tool opens source segments read-only, excludes the newest segment,
checks header/frame/payload CRCs through the incumbent replay decoder, and rejects
segment recycling. It selects one independent source hash partition; the source
byte numerator includes its original frame headers, payloads and segment headers.
Use at least ten minutes of segment creation-time coverage. No writable database
is opened. Default selected-source read limit is 20 MB/s and GOMAXPROCS is one.

PDC2 uses the existing probe's source-frame record cuts, 4 MiB expanded estimate,
and segment-wide dictionary. Arrival clocks retain their full nanoseconds.

PDC3 carries a name/opaque-definition dictionary and per-series timestamp
(delta-of-delta) and Gorilla IEEE XOR predictors within each segment epoch.
Frame clocks use exact signed deltas from the preceding frame clock. Names and
IDs are varint framed. Metadata and value bits form separate streams before
single-threaded fastest zstd with a 1 MiB window. Every record has a checked
expanded length, zstd checksum and CRC32C covering the discriminator, length and
compressed payload. Sequence numbers and predecessor CRCs reject skipped or
reordered continuation blocks. Replay/copy starts at a segment epoch; later
blocks need that epoch's state.
`-independent-blocks` resets state in every block for independent block recovery.
`-series-clocks` predicts each frame clock from the previous clock of the SAME
series instead of the preceding frame. `-compare-series-clocks` measures both
clock choices at all three block budgets on the exact same input, using separate
stream states. Neither clock choice reduces timestamp precision.

The 1/4/16 MiB cuts count canonical decoded input bytes:
`24 bytes per apply group + name bytes + definition bytes + 16 bytes per point`.
Complete apply groups remain intact; cuts are **not compressed block sizes**.
By default predictors reset only at segment boundaries. Reported journal bytes
include an
additional 32 bytes per record for the surrounding journal header; they exclude
production policy/catalog/checkpoint records and filesystem allocation.

Encode/decode CPU intervals use process user+system CPU with GOMAXPROCS=1;
source I/O, source decoding, comparison and deliberate rate-limit sleeps are
outside codec intervals. Compressor initialization and periodic GC inside an
interval count. Source decode CPU is reported separately and includes verified
source reads. Incumbent encode CPU is not reconstructed by this tool.

Every point, frame boundary, metric-name byte, metadata byte, signed timestamp,
arrival nanosecond and float bit is compared on each block. Policy definitions
are empty because the source input-log record has no v2 policy definitions.
This is a codec experiment and its first real consumer, not a daemon integration
or a crash/replay qualification of the production journal.

`-analysis` compares full-nanosecond same-series clocks and the lossless
`SparseClocks` bitmap at 4MiB decoded cuts, checking every tuple, frame
boundary and definition. `fields.pre_zstd` counts exact serialized names, IDs,
timestamps, value bytes (including final bit padding), clocks, definitions and
framing. These sum to the raw zstd input; `Headers` adds the 25B PDC3 and 32B outer
record headers. `Encoded` is the actual compressed total including headers.
SparseClocks charges its presence bitmap to clock bytes. Individual compressed field sizes are undefined because fields share one zstd
stream. No proportional allocation is presented as an exact compressed size.

Add `-streams 1,4,8,16,32,64 -sample-points 250000 -repeats 8` to measure independent
stream encoders on a repeated real corpus retained during analysis. Each stream
has its own dictionary, predictors and single-thread zstd instance. Encode rates
include allocations/GC and output retention; decode rates include full exact
comparisons. The corpus is shared read-only. These finite CPU measurements exclude
admission, writes/fsync and backend replay. They do not establish end-to-end
capture throughput, sustained performance or a particular process-wide core cap.

For a representative parallel comparison, load a **complete original segment**
and encode it **once** per stream:

```sh
go run ./tool/v2-journal -input /path/to/input-log -segments 1 \
  -parallel-only -sample-points 2000000000 -repeats 1 -streams 8,16,32
```

Verify `analysis_done.sample_points == analysis_done.points`. Repeating a small
corpus can make every name's value match its preceding pass and artificially
improve XOR prediction; those warm curves are diagnostics, not capture evidence.
`-analysis` and `-parallel-only` select the newest requested complete segments to
reduce recycling risk. Source descriptors and headers are checked before/after.

For a durable name-catalog comparison on the same selected source points:

```sh
go run ./tool/v2-journal -input /path/to/input-log -segments 6 -read-mbps 10 \
  -catalog-dir /path/to/experiment
```

The four variants are normal PDC3, sparse-clock PDC3, normal PDC4 and sparse-clock
PDC4. PDC3 restarts its dictionary per segment. PDC4 restarts only predictors;
stable IDs refer to a stream-owned durable catalog shared across those segments.
Names, opaque definitions, clocks, frame order and every point bit are compared.
Raw field accounting charges the catalog names/definitions/IDs/framing once;
`catalog_storage` separately reports actual compressed checkpoint plus WAL bytes.
Add those bytes to PDC4 record bytes for a finite-cohort total. Checkpoint bytes
are not hidden or attributed proportionally to jointly compressed fields.

`-capture-seconds 120 -capture-streams 32` then tests appending the retained real
cohort with a one-second group-fsync interval. Run capture on CPUs0–95 of a
192-CPU benchmark machine; the tool pins its separate raw-TCP sender process to
CPUs96–191. Each capture stream owns a catalog, encoder and file. Catalog seeds
are checkpointed before capture. New definitions use `EncodeDurable` before the
referencing append; group sync waits for catalog sync before file sync. The
reported p99 measures that complete group-sync call, including catalog work if
needed. Throughput includes receive/decode/encode/write and final sync. Capture
CPU excludes the separate sender; elapsed time includes its startup. Seed
catalog bytes are reported separately from record bytes. Every captured record
is replayed and compared to the exact sender tuples after timing.

The sender repeats an explicitly retained real cohort (at most 30M points).
Predictors remain warm across repetitions; this sustained IO test is distinct
from a one-pass representative codec test or production admission/backend replay.
The probe uses a four-byte outer record length plus PDC4's own CRC; ordinary
comparison accounting includes the modeled 32-byte production journal header.
Output includes source selection and exact point counts. Use a fresh output
root for each invocation, and an external daemon-health guard on shared hosts.

A completed timed capture can be verified again, independently and concurrently
per stream, without rerunning capture:

```sh
go run ./tool/v2-journal -catalog-verify /path/to/experiment/capture \
  -capture-streams 32
```

The verifier reads the persisted catalog and each segment, rewinds the exact raw
sender fixture at its recorded repetition boundary, compares every tuple and
reports the total verified point count. Match that count to the original capture
receipt. Verification has its own resource/health guard on a shared host; it is
outside the timed capture interval.


`-capture-unseen-every N` generates one globally never-repeated name per N
points per stream, deterministically across fixture cycles. `-capture-unseen`
retains its historical 10% spelling. The persisted churn manifest drives the
same transformation during full tuple verification. All stream checkpoints,
compressed catalog WALs and journal bytes are charged in `capture_storage`.

Capture now retains encoded records until the one-second group boundary, syncs
the compressed catalog suffix, appends the records and syncs the journal. Its
catalog timing includes group compression, WAL flush and catalog fsync; group
p99 includes both sync calls and journal append. No per-record catalog sync is
performed. The two files still require separate sync syscalls for ordering.

Use `-catalog-fixture-only -catalog-dir DIR -input INPUT -segments N` to read
complete immutable input-log segments without running codec variants, report
first-observed name counts and source arrival intervals per segment, and save
the first 30M source points as a raw fixture. The first segment warms the seen
set; later first-observed-name fractions upper-bound true lifetime novelty.
They are not daemon new-metric counters and do not prove post-restart novelty.
All source reads remain read-only and use the existing read-MB/s limit.
