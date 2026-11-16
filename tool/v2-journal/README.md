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

`-analysis` compares full-nanosecond same-series clocks and the reversible
`ClockDeltaOfDelta` predictor at 4MiB decoded cuts, checking every tuple, frame
boundary and definition. `fields.pre_zstd` counts exact serialized names, IDs,
timestamps, value bytes (including final bit padding), clocks, definitions and
framing. These sum to the raw zstd input; `Headers` adds the 25B PDC3 and 32B outer
record headers. `Encoded` is the actual compressed total including headers.
Individual compressed field sizes are undefined because fields share one zstd
stream. No proportional allocation is presented as an exact compressed size.

Add `-streams 1,4,8,16,32,64 -sample-points 250000 -repeats 8` to measure independent
stream encoders on a repeated real corpus retained during analysis. Each stream
has its own dictionary, predictors and single-thread zstd instance. Encode rates
include allocations/GC and output retention; decode rates include full exact
comparisons. The corpus is shared read-only. These finite CPU measurements exclude
admission, writes/fsync and backend replay. They do not establish end-to-end
capture throughput, sustained performance or a particular process-wide core cap.
