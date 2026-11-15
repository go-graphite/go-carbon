# Exact journal codec comparison

`go run ./tool/v2-journal -input /path/to/input-log -partition 0 -segments 7`

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
reordered continuation blocks. Replay/copy starts at a segment epoch; later blocks need that epoch's state.
`-independent-blocks` resets state in every block for independent block recovery.

The 1/4/16 MiB cuts count canonical decoded input bytes:
`24 bytes per apply group + name bytes + definition bytes + 16 bytes per point`.
Complete apply groups remain intact; cuts are **not compressed block sizes**.
By default predictors reset only at segment boundaries. Reported journal bytes include an
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
