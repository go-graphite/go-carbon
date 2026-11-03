# Point cache encoding

`compactPoints` is a memory-only, lossless bit stream. It preserves arrival
order, int64 timestamps and all float64 bits. It is independent of the disk
and input-log codecs.

The first point contains a zigzag uvarint timestamp, one scale byte, and
either a zigzag uvarint integer seed or eight little-endian float bytes.
Scale 0 means raw bits, 1 means integers, 2 means hundredths, and 3 means
thousandths. Subsequent fields are packed least-significant bit first.
The control codes below are written in **reading order**, not numeric order.

Timestamp delta-of-delta codes are `0` for zero, `10` plus 7 zigzag bits,
`110` plus 9 bits, `1110` plus 12 bits, or `1111` plus 64 bits. The first
point leaves the timestamp delta at zero. Subtraction/addition wrap as int64
in both directions, including across MinInt64/MaxInt64.

A numeric run uses these value codes:

| Code | Meaning |
| --- | --- |
| `0` | Identical float bits; integer delta becomes zero |
| `10` | Repeat the previous integer delta (zero delta-of-delta) |
| `110` | New zigzag integer delta |
| `1110` | XOR escape; next bit selects reused (0) or new (1) window |
| `11110` | Zigzag integer delta-of-delta |
| `11111` | Numeric seed: two scale bits and a zigzag integer |

The encoder chooses delta or delta-of-delta at each integer value, including
the short code for repeated deltas. Integer fields use `0` + 4, `10` + 8,
`110` + 16, or `111` + 64 zigzag bits. The encoder chooses between numeric and XOR representations by their
encoded cost. An XOR escape starts a raw run. Raw runs use `0` for identical
bits, `10` for a reused XOR window, `110` for a new window, or `111` for a
numeric seed. A new XOR window stores 6 leading-zero bits and 6 bits for
significant width minus one, followed by the significant bits. A reused
window must contain the new XOR and cost no more than a fresh window.

Decimal normalization is exact, not quantization. After rounding the scaled
value to an int64, the encoder divides it by the scale and compares the
result's **float bits** with the input. Only identical results qualify.
Range checks precede int64 conversions. Negative zero, NaNs (including
payloads), infinities and all other nonqualifying values use XOR. Raw runs
probe integers and check decimals every 16 points; numeric runs can promote
to exact decimals immediately. An unusual value can therefore interrupt a
numeric run without disabling it for the rest of the series.

## Unlocked readers

Every complete 64-bit word is appended once to `buf`. A partial word stays
in the struct's `tail` and `used` fields. Copying the struct under the shard
lock therefore captures the entire unfinished word as well as the immutable
buffer length. Readers never inspect spare capacity. A writer can fill and
publish later words without touching a byte or bit an existing snapshot
reads. No atomic read/modify/write of a shared byte is needed.

Capacity grows by one quarter, with a minimum 8-byte increment. The
allocation uses an append to nil so the slice exposes the runtime
allocator's rounded capacity and the writer can use its padding. Existing shard capacity hints remain in use. Memory accounting
includes the larger encoder struct automatically through `unsafe.Sizeof(item{})`.
The wider growth step reduces copying and allocation CPU; the mixed-workload
capacity regression gate is 1.90 B/point, including the full partial word.
Benchmarks distinguish encoded bytes (including the partial tail), slice
capacity, state plus capacity, and actual retained heap with allocator rounding.
Metric names, indexes and other cache metadata are reported separately.

## Upgrade and rollback

The bit stream never crosses a process boundary. Compact and block dumps
transcode it to `internal/recovery.AppendCompactPoints`, the existing byte
format; pending recovery uses the existing checked decoder before encoding
points in memory. Legacy dumps, compact dumps, block dumps and input logs
retain their current identifiers and bytes. A new binary reads all existing
formats and a previous binary reads every dump the new binary produces.
There is no new durable format to negotiate. The byte-for-byte dump test and
checked recovery decoder enforce this boundary.

Transcoding adds shutdown/recovery CPU and removes the block dump's borrowed
queued-buffer path. Scratch is reused across shards and holds one decoded metric, not the
whole decoded cache; block writers copy the reused encoding scratch. Measure
restart and shutdown behavior in the first canary as well as steady-state heap,
CPU and reads.

## Local measurements

`BenchmarkCompactWorkload` covers 100-point and 4,096-point series, append with
reused capacity, fresh append with growth, reusable decode, and allocating
reads. On Linux and macOS, process CPU time is reported alongside elapsed
time so scheduling delays on shared hosts do not inflate core estimates.
It shares deterministic stream-codec value generators; half the counters
use variable increments and timestamps include anomalies from the cache's
existing generator. The mix is 50% counters, 20% stable integer gauges, 20%
rounded gauges and 10% random-mantissa floats (70% integer series).
`BenchmarkCompactCacheCycle` includes single-point ingestion and confirmed
writeout. `CACHE_COMPACT_MEMORY=1` enables `TestCompactWorkloadHeap`, including
random run lengths averaging 105 points and a 30%-random-float sensitivity case.

A 70%-integer share alone cannot guarantee a fixed bytes/point budget. If the other 30%
contains independent 52-bit mantissas, those mantissas alone require 1.95
bytes/point before timestamps, integer values, metadata or slack. Whole-cache
heap also includes metric names and indexes; a point-buffer target is not a
whole-cache heap claim. Measure the actual mix before a wider rollout.


Queued item headers keep the input log's `StreamRef` as their single metric
name. A raw `points.Points` header is created at writeout, or earlier only when
pending recovery needs a stable identity; quota state stays inline to avoid a per-metric allocation and pointer
lookup on configured ingest paths. A successful write, retry, and live successor keep their
own identities. This changes no dump, handoff, or input-log bytes.

A shared name registry can later supply metric IDs at `newItem` and the
`itemMap.get*` / `setHashed` boundary. That requires the same registry for cache,
input-log naming, pending recovery and stream names. This branch continues to
key and validate items by full metric names; ID keys belong to that separate
integration.


Input-log staging keeps raw 16-byte points beside 8-byte references. It avoids
compressing each point twice. The log consumer resolves each consecutive
reference run once, fills reusable row scratch, then uses the existing durable
encoder. Recycled columns release burst capacity after a much smaller window;
ordinary windows retain and reuse their arrays.
