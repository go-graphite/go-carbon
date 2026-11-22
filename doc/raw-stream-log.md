# Experimental raw stream journal

This prototype tests journaling Graphite stream v1 frames without decoding or
re-encoding their points on admission. It is a separate opt-in tool, with no
daemon configuration, startup cutover, automatic expiry, or production adoption.

```
go run ./tool/raw-log -experimental-raw-log -mode bench \
  -dir /a/new/private/directory -rate 1000000 -seconds 3 \
  -connections 280 -live-replay -pebble
```

`capture` records an offered synthetic TCP workload. `bench` records one epoch,
rotates the log, and replays the sealed epoch, optionally alongside another live
epoch. `replay` reads existing raw segments. Each command requires the explicit
experimental flag. The tool uses the strict load test's live name/value pattern;
it does **not** provide that test's historical journal or daemon orchestration.
The microbenchmarks in `internal/rawlog` isolate capture costs and label their
different sync policies. Decode-only callback rates are not store commit rates.

## Capture and durability

A connection supplies its ordinary stream preamble. `WireReader` shares header
validation with `Decoder`, checking reserved bits, lengths, row bounds and CRC32C
without decompressing a payload or interpreting its dictionary. `Writer.Stage`
then copies the **unchanged** complete wire frame to its buffered file writer.
Its return releases the input buffer; it is not a durable receipt.

The caller calls `Sync` once per second; Stage also forces a sync at 64 MiB of
unacknowledged capture data. Only successful file sync advances the durable
sequence/point frontier. I/O errors are sticky and prevent further capture.
The buffer is 1 MiB; synchronous file writes provide backpressure rather than
an unbounded heap queue. A sync holds the writer mutex, so this prototype still
pauses admission during each sync. It removes per-frame sync waits, not all waits.
Connection count and aggregate decoder dictionary memory are not yet budgeted.

Stream v1 has **no application acknowledgement message**. TCP send completion
means bytes reached a network stack; it does not mean disk durability. The
internal durable receipt must not be represented as a new relay acknowledgement.
Unacknowledged input can be lost on crash; its elapsed exposure includes a sync's
latency and any blocked scheduling, not a hard one-second upper bound. No
acknowledged prefix is intentionally discarded.

## Segment state and crash recovery

A raw segment begins with `GCRAW001`, a snapshot prefix, and a ready marker.
Records contain a kind, bounded length, uint64 sequence, connection ID, original
arrival time, and an outer CRC covering metadata and payload. The wire frame's
own header and CRC are retained. The format is deliberately separate from the
existing journal; unknown versions fail closed.

For every active connection a snapshot prefix has its checked preamble followed
by reset and bounded state frames. These contain names, predictor-valid bits,
timestamps and IEEE value bits, with no points or caller slots. A ready marker
contains the preceding segment's final sequence. Only then do ordinary capture
records start. Connections opened later start from their preamble/empty state.

A CRC-only writer cannot synthesize predictor state. The prototype's exclusive
`Rotate` fence first syncs the old segment, decodes its durable records, builds
snapshots, creates the new segment exclusively, syncs its prefix, then syncs the
directory before accepting more input. Old segments remain pinned. Production
should reuse replay workers' already-decoded state at an exact record fence;
otherwise rotation decodes the same points twice and may pause intake for a
large fraction of a segment's replay time. Never generate a snapshot by decoding
an incoming, not-yet-durable frame merely to permit its append.

The prototype refuses to reopen an existing segment for writing and performs no
automatic repair. Truncated records, bad outer CRCs, sequence gaps and incomplete
snapshot prefixes fail. A clean EOF at a complete record boundary is valid.
Recovery in a production owner would scan to the last verified durable frontier,
ignore an incomplete new segment while retaining its old predecessor, and resume
at a persisted apply cursor. An outer CRC/sequence failure cannot be skipped by
guessing the next frame boundary.

Snapshots can be large: each predictor-bearing dictionary entry carries its
name, length varint, one validity byte and 16 predictor bytes, plus framing and
optional compression. Budget the **sum over active connections**, not unique
metrics across the store. Five million 100-byte names would exceed the default
64 MiB dictionary budget long before reaching that name count. The encoder's
normal dictionary resets clear predictor state; keep those reset frames verbatim.
Snapshot frames are bounded but a whole snapshot is not one small fixed record.
The prototype materializes whole snapshots and retains closed connection state;
streaming snapshots, connection lifecycle eviction and global budgets remain work.

## Replay, order and policy

The journal assigns a total record sequence under one append owner. Each wire
frame also has its original row order; stream v1 already stably sorts rows by
connection-local ID. A worker owns each connection's decoder. Workers can decode
independent connections concurrently. A bounded reorder window applies results
in record order, and rows remain in the decoder's order within a record. Thus
same-metric updates from two connections do not exchange correction precedence.

The prototype conservatively orders **all** apply callbacks. A production
scheduler can dispatch to canonical-name shards after ordered name resolution,
retaining `(record sequence, row ordinal)` per name. It must not shard by wire
connection or by unnormalized tagged spelling: different connections and sorted
or repeated-tag spellings can name the same stored metric. Normalization and
alias barriers must run before scheduling. Do not sort or coalesce across such
barriers without the existing cache/store correction oracle.

No raw-log deduplication is introduced. Duplicates, out-of-order timestamps and
corrections reach the same apply policy with their IEEE bits intact. Cache
coalescing and the existing continuation codec decide final stored slots. A
checkpoint is an applied-and-store-durable prefix, not merely a decoded or
appended sequence. Restarting before that prefix needs the current idempotence /
correction rules or a durable exact cursor to avoid late old data replacing newer
corrections.

CRC and header admission are not semantic point validation. Full decode validates
names, dictionary use, predictors and compressed payloads **after durability**.
A semantic failure emits no rows for that frame and poisons that connection until
its next independently verified snapshot/new connection epoch. Other connections
can apply, but replay returns an error; the owner must retain affected segments
and surface the failure. Rotation currently fails the whole writer if it cannot
create a valid snapshot. A production owner needs an explicit broken-connection
checkpoint representation or must close/re-establish that connection.

Quota and throttle decisions occur at apply time through the ordinary cache
sink, after canonical name resolution. Raw admission cannot know a metric's
quota. Consequently raw bytes count wire-valid offered points, not necessarily
accepted cache points. Bound disk backlog independently and stop/backpressure
before capacity is exhausted. Changing policy during a backlog can change its
admission decisions; exact old decisions require recording the relevant policy
version/epoch. The prototype exposes an apply callback; it does not implement
the daemon's quotas or policy epochs.

Direct mode needs a global cutover fence: stop new raw admission at sequence F,
sync through F, apply every accepted record through F, sync/checkpoint the store,
then publish the normal sink for later records. Each live connection must inherit
its decoder state at F (or reconstruct it from a snapshot plus durable suffix).
A pointer swap alone loses predictors and can let direct corrections overtake
old replay. Reverting to capture requires a symmetric durable fence. The tool
does not implement this daemon transition.

## Coexistence, retention and migration

Plaintext, pickle, HTTP and Kafka retain the current point encoder and journal.
A future mixed-format manifest must put decoded and raw inputs into a common
sequence/apply order wherever their names can overlap. A missing raw feature must
fail explicitly; an old binary must not ignore raw segments on rollback.

Keep readers for both formats throughout migration. Start with a disabled writer
flag, add real receiver/recovery integration and mixed-format crash tests, then
qualify on the actual historical fixture with live input and the ordinary
cache/persister. Only a separately scheduled canary should enable raw writes.
Disable new raw writes for rollback, drain/checkpoint retained raw segments with
a compatible reader, then permit an older binary.

Retire a segment only when every point it protects is represented in a durable
store checkpoint or in a newer durable log, with no replay/copy reference left.
Independent snapshots remove **decoder** dependencies; they do not remove
unpersisted-point dependencies. Idle connections and stuck metrics cannot pin all
history forever: reuse the current age/relog machinery for those accepted points,
or keep an explicit expiry policy and loss accounting. Never expire a name whose
newer raw backlog has not yet been applied; original arrival times and record
watermarks must participate in expiry. The prototype never deletes segments.
