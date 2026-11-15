# Ordered parallel input-log replay

A log segment contains 16 independent dictionary/delta streams. Each metric
belongs to one partition. Segment sequence, frame offset and callback record
number define the order within that partition; unrelated partitions may replay
concurrently.

```text
segment/frame inventory
  ├─ partition reader → verified decoder → admission + fused cache insert → cursor
  ├─ partition reader → verified decoder → admission + fused cache insert → cursor
  └─ ...
                         join all readers → publish checkpoint cursor map
```

Each reader owns its frame list, positioned I/O buffer and decoder. The worker
count is bounded by the partition count and GOMAXPROCS. When there are fewer
workers than partitions, each partition still has exactly one owner. Admission
blocking in one worker cannot block independent workers behind a central payload
producer. Decoder buffers are bounded by the largest frame each worker sees.

A cursor advances only after a complete metric group has been accepted. Its
segment salt identifies the segment incarnation, so recycling a segment cannot
make an old cursor skip new points. Dictionary prefixes are decoded even when
their protected points are skipped. A damaged stream remains blocked for the
rest of its segment; the next segment starts a new dictionary.

The application resolves and prices each metric group once, reserves recovery
capacity, and passes the resolved identity to the normal cache recovery insert.
The insert retains recovery-source ownership and does not re-log recovered
points. Tag normalization remains in the cache insert.

Replay cursors have one writer per partition. Cancellation joins every reader
before the application copies cursor slots into the checkpoint's map. Publishing
a cursor still requires the existing durable cache snapshot/store transaction;
reader progress alone is not persistence authority. Segment retirement continues
to use the existing persistence low-water marks and retention pins.

This change affects journal replay. Startup capture still owns a separate spool.
Unifying that intake with the journal also requires an upgrade boundary that
orders the original journal, retained capture epochs and newly appended journal
records across a crash. That transition is separate from the reader ownership
change described here.

## Validation and measurement

`TestReplayReadersProgressIndependently` blocks one partition and requires another
to finish more frames than the former producer queue could hold.
`TestJournalReplayStoredBitDifferential` compares ordinary and fused cache inserts,
then persists, closes and reopens both stores. It covers special float bit patterns,
same-timestamp corrections, aliases and multiple appends. Existing replay cursor,
corruption, dictionary-prefix and startup archive crash tests retain their roles.

On Unix, `BenchmarkJournalReplayFixture` accepts a private capture-frame path in
`CAPTURE_BATCH_FIXTURE`. It creates a journal using the ordinary writer, distributes
names across the actual cache/log partitions, and replays into a cache with 200,000
resident names. It reports process CPU seconds per million points and wall time per
point, and checks every point's order and bits outside the measured interval.
Repeated points make this a compressed fixture workload, not a production
throughput prediction. `JOURNAL_REPLAY_STACKS` optionally writes a goroutine snapshot;
use that in a separate diagnostic run because sampling perturbs timing.

## Shared startup intake log

`[input-log] startup-append = true` enables journal intake before cache and storage
construction. The default remains false. This mode requires native TCP store
input handoff. The successor takes input sockets before acquiring the journal
lock, so the predecessor can finish its durable handover.

Connection buffers submit bounded native batches. Names are validated and, when
tags are enabled, normalized before grouping and routing to the same sixteen
partitions as cache collection: `(xxh3(name) & 1023) / 64`. Append acknowledgments
follow data sync. The existing partition encoders, compression workers and
positioned writes are reused. At most 32 startup submissions decode concurrently;
each input-log request is additionally limited to the frame budget.

Each partition has one independent file reader, decoder and committed frontier.
Readers follow growing, synced journal prefixes without holding the writer across
cache admission. Already durable points enter cache with independent recovery
source references, so they are never relogged at a new cache generation. Those
references follow queued items, replacements, retries and in-flight identities.
A conservative original-source pin retains the complete startup suffix until
intake switches, every reader joins, every source confirms and the store syncs.

```text
TCP → append + sync → committed partition frontier → reader → cache → store
                            ↓                                  ↓
                      retained source ← confirmation + store sync
```

The receiver's exclusive submission fence compares every reader with the
committed tail. A mismatch retries while ordinary replay proceeds. A match freezes
startup replay and enables ordinary cache/direct journal collection. Connection
buffers that were not yet submitted take the direct sink exactly once. The seal
does not wait for admission and creates no capture file or separate direct marker.

The existing disk budget and five percent filesystem reserve protect startup
appends on the journal filesystem. A reserve pause backpressures intake while
replay and persistence continue. Input-log write/error counters cover the append
path; startup intake/replay observations remain available.

Shutdown during replay joins readers before committing the existing cache
snapshot/cursor transaction. Salted frame-end cursors skip only complete frames
covered by that snapshot. Appends after its publication remain journalled for the
successor. After the direct switch the ordinary durable dump path is authoritative.

### Legacy startup spools

This mode deliberately does not migrate legacy capture epochs. On first startup,
it scans their point timestamps, records the capture start and a conservative
backfill window in `startup-discard.<epoch>/window.json`, syncs that record and both
directories, and renames the capture files and associated startup snapshots/cursor
into that directory on the same filesystem. An interrupted move resumes before
intake opens. Files are never deleted. An INFO event identifies the window, and
`input_log_legacy_spools_moved_total` counts moved groups. Operators backfill from
replicas where retention requires it, then remove the retained directory.

The ordinary checkpoint and historical input log remain authoritative. The new
journal writer starts a fresh segment/dictionary tail. There is no legacy ordering
plan or upgrade-cut transaction. Turning the switch off restores the default
capture behavior; retained shared-mode snapshots require completion with the new
mode before rollback.

### Validation

The shared path has a fifteen-stage process-exit differential comparing stored
archive timestamps and exact float bits against uninterrupted input. Stages cover
append, sync, replay/attachment, snapshot and cursor publication, direct fencing,
confirmation, store sync and source-retention release. Separate tests exercise
alias spellings, special floats, no relog, early durable ownership, legacy-window
moves and a complete application dump/restart.
