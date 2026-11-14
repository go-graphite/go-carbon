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
