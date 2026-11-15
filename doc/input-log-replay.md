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

## Proposed shared startup intake log

The following is a follow-up design, not behavior implemented by this PR.

The input log should open before receivers are offered, independently of cache
construction. A committed append is the acknowledgment boundary. Cache
collection must not append a second copy of a record that already has a durable
log position. Normal metric preparation must be shared with this earlier path:
aliases need the same canonical partition and correction order before encoding,
with the existing validation and admission semantics preserved.

Each partition has a durable committed tail, an applied frontier, and recovery
coverage established by persistence or a committed cache snapshot. Applying a
record advances only the applied frontier. Unread records pin their segments;
applied but unpersisted records retain their original segment/generation through
cache source ownership. Confirmations release those references. A minimum
protected frontier with no uncovered holes controls retirement, rather than
reader progress or the current cache generation. Dictionary prefixes remain
available for every retained suffix.

```text
accepted input → append + sync → committed tail
                                    ↓
                         ordered partition reader
                                    ↓
                             applied frontier
                                    ↓
                         cache → confirmed persistence
                                    ↓
                           recovery-pin release
```

Replay must continue beyond the startup inventory to the committed live tail.
When its remaining suffix fits a bounded admission reservation, a partition can
fence new input assignments, finish that suffix, and switch its owner to direct
application under the same partition fence. Incoming writers then append and
sync before direct application. The fence must cover position assignment and
the mode transition; a global log mutex must never be held while waiting for
cache admission. If a bounded suffix cannot be admitted, ordinary replay
continues. Other partitions keep progressing. Source references and no-relog
insertion are required on both sides of this transition.

An upgrade needs a durable cut before any new intake acknowledgment. Existing
journal history and retained capture epochs already have a correction order,
including direct-switch fences. Roll to a fresh journal segment/dictionary and
commit an upgrade manifest that records the old journal cut and read-only
legacy source order/cursors. A crash must recover this order:

```text
existing checkpoint + ordered legacy history → journal suffix after upgrade cut
```

The manifest must survive segment retirement and partial replay, and its update
must be committed with the cache snapshot/cursors that protect skipped records.
Retained legacy sources replay once and retire after persistence. The upgrade
path must never open another legacy capture writer. Segment identity includes
salt as well as sequence because retired sequence numbers can be reused.

The journal writer owns one disk-pressure decision, preserving the current 5%
reserve pause semantics and still-meaningful metrics. Shutdown first fences
partition intake/direct application, joins readers, and commits the complete
cache-snapshot/cursor transaction. Only then can old source pins be released.

This design still requires implementation and crash proofs for the upgrade cut,
partial partition switches, append/sync acknowledgment, original-generation
retention, and shutdown. The existing 15-stage archive matrix should exercise
those boundaries with both legacy capture formats, aliases, special float bits
and same-timestamp corrections before the capture writer/catalog code is removed.

## Prepared append ownership (Draft)

The application now acquires the journal directory after the predecessor commits
its socket transfer, before constructing cache/storage. Cache attachment reuses
that owner. It still starts journal collection after historical replay and still
uses capture spool writers for startup intake.

The follow-up contains cache-independent primitives awaiting application wiring:

- `StartRecovery` preserves Open's immutable inventory while the writer runs.
  A dedicated pin protects unread frames. `CompleteRecovery` joins through the
  writer and transfers retention to a conservative current-generation fence;
  it restores the original tick even if earlier empty cycles removed it.
- `StartAppend` and `Append` commit bounded native batches through the existing
  partition encoders, compression workers, positioned writes and syncs. A receipt
  carries the original append generation and salted committed tail. `AttachSource`
  establishes cache collection without replacing the writer or history inventory.
- `CommitUpgradeCut` commits a fresh segment/dictionary boundary and an immutable
  ordered legacy-source plan with salted journal cursors. Segment sync, temporary
  manifest sync, rename and directory sync precede success. Reopening uses the
  committed plan, and retirement cannot reuse its sequence number.

The append pin deliberately has no release operation yet. Original-generation
source tracking, bounded live-tail readers, canonical routing, the shared disk
reserve and the shutdown snapshot fence must be connected before activation.
The application does not call these new append/cut primitives. This Draft must
remain unmerged until their first real intake usage and the extended archive
crash differential are included. The cut's five-stage process-exit test proves
its file transaction; it does not prove the application's legacy ordering or the
new startup path. The existing 15-stage matrix continues to cover capture spools.
