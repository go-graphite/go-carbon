# Immutable partition copy experiment

This offline prototype adds a copy boundary to the portable format2 contract.
It is not wired into the daemon, storectl, ownership changes, or READY. It builds
on the immutable-parts prototype and keeps that PR Draft.

The copy source owns a pinned inventory of immutable files, partitioned by
`(vshard, time partition)`. Each file contains exactly one such range. Its keys
start with that range, then a globally unique part identity, then the exact
canonical metric name, archive, block start and ordinal. IDs therefore require
no remapping between replicas. The part identity before the metric makes seal
SST ranges disjoint for atomic multi-file ingestion. Independent names never
alias through a truncated hash.

`copyCompare` exchanges fixed-size partition digests over TCP. Its control
traffic is 42 bytes per healthy target partition plus fixed framing and four
bytes per mismatched source partition. The digest covers the physical immutable
part inventory and policy, rather than logical metric equality; different
sealing or compaction histories conservatively require a copy. Comparing all
partition summaries is O(partitions), while payload I/O is O(mismatched bytes).
The pinned source descriptors are supplied once before a comparison round.
Target-only partitions are outside this donor's authority and are left alone.

`copyTransfer` streams whole SST bytes over one TCP connection per worker.
The sender opens only requested files. The receiver uses one bounded 1MiB buffer
per active stream, verifies SHA-256, fsyncs the staging file, checks its first and
last keys against the partition bounds, and calls Pebble `IngestAndExcise`.
No metric lookup, value decode, ID rewrite or read-modify-write occurs in the
transport. An authoritative empty partition uses a synced blind range tombstone
because Pebble treats an empty `IngestAndExcise` as a no-op. Failed, truncated,
cancelled or corrupt transfers remove their staging files before publication.

The caller must fence writes to a receiving partition and publish its manifest,
policy and durable replay frontier coherently before serving it. Replacement
must start from a pinned log cursor, preserve log retention through the copy,
then apply every record after that cursor through the normal writer before
READY. The unit test checks a late-correction snapshot catch-up, but does not
implement durable input-log cursor handoff or prove READY during live ingest.

Stock Pebble compaction can mix partition ranges. The fast path retains the
original sealed logical SSTs separately from Pebble's physical compaction files;
those files must stay pinned for active snapshots. Storage ownership and
retirement of those logical files are not implemented here. `copyExport` is the
explicit compatibility path: one bounded snapshot iteration writes a new SST.
That export cost must be reported separately from zero-rewrite transfer rates.
This does not claim existing numeric-ID databases are portable.

## Run

Normal correctness checks include full byte equality after transfer and repair,
stale target-key removal, special float payloads, late snapshot catch-up,
multiple disjoint parts, empty partitions, checksum failure, cancellation and
range-bound rejection:

```sh
go test ./tool/immutable-parts -run TestCopy
```

The opt-in copy render differential reuses the snapshot-reader differential
against classic Whisper, the native store and the continuation store. It inserts
an SST/TCP/ingest round trip before rendering all archive slots. Stage the
`internal/partsread` and `internal/sealrollup` companion PR packages first, then:

```sh
go test -tags partsread_integration ./tool/immutable-parts -run TestCopyRenderDifferential
```

The benchmark is an explicit test-binary entry point so it needs no change to
the shared CLI. Build with `go test -c ./tool/immutable-parts`, set `COPY_ROOT`
to an owned empty directory, then invoke `-test.run=^TestCopyCampaign$` with
`COPY_ROLE=prepare`, `serve`, or `receive`. Preparation generates 1000 partitions
and ingests retained hardlinks into 16 donor DBs. `COPY_BLOCKS` controls blocks
per partition (default160, 1024 points/block, uncompressed SSTs). The serving
process binds loopback and publishes its address beneath COPY_ROOT. Receiving
uses 16 target DBs, `COPY_STREAMS=1..64`, and `COPY_HOLES=0..1000` missing
partitions. Healthy partitions are materialized outside the timed repair.
Sender and receiver can run as distinct processes for CPU accounting. The
benchmark deletes each target after close; source cleanup belongs to its caller.
It is a trusted offline protocol; authentication, admission, resumable sessions,
manifest persistence and daemon lifecycle integration remain future work.
