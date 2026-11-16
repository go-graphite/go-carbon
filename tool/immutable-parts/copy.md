# Immutable partition copy experiment

This offline prototype adds a copy boundary to the portable format3 contract.
It is not wired into the daemon, storectl, ownership changes, or READY. It builds
on the immutable-parts prototype and keeps that PR Draft.

The copy source owns a pinned inventory of immutable files, partitioned by
`(placement group, vshard, time partition)`. Each file contains exactly one such range. Its keys
start with the exact placement group and that range, then a globally unique part identity, then the exact
canonical metric name, archive, block start and ordinal. IDs therefore require
no remapping between replicas. The part identity before the metric makes seal
SST ranges disjoint for atomic multi-file ingestion. Independent names never
alias through a truncated hash.

`copyCompare` exchanges partition digests over TCP. Entries contain the exact
length-prefixed portable partition prefix and a SHA-256 digest. Control traffic
is 54 bytes plus the placement-label length per healthy target partition,
20 bytes of framing, and four bytes per mismatched source partition. The digest covers the physical immutable
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

## Logical summaries and additive block repair

`copySealBlocks` persists a versioned `copyBlockManifest` alongside the copy
partition. Its canonical SHA-256 covers the partition, policy and sorted opaque
key/value record multiset. It removes only physical PartID from record keys;
archive, block start, ordinal, value length and every value byte remain covered.
Different SST counts and PartIDs compare equal when their encoded records agree.
PartID must carry identity only: precedence/incarnation/tier must be in the
stable record envelope before production use. Different encoded records that
happen to render equally conservatively mismatch.

`copyPlanBlocks` compares persisted block key sets when summaries differ.
`copyRepairPartition` copies only donor blocks absent on the target, verifies
their physical hashes and builds an SST containing the original physical data
blocks byte for byte, including compression and checksum trailers. Only index,
properties and footer are regenerated. `Additive` partitions use ordinary Pebble
ingest, preserving target-only keys. The old authoritative replacement API
remains separate and requires fencing.

Shared canonical keys with different bytes are counted as conflicts and withheld.
A nonzero count blocks READY; these blocks need the normal precedence-aware
merge using a stable source/incarnation/order envelope. No other value needs
decoding. Wire bytes equal missing physical blocks plus new SST metadata;
sample decode count is zero. Physical SHA-256 still protects transfer integrity
independently of the logical digest.

The raw-block assembler accepts trusted, seal-generated Pebblev4 manifests,
CRC32c trailers, sequence-zero SETs, inline values and one encoded v2 value per
physical data block. Seal inspection rejects multi-record blocks, value
indirection, merges and range mutations; it never silently takes the slow
export path. Data blocks may be compressed. Manifest authentication, persistent
publication and core-writer integration remain prerequisites. The offline copy
inventory is supplied out of band; the existing TCP summary operation remains
the earlier physical-inventory protocol.

Conversion should emit an SST per placement group/vshard spanning many time
partitions, with a partition-to-table/key-range index. Whole ownership ranges
can then ship whole SSTs. Partition hole repair additionally needs the seal-time
block offset/key-set/hash index described here. The prototype seal inspector
currently expects partition-bounded input; a multi-partition writer must publish
partition manifests during its single seal pass. Compaction must retain the
logical files and their block manifests independently of physical Pebble SSTs.

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
benchmark deletes each target after close; source cleanup belongs to its caller. `COPY_ONLY_GROUP`
selects a namespace from the returned plan; preparation uses four placement groups.
It is a trusted offline protocol; authentication, admission, resumable sessions,
manifest persistence and daemon lifecycle integration remain future work.

For a cross-host run, `COPY_BIND` selects the sender interface and port and
`COPY_ADDRESS` overrides the receiver's source address. `COPY_PARTITIONS` sets
fixture size, `COPY_RETAIN_ONLY=1 COPY_NO_EXPORT=1` prepares retained SSTs without
donor DB/export work, and `COPY_SECONDS=120` repeats complete transfer rounds
until its deadline. Counters report completed files, excluding any incomplete
last file. On Linux, `COPY_COLD=1` advises DONTNEED on each requested owned file
before reading; verify physical reads with sender `/proc/PID/io`. This evicts
only fixture pages, and is distinct from a fixture larger than the page cache.

Namespace moves use the same transport: dual delivery during an externally
coordinated route switch, one placement-group range transfer, partition digest
repair for the overlap window, parity at a durable common frontier, then an
explicit source group range deletion. The private namespace test verifies this
data sequence, including preserving another group; it does not change routes.
Catalog entries, name-incarnation tombstones and replay frontiers must be pinned
with the data and published coherently. Copying data alone must never resurrect
a deleted name. This format does not yet carry the required incarnation/tier
contract, so READY and operational namespace deletion remain blocked.
