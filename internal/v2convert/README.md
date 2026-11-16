# Offline v2 conversion boundary (experimental)

The forward scanner and per-input-SST transaction accept **materialized current
GCP3 archive/metadata records**. Source owners must resolve database visibility,
merge operands, range deletion, obsolete generations and native continuation
state before conversion. Native chunkstore SSTs remain scan-only. This command
never opens or adopts a running database.

```sh
go run ./internal/v2convert/cmd -source /owned/frozen-ssts
# Use the original source incarnation and ordering stream, not the converter's:
go run ./internal/v2convert/cmd -source /owned/gcp3-input -output /owned/output \
  -source-incarnation 09000000000000000000000000000000 -source-stream journal-0
```

`CoreRecords` accepts published singleton and shared dense values. Metadata
must accompany every metric in each original part. `CoreWriter` uses
`v2core.WriteDenseRollupPart`, `v2part.NewDenseWriter` and
`v2part.ReadExpanded`; there is no private output codec. It retains exact GCP3
logical keys, float bits, occupied slots, original replacement orders,
correction flags and policy/activity/revision metadata. It groups all vshards
of one `(placement group, partition, original part ID)` into a schema-2 SST.
Different original part IDs are never silently relabelled or combined.

The decoded input bound includes record headers and all expanded singleton
keys/values. A dense input cannot bypass it. Within that bound, records are
sorted once and each original part produces a checksum-addressed `.sst` and a
canonical `.sst.descriptor`. Exact readback checks each reconstructed key and
value against the sorted input, rejecting duplicates, missing and extra rows;
physical files need not concatenate in global key order. An ordered transaction
digest records the verified logical stream. Persistent progress and transport
verification use whole-file SHA-256, with no per-name digests.

A source checksum plus adapter/writer versions selects the destination. Closed,
synced SSTs and descriptor sidecars are checksummed into `receipt.json`, synced,
and published together by directory rename and parent sync. Resume verifies
all recorded files. Source files must remain immutable throughout the operation.
Killed runs can leave private `.building-*` directories; remove them only after
confirming that no converter is using them.

Synthetic tests compare full dense SST bytes with the existing native exact
128-slot point-block encoder under the same outer Zstd settings. The two
retention ladders and a 229,155-slot/1,720-name generated daily corpus pass the
1.2x SST byte target and exact readback. These tests do not reproduce captured
native SSTs or prove whole-database/allocated-byte density. The original source
part-ID distribution still bounds coalescing. Separate source SST transactions
are not combined. Source checksum and decoding are separate passes; exact
readback adds an output pass. Disk-speed conversion remains unqualified.

Catalog/policy/tombstone authority, migration frontiers, native source semantics,
source reclamation and live publication remain external responsibilities. A
conversion receipt is not a live adoption or complete coverage certificate.
