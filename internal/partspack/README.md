# Exact packed archive blocks

`EncodeBlock` calls the existing `chunkstore.EncodeSealedPoints` codec. Ordinary
archive blocks therefore have exactly the cold-pack timestamp/value bytes,
including its implicit timestamps, integer modes, IEEE byte planes and optional
compression. No decimal quantization is enabled. Blocks have at most 8,192 points;
the offline tool targets 4,096, matching cold packs.

`Policy` moves repeated aggregation method, x-files-factor bits and archive
geometry into one descriptor per compatible part set. Its SHA-256 can be
referenced by an immutable manifest. Archive ordinals retain their original order.
Names remain exact canonical bytes in the shared portable keys. `EncodePart`
adds a self-contained dictionary inside an explicit `GCD1` value/index; its
ordinals are local to that part and never become replica identities.

The zero-version fallback contains count, CRC32 and exact signed timestamps/IEEE
bits. It preserves empty or non-monotonic images and timestamps outside the
existing codec's domain. The offline repartitioner rejects empty archives and
duplicate timestamps because catalog/version semantics require backend ownership.
The library itself preserves their supplied order and bits.

`PartitionOptions` is experimental geometry, explicitly selected by the offline
tool. Increasing widths amortizes sparse archive and SST overhead, but delays
expiry. A width of one quarter of an archive's horizon can add almost 25% retained
history at an expiry boundary. Same-slot compression savings do not qualify that
retention policy. Never reinterpret an existing partition number or expire a
mixed-archive prefix until every contained archive has expired.

The first consumer is `tool/parts-pack`. This is an offline format experiment;
there is no daemon flag, durable publication protocol, reader adapter or automatic
retention change here. Production metadata/provenance and copy dependencies must
be agreed with the portable backend before integration.


`EncodePart` and `OpenPart` preserve every portable key field and opaque block
payload byte. One part contains one placement group, routing vshard, existing
time partition and globally unique PartID. Policy and sorted exact names travel
inside the same SHA-256 authenticated bytes as every dependent record. The name
dictionary uses reversible prefix sharing. A second dictionary factors repeated
12-byte payload prefixes (for ordinary cold blocks: version, flags, count, first
timestamp and step); numeric bodies remain byte-identical. A validated uncompressed cold-block CRC
may be omitted under the whole-part checksum and reconstructed byte-identically
at open. Compressed blocks whose CRC covers the precompression representation
and arbitrary opaque payloads retain their original CRC bytes.
Opened records borrow unchanged payloads or own reconstructed payload buffers.
Keep the input immutable for the lifetime of all records.

The encoded limit is64MiB, with at most524,288 records/names and64MiB of expanded
dictionary names; reconstructed key/payload accounting is bounded to256MiB.
These limits exclude Go object overhead and temporary encoding/sort buffers.
Malformed counts, prefixes, references, cross-scope keys, routing errors,
duplicate keys, truncation and checksum failures are rejected.

Copy/pin/retire the complete encoded part. Compaction that changes its scope must
rewrite the dictionary and every reference together. Namespace filtering must
carry the complete dictionary dependency. A SHA-256 checksum detects corruption;
it does not establish writer authority or provenance. This codec supplies no
incarnation, tombstone, frontier or catalog publication protocol.

The offline `-dictionary` consumer preserves source time partitions and emits
one SST per partition. `-dictionary-bundle` is a separate explicit experiment
that bundles logical parts into a larger SST within one placement group/vshard.
It preserves each logical part's signed partition identity and adds no retained
age. Such an SST spans time partitions: the current `ValidateFileRange` contract
rejects it for whole-partition SST transfer, so portable manifest/copy/retention
integration must change explicitly before production use. Logical range deletion
requires compaction to reclaim physical bytes; the experiment does not qualify
steady retained size, compaction scratch or peak disk use.
