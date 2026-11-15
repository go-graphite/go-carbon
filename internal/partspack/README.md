# Exact packed archive blocks

`EncodeBlock` calls the existing `chunkstore.EncodeSealedPoints` codec. Ordinary
archive blocks therefore have exactly the cold-pack timestamp/value bytes,
including its implicit timestamps, integer modes, IEEE byte planes and optional
compression. No decimal quantization is enabled. Blocks have at most 8,192 points;
the offline tool targets 4,096, matching cold packs.

`Policy` moves repeated aggregation method, x-files-factor bits and archive
geometry into one descriptor per compatible part set. Its SHA-256 can be
referenced by an immutable manifest. Archive ordinals retain their original order.
Names remain exact canonical bytes in the existing portable keys; this change
introduces no name dictionary or replica-local identity.

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
