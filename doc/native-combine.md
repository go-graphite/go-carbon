# Native history combine

The opt-in native combine engine merges names already present on a survivor.
It keeps coarse archives independently authoritative and gives the survivor
precedence at equal timestamps, matching the reviewed transfer importer.

## Encoded history

`ContinuationCodec.CombineNative` validates both immutable inputs and selects
newest circular-slot owners independently in every archive. Dense sealed scalar
runs and byte planes, and dense exact classic groups, can prove timestamps and
finite values without expanding point arrays. Other supported groups use bounded
decoder scratch to prove eligibility. Unsupported layouts, sidecars, off-grid
values and unprovable normalization use the authoritative archive union.

The result uses classic addressing with immutable references to original encoded
payloads. Each `a` group maps original payload ordinals to classic 128-slot
positions; `r` holds the original bytes and `R` counts distinct live mappings.
Compatible ordinary point groups remain byte-identical `p` groups. Anchor and
virtual-rollup boundary groups are materialized. This avoids compressed rollup
buffers reinterpreting independently authoritative coarse values after a later
historical correction.

A foreground full-group replacement retires its references in the same batch.
An empty replacement remains authoritative. Repeated buffered writes retire a
mapping once, and the last mapping removes its shared payload. Already referenced
histories can serve as input for another donor.

## Publication and reader compatibility

Preparation uses one immutable database snapshot and CPU-sized workers with
separate readers and arenas. Each worker retains at most one completed result
until the ordered SST writer consumes it. An error cancels and joins all workers
before the snapshot closes. SST key order and reserved-ID order remain unchanged.

Publication takes the existing name stripe, rechecks ID/generation/revision and
policy, and atomically writes the catalog and per-name piece. A changed name
retries. Decoding, SST construction/ingestion and WAL sync stay outside that
stripe. Durable original donor input, source-free recovery, partial publication,
range receipts and lost-ack handling retain the existing protocol.

A referenced publication durably raises `CHUNKSTORE` to
`go-carbon-chunks-v12` before ingestion. The codec explicitly advertises the new
reader capability. Older readers reject before opening Pebble. All serving,
recovery and transfer-source binaries must support v12 afterward; cold-pack and
float-codec downgrade operations refuse to lower this fence. Rollback must retain
the reference reader. No vendor format or go-whisper writer was modified.

`CombineCopiedBlocks` counts retained encoded payloads, including payloads that
required decoding for eligibility. `CombineMaterializedArchives` counts the
whole-archive fallback; zero does not mean zero decoded input groups.

## Qualification

`store-copy bulk-eval` imports the actual captured survivor, stops private
background maintenance before seeding, and checks original encoded donor layouts
before timing. Logical survivor images preserve history semantics but lose their
native layout and therefore cannot qualify native survivor read cost.

For a native survivor, capture metadata can contain `TargetNative` with a second
checksummed transfer stream and the same names. The donor file then begins with
`BKPAIR01`, followed by its ordinary transfer stream. This proof-only envelope
makes older evaluators fail closed instead of ignoring the survivor. Both private
survivor stores verify their original layouts before copying. Source/target
policy mismatch and modified capture hashes fail before measurement.

Snapshot equality, the independent Fetch oracle, future writes, revision races,
crash recovery and controlled Linux throughput are separate checks. Rates must
include input/output bytes, CPU, disk/NIC, peak memory and publication locks.
There are no new runtime rate, cache, debt, name or p99 admission governors.

The engine alone does not revisit phase1 deferrals in an existing campaign:
`nativeStreamCovered` treats an acknowledged phase1 range as covered. An integrated
controller needs a separately tested deferred-ordinal path that preserves the
old receipt and completed ordinals. Do not treat `--combine-existing` alone as
that integration or as live canary readiness.
