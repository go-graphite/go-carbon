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

## Continuing a phase-1 campaign

`--engine bulk --combine-existing --bulk-only` revisits acknowledged native
deferrals in the same journal. Phase-1 receipt identities remain byte-compatible.
Combine requests use a separate identity domain; a deferred attempt also binds
the preceding receipt and an immutable selected-ordinal bitmap. The first attempt
links to the phase-1 receipt. This chain permits a fresh attempt when a cache-only
target becomes committed, while lost acknowledgements reuse the same intent. The receiver
exports only selected names, and advertises `NativeDeferredCombine` so older
receivers fail the controller's capability check.

The controller fsyncs the selection before sending the pull. A lost response
reuses that selection and checks the durable receipt before opening a donor pin.
Acknowledgement merges copied bits into the parent's completion bitmap and
retains the original receipt and every combine receipt, including after completion.
No overlapping journal ranges are created. Already completed ordinals are neither
exported nor republished. Selected names that remain absent stay unfinished.

Use `--deferred-only` to exclude unissued plan gaps. Unacknowledged phase-1 intents
must be resolved first. `--deferred-range STREAM:START:END` restricts work to one
acknowledged journal range; it also excludes gaps. Both scope flags require
`--combine-existing --bulk-only`. `--max-batches` counts attempted parent ranges,
including recovery, and remains a canary scope bound rather than a rate governor.

For example, from the existing campaign directory, using its original plan and
journal (the stream/ordinals below must come from that journal):

```sh
store-copy run --plan refresh1-plan --state copy-refresh1-state.jsonl \
  --engine bulk --combine-existing --bulk-only --deferred-only \
  --deferred-range STREAM:START:END --max-batches 1 --apply
```

Each combine receipt covers the original ordinal count: its unset bits include
previously completed phase-1 names. Remaining work is the parent's merged `Done`
bitmap, not the new receipt's `DeferredCount`. Canary readiness additionally
requires the final integrated build, real native-pair oracle and owner rollout.
