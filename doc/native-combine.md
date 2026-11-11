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

Preparation uses one immutable database snapshot and, by default, at most two workers with
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
Receiver memory limits below apply independently of controller rate, batch and worker flags.

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

## Receiver memory limits and cancellation

Each store admits one native import and one native export at a time, with
separate slots so reciprocal copies cannot deadlock. HTTP pulls acquire admission
before decoding metadata or opening a donor response. New requests above 4096 names or 1 MiB of name bytes
fail with HTTP 413; range metadata is limited to 16 MiB. Per-history encoded bytes
(including key-vector overhead) are limited to 128 MiB. Archive capacity and
physical history are conservatively limited to 33,554,432 points per input and
result, before archive decoding. A descriptor/group scan measures source sizes
without retaining history. Names above these limits become explicit skip records
in the checksummed v2 stream; the remaining names still copy. Such streams require
an updated receiver (old receivers reject the new skip flag).

`Skipped` in the durable receipt reports each uncopied ordinal/name, encoded bytes,
physical point bound, archive slots, reason and estimated worker bytes when the
combine budget is binding. These ordinals remain deferred, never completed.
`store-copy` prints a `native_skipped` event per name after journaling the receipt,
including on acknowledgement recovery. A retry keeps the original receipt;
retrying a size skip with different receiver limits needs a new range identity.

Preparation defaults to a conservative 1 GiB per worker from a 2 GiB scratch budget,
and publishes at most 32 names before preparing the next batch. Before combining,
admission charges input arenas, vector growth, encoded output and codec scratch.
Dense native groups retain implicit timestamps; irregular groups and the snapshot
fallback are charged for point materialization before allocation. A name whose
estimate exceeds its worker's memory share is skipped durably, leaving its existing catalog intact.
All other range, SST, metadata and cancellation bounds remain unchanged. This is a static
allocation estimate, not a process RSS limit: foreground writes, Pebble caches,
native allocations and old range-tombstone fragmentation require separate
headroom. Increasing server CPU count alone cannot exceed the configured worker cap.

The receiver can increase preparation parallelism with these restart-only TOML
settings (shown with an explicit 8 GiB total budget):

```toml
[whisper]
store-native-combine-workers = 8
store-native-combine-memory = 8589934592
```

Defaults remain 2 workers and 2147483648 bytes. Workers must be 1..32 and
memory/workers must be at least 1 GiB, even if `max-cpu` or a short batch uses
fewer workers. Increases above 2 GiB must fit within 25% of the smaller of host
RAM and the Linux cgroup limit; inability to detect RAM rejects an increase.
The existing 2 GiB default remains valid on smaller hosts. One import per store
keeps the aggregate worker scratch inside that single budget. Export, staging,
Pebble and foreground memory are additional. SIGHUP rejects budget changes;
reopening the store avoids overlapping old and new worker allowances. A durable
size skip still requires a new range identity after increasing the allowance.

SST builders rotate after 8 MiB of aggregate input or eight active namespaces.
An individual key/value can overshoot the byte threshold by its admitted size.
Closed SST receipts and cumulative cleanup spans each have a 16 MiB metadata
budget; exceeding either rejects the range before further staging or ingestion.
The bucket-reader cache holds at most eight cursors plus one reusable overflow
cursor per history reader. Old histories are retired with point tombstones in
1 MiB WAL batches. Durable intents and pieces survive partial retirement so
recovery repeats cleanup safely without a donor connection.

The complete original stream is still checksum-verified before publication;
only bounded writer state, per-name data and range metadata stay in memory.
Dump-stop cancels native transfers before flushing or draining persistence; service
stop cancels before waiting for HTTP handlers. Cancellation stops admission,
range-lock waits, decoding, publication and work
between ingest groups of at most eight bounded SSTs. Pebble's synchronous ingest
has no context API and can still block the shared commit pipeline until the
current group returns. No background import is detached on client cancellation.

Existing receipt identities are unchanged. Preserve old journals and resolve
receipts before planning new ranges. An unfinished legacy range above these
limits fails closed; do not shrink or rewrite its identity in place. Reconcile
its already published pieces before authorizing a separate immutable plan.
Names exceeding the per-history limits need a future streaming combine path.

Existing checksum-ready combine intents can resume under their original identity
without the new name-count limit, provided their complete encoded intent fits
the 16 MiB metadata budget. This limit is checked before JSON decoding; history
limits and bounded preparation/retirement still apply. A large legacy intent
that exceeds this budget fails explicitly. The existing monolithic intent
format cannot resume arbitrary-size metadata within that budget; doing so needs
a separately reviewed paged intent format. Unready legacy requests still use
new-request admission. Completed receipts remain readable.
