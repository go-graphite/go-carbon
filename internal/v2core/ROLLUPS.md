# Offline canonical rollups

`Config.Rollups` makes core the sole archive/frontier/activity owner. Each
producer submits explicit `Batch` applications using `AppendBatches` or its
bounded pipelined counterpart. `v2daemon` layer 3 is the first consumer. Names
must have one serial producer. Policy bytes come from `EncodeRollupPolicy` and
bind an exact canonical name, placement group, retention ladder, method and XFF.

The pending journal packet retains every original group, point sequence, order,
clock and policy. Definitions sync before the point payload and
its checksummed application envelope. Core's existing owner cursor discards
already durable packets before planning; a duplicate order inside a new packet
fails. Writable daemon restart/replay remains a separate integration task.

A serial core owner prepares each policy once, reuses a reducer Workspace and
record arena, and supplies immutable sparse Slots. Planning groups by metric
without merging applications. All canonical replacement records detach before
scratch reuse. Commit installs slots/frontiers/activity only after the existing
atomic raw/part-descriptor/frontier SST ingest succeeds. Archive originals are
closed, synced and retained before that publication; their rows never enter
the LSM. Planning or ingest failure
publishes none of that state. Reopen recovers from one immutable snapshot.

Archive records use `partsformat` GCP3 keys, with exact placement group above
CRC32C(name)%4096, time partition, an immutable part UUID, exact name, archive,
physical ring slot in BlockStart, and ordinal 0. Each dense archive part spans the routed vshards of one placement group and
one partition. Keys remain sorted in the published GCP3 order. Archive max-u32 is reserved for version-1
policy/frontier/activity metadata in this offline experiment. Other values use
`sealrollup.EncodeRecord`. Greatest original replacement Order wins for a
physical slot before time filtering, including occupied NaNs. No partial sums
or averages are merged. The reader validates metadata against the policy digest.

Partition 0 retains legacy ring data and independently published metadata.
New records use UTC daily expiry partitions: sample time + archive retention +
the next coarse step (when present), rounded down to a day. The extra step keeps
source samples through the last coarse window that may need a late correction.
Each data part also carries its metadata envelope for unchanged portable-part
validation. Readers, recovery and moves resolve physical ring winners across
all partitions before filtering times; resolved coarse values are never reduced
again by merging. Existing stores keep their partition-zero files; ordinary
merges prune their expired records without a wholesale conversion.

`CORE_MERGE_MAX_BYTES` is a positive runtime byte count, default 1073741824
(1GiB). Automatic merges require sixteen same-source local parts in the same
group, partition and size tier whose total compressed bytes fit the cap. A part
above it is never selected. The output writer enforces the same limit including
SST indexes/footer and aborts an oversized proposal without publishing it.
Thus new staged SST space is at most one cap per active merge worker; old inputs
remain protected by existing snapshot/grace fences. This does not bound disk
held by readers or exported hard links, and the fixed cap is not a free-space
reservation. A smaller cap trades lower rewrite work for more terminal parts.

Merges prune records only after their conservative expiry is before the metric's
durable application clock. Background GC retires positive expiry partitions
once their complete day precedes the owner's durably committed `ArchiveClock`.
Wall time cannot authorize this during historical log replay. An idle owner
retains data until another application advances its clock. Partition zero is
never age-dropped, preserving reset/frontier/discovery authority. Retirement
publishes binding removals atomically and unlinks after the existing reader
fence and grace period. Imported files keep move-catalog authority and are not
rewritten or age-retired by the receiver's local-original policy.

Older binaries reject nonzero rollup partitions: upgrade readers and writers
together; a rollback needs a compatible reader. The rollup16 closed-window
planner can continue supplying the same canonical records; no aggregation is
introduced here.
Numeric raw keys and the input journal remain the existing core experiment.
Whole-engine portable copy, catalogue/frontier movement, tombstones, retained
SST lifetime and online readers are not qualified by these archive keys.

Revision starts at creation (1) and advances once per original application that
admits a point into a retained archive, including repeated values/timestamps.
LastUpdate follows the original admission clock, clamped against backward clocks;
it never follows maximum sample time. The current admission clock has seconds
precision and is persisted as nanoseconds. A higher-resolution source must carry
its original activity clock before it can claim nanosecond metadata equivalence.

The differential matrix uses actual classic UpdateMany calls and physical-slot
bytes, classic render windows, and the existing engine's controlled activity and
revision metadata. The finite fixture additionally checks every reopened raw
row, catalog entry, journal application, archive slot and metadata record.
Throughput includes final Drain and Close; correctness read-back is outside time.

Canonical rollups use persisted `RollupRoutingVersion: 1`: physical owner is
`CRC32C(canonical name) % 4096 % owners`. All names in a portable virtual shard
therefore share one physical owner. Numeric raw-only layers retain their original
ID routing. The marker is part of the durable core configuration; old rollup roots
without this marker fail configuration validation rather than silently changing
ownership. Read consumers must use name routing for canonical layers. Placement
group remains above virtual shard in every GCP3 key; no archive file crosses a placement-group or partition boundary.

The producer caches validated immutable identity routes without a shared lock.
Each seal allocates sample and original-application descriptor arenas together;
it preserves every original batch boundary and order. Per-metric key templates
cache validated immutable identity bytes; output keys remain detached, with only
part UUID, archive and physical slot filled during the seal. Archive, metadata,
raw rows and owner frontier publish through descriptors in one atomic ingest.

`OpenReadOnly` opens Pebble in read-only mode and refuses core admission. It
publishes persisted snapshots without reconstructing writable reducer state.
The finite fixture uses it for complete raw and journal verification after the
original timed Drain/Close; writable reopen/reducer recovery remains covered by
the existing restart differential tests. The small fixtures keep their complete
archive/metadata oracle.


## Authoritative archive files

A pinned Pebble snapshot binds immutable `parts/<SHA256>.sst` originals through
its descriptor namespace. `ScanRollups`, `ScanCatalogNames` and `RollupSnapshot`
read that exact inventory using the standard external SST iterator. Recovery
resolves the greatest original order/revision and reset frontier from the same
files before replay. The legacy in-LSM GCP3 namespace remains readable, with
identical duplicate keys deduplicated and conflicting duplicate values rejected.
No new archive output is ingested or compacted by Pebble; only raw rows, small
part descriptors and the owner frontier participate in its atomic manifest edit.
Overlapping virtual-shard ranges in packed archive files therefore create no LSM
archive compaction work. Placement-group copies still transfer original files
and descriptor sidecars by complete-file checksum.

The snapshot includes a separate local owner-directory locator. Writable Open
rebinds this locator before recovery after a complete root move. A relocated
`OpenReadOnly` fails closed until that writable rebind; callers opening naked
copied snapshots can pass `WithRollupDirectory(ctx, localOwnerDirectory)` to the
unchanged `NewRollupSnapshot` API. Locators never enter portable descriptors or
part identities. Readers own external file handles until their iterator closes;
committed originals are retained for pinned snapshots, and unpublished originals
are removed only during exclusive writable startup. Missing committed originals
are errors, even when older LSM archive versions remain available.

### Summarized orders in native single-archive merges

Dense extension bit `1<<1` replaces a run's row-order column with one unsigned
maximum. Slot, timestamp, value and correction encoding stay unchanged; aligned
timestamps are allowed, kinds and exact-order flags are not. Decoding fills the
existing compatibility Order/Sequence slots with the representative maximum.
It is a source/envelope bound, never a reconstructed history clock.

The owner selects this mode for a complete output name only in a same-source,
sequence-adjacent prefix merge whose members all have nonzero descriptor Seq,
whose policy has exactly one archive, and which has no copied dependencies or
active legacy order reset (metadata v4, or ResetOrder=0). All resolved output must
be Raw with Upto=0 and unique physical slots across every run. Every other name
retains exact orders, including fine rows of multi-archive history. Native fresh
seals apply the same policy/reset and complete-name record certificate at the
planned-part handoff, after the owner assigns Seq. The reference and tuple
writers select the same encoding; copied/repack and generic converter paths
continue writing exact orders. No source clock is inferred from the maximum.

Physical replacement uses timestamp, live/copied class, source-local part Seq
and the established in-part winner. Multi-archive Whisper propagation instead
needs exact admission/pass clocks and Upto bounds; a run maximum cannot supply
that history. Readers and portable import reject summaries under history policies
or active legacy order resets. Old exact runs remain readable, including mixtures
with summarized parts. Lossless conversion expands the representative and preserves
that logical input and its receipt digest; only the owner merge and native seal handoff certify elision.

Deploy capable readers, converters and move recipients before enabling these
writes: older readers reject the new extension. After a summarized part exists,
rollback requires a capable reader with summary writing disabled; the original
row clocks cannot be recovered for an arbitrary older reader.
