# Offline canonical rollups

`Config.Rollups` makes core the sole archive/frontier/activity owner. Each
producer submits explicit `Batch` applications using `AppendBatches` or its
bounded pipelined counterpart. `v2daemon` layer 3 is the first consumer. Names
must have one serial producer. Policy bytes come from `EncodeRollupPolicy` and
bind an exact canonical name, placement group, retention ladder, method and XFF.

The pending journal packet retains every original group, point sequence, order,
clock and policy. Definitions sync before the unchanged PDC4 point payload and
its checksummed application envelope. Core's existing owner cursor discards
already durable packets before planning; a duplicate order inside a new packet
fails. Writable daemon restart/replay remains a separate integration task.

A serial core owner prepares each policy once, reuses a reducer Workspace and
record arena, and supplies immutable sparse Slots. Planning groups by metric
without merging applications. All canonical replacement records detach before
scratch reuse. Commit installs slots/frontiers/activity only after the existing
atomic raw/archive/frontier SST ingest succeeds. Planning or ingest failure
publishes none of that state. Reopen recovers from one immutable snapshot.

Archive records use `partsformat` GCP3 keys, with exact placement group above
CRC32C(name)%4096, partition 0, a random immutable part UUID, exact name, archive,
physical ring slot in BlockStart, and ordinal 0. Each extension SST stays inside
one (group,vshard,partition) range. Archive max-u32 is reserved for version-1
policy/frontier/activity metadata in this offline experiment. Other values use
`sealrollup.EncodeRecord`. Greatest original replacement Order wins for a
physical slot before time filtering, including occupied NaNs. No partial sums
or averages are merged. The reader validates metadata against the policy digest.

Partition 0 intentionally identifies the complete physical ring, not an event
time bucket. This change does not authorize deleting ring data by time partition.
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
group remains above virtual shard in every GCP3 key; no SST crosses either a
placement-group or virtual-shard/partition boundary.

The producer caches validated immutable identity routes without a shared lock.
Each seal allocates sample and original-application descriptor arenas together;
it preserves every original batch boundary and order. Per-metric key templates
cache validated immutable identity bytes; output keys remain detached, with only
part UUID, archive and physical slot filled during the seal. Archive, metadata,
raw rows and owner frontier still publish in the same atomic ingest.

`OpenReadOnly` opens Pebble in read-only mode and refuses core admission. It
publishes persisted snapshots without reconstructing writable reducer state.
The finite fixture uses it for complete raw and journal verification after the
original timed Drain/Close; writable reopen/reducer recovery remains covered by
the existing restart differential tests. `DefaultFixture.DeferArchiveOracle`
explicitly defers the production-size classic archive scan. Literal small
fixtures keep their complete archive/metadata oracle. The result labels this
deferral and never reports an archive-oracle pass for that run.
