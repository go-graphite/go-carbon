# Copied-part precedence (offline foundation)

A receiving catalog binds an immutable part to class 0 (copied). Original,
locally sealed parts have class 1. The source stream identity remains in the
binding, and the physical SST and descriptor remain unchanged. Slot winners
compare class first and source order only inside the same serial stream.
Independent donor streams at the same class fail closed rather than comparing
unrelated counters. This first-move foundation does not flatten prior moves.

`InstallCopiedPart` accepts the current dense group-file encoding and verifies
checksum, keys, archive policies and source frontiers before an atomic Pebble ingest of
the unchanged data file plus a small catalog SST. Retry compares the full
binding including its descriptor; it cannot relabel an existing live UUID.
Snapshots taken before installation remain unchanged. Pebble compaction keeps
the catalog entry and all source record orders; full and range reads use the
same precedence. The bounded range cache also retains each winner's origin.

The first operator usage is an independent OFFLINE read catalog:

```sh
storectl move-stage --catalog-dir /private/scratch/move-read \
  --part /private/scratch/source.sst \
  --descriptor /private/scratch/source.sst.descriptor
```

The command requires a new empty directory or its own `MOVECATALOG` marker and
opens only its `read-index` child. Repeat installations skip completed entries.
Output deliberately has `Ready:false`: this command certifies neither routing
nor lifecycle/catalog completeness, and is not a daemon adoption operation.
`sync --parts` remains the resumable whole-file transport preceding this step.

Cross-boundary coarse slots cannot be selected from either owner's partial
aggregate. `PlanMoveRollupRebuild` rebuilds them from the winning raw slots,
retains exact raw IEEE bits/orders and retains older coarse history outside the
touched fine coverage. The installer must prove complete fine coverage, persist
the rebuilt projection and seed the receiver's live reducer before switching
reads. Mixed-origin coarse reads currently fail closed until that authority
exists. The pure rebuild planner alone does not provide it.

Writable core recovery rejects imported catalogs until owner-serialized numeric
ID and reducer adoption is implemented. Do not ingest into `Engine.DBs` during
intake. The current daemon's producer catalog and reducer are local state, so
file arrival alone cannot safely update them. The owned-host dual-write/read
switch/performance proof remains required; this code does not claim a live move.

Required semantics: B never rolls back; a final incremental copy fills a raw slot
B missed entirely; a dropped overwrite of a slot B already holds remains as B
saw it, matching existing single-owner relay-drop and replica-divergence behavior.
