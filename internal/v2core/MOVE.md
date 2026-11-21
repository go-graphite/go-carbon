# Copied-part precedence and owner attachment

A receiving catalog binds an immutable part to class 0 (copied). Original,
locally sealed parts have class 1. The source stream identity remains in the
binding, and the physical SST and descriptor remain unchanged. Slot winners
compare logical timestamp first, then class for equal timestamps, then order
only inside the same serial stream. A copied timestamp at 1100 replaces a live
timestamp at 1000 in the same 100-slot ring position. For equal timestamps the
receiver's live value wins, preserving its IEEE bits. Read callers still apply
retention filters; adoption excludes expired records from reducer seeding.
Independent donor streams at the same class and timestamp fail closed rather than comparing
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

`PrepareMove` opens an independent receiving catalog and retains unchanged part
files outside the live owner. On a running L3 daemon, the serial producer calls
`AdoptMove`; a TCP receiver schedules that call through `Server.OnOwner`:

```go
prepared, err := v2core.PrepareMove(ctx, stagingDirectory, copiedParts)
// Handle err, retain prepared through the call, and close it afterwards.
err = receiver.OnOwner(0, func() error {
    return store.Producers[0].AdoptMove(ctx, prepared, now)
})
```

The producer interns canonical names in B's own catalog and syncs it first.
Copied A numeric IDs never become B identities. The existing serial core owner
seals preceding intake, resolves raw winners and rebuilds cross-boundary aggregates.
Pebble requires non-overlapping ranges within one ingest, while group files can
overlap in key range. The owner therefore stages each unchanged data file with
an atomic pending binding. Snapshot readers skip pending files, including after
restart. One final catalog ingest activates all bindings, receiving identities
and coarse baseline authority together. It then seeds the reducer and publishes the new
snapshot. No adoption application order or input cursor is invented. Restart
reconstructs the same reducer and identity before ordinary input-log replay.
Completed attachments are idempotent, including after a crash at the manifest
commit. Writable recovery continues to reject unadopted offline imports.

The first attachment supports one producer and one owner. Every imported name's
coarse windows must still fit within complete retained fine coverage; older
coarse history and steps wider than fine retention fail closed. A subsequent
implementation must prove a boundary inventory before retaining and adopting
older coarse history. Independent donor streams for the same name, previous
move flattening, lifecycle reset reconciliation and multi-owner adoption are
outside this first path. Raw part bytes and source counters remain unchanged.

Readers use the persisted baseline for coarse slots, then overlay later local
replacements. Copied raw slots retain class 0 for future incremental repairs;
adoption never promotes them to live. Ordinary stores retain the descriptor
inventory publication path; mixed stores currently rebuild their metadata
directory from the pinned snapshot. The administrative HTTP adapter in the
subprocess integration test is test-only; no production endpoint is added.

Required semantics: a final incremental copy fills a logical timestamp B missed
even when its physical ring slot contains an older live epoch. A dropped
overwrite of the same timestamp remains as B saw it. Source order counters are
never compared across owners, and copied files are never rewritten.

The own-daemon test keeps A, B and a reference daemon writing through copy and
receiving-daemon restart. It interrupts the copier, retries verified files,
kills B after catalog activation, then compares every raw and coarse value bit
with the reference. Its stream and seal rate are bounded for a correctness run;
it makes no load or throughput claim. Failed runs dump only their own child PIDs.
