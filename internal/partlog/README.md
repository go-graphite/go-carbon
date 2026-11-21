# Exact predictor journals

PDC3 preserves name bytes, opaque definitions, signed 64-bit timestamps and
arrival clocks, apply-group ordering, and IEEE value bits. Sparse clocks omit
only zero deltas. Each instance belongs to one stream owner.

PDC4 uses a durable `NameCatalog` instead of repeating name definitions in every
segment. These stable catalog IDs are local journal handles, separate from
portable storage keys or placement identities. Every record carries its 128-bit
catalog epoch, sequence and predecessor CRC. Frame IDs use signed deltas from
zero at each record boundary. Timestamp, clock and value prediction is identical
to PDC3. Names and definitions come exclusively from the matching catalog.

```go
catalog, err := partlog.OpenNameCatalog(catalogDirectory)
// Handle err and defer catalog.Close().
encoder := partlog.CatalogEncoder{
    Catalog: catalog, SeriesClocks: true, SparseClocks: true,
}
record, err := encoder.EncodeDurable(frames)
// Handle err, then append record to the journal and await its group fsync.
```

`EncodeDurable` fsyncs new catalog definitions before exposing their referencing
record to a journal that might sync independently. Clean catalogs do not fsync.
It batches definitions per encoded record; it does not fsync per point. For
one-second group commit, use plain
`Encode` and retain every returned record in memory. At the group boundary,
call `catalog.Sync()`, append the retained records, then fsync the journal before
acknowledging the group. Never publish references to an independently flushing
journal before the catalog barrier. This uses one catalog fsync per dirty group,
plus the journal fsync: two files cannot share one `fsync` syscall.
Never acknowledge points before the referencing journal record is durable.
An error poisons the relevant encoder/decoder; discard it and recover.

One stream owns its catalog and codec instances, with no shared writer lock.
Neither catalogs nor codec instances support concurrent calls. Returned metadata
is immutable catalog-owned memory. A stream must keep its catalog directory
exclusive; opening two writers to the same directory is unsupported.

Start a **new encoder at every segment boundary** and a new decoder at the first
retained segment. That resets predictors without resetting catalog IDs. Within a
segment, sequence/predecessor checks reject missing or reordered records. A
checkpoint does not reset a live encoder's predictor state. Trimming an earlier
segment is safe only when its application data is persisted and the retained
segment starts an independent predictor epoch; retain the catalog checkpoint and
its WAL suffix alongside all referencing segments. Catalog IDs are never reused.

The catalog WAL has a CRC-protected epoch header. Each append has a separately
CRC-protected length and a CRC over its header and contents. New WAL appends
batch at most 4MiB of estimated uncompressed metadata, sort names lexically,
front-code their common prefixes, retain their original numeric IDs, and zstd
compress each independent batch. `Sync` flushes pending batches and fsyncs once;
a full in-memory batch can be written earlier without syncing. Unusually large
individual definitions retain the legacy record format. The length high bit
marks a compressed group; new readers accept old and mixed WALs. Older readers
reject grouped WALs, so downgrade requires retaining a compatible checkpoint and
an empty suffix from a quiescent stream. Recovery discards a
physically incomplete final WAL append, and rejects corruption, conflicting
ID/name/definition bindings, holes and epoch mismatches. A checkpoint contains
the full name table, its entry count, zstd checksums and a whole-file CRC. It is
written to a temporary file, fsynced, renamed and directory-fsynced before the
WAL suffix is truncated and fsynced. A crash before reclamation can replay the
old WAL prefix, checking all duplicate ID bindings. Checkpoint is synchronous;
call it from the quiescent stream owner. Keep the old segment epochs until the
application's durable persisted-data frontier permits their removal.

The first PDC4 consumer is the offline `tool/v2-journal` catalog/capture probe.
Production dispatch and trimming must preserve PDX1/PDC3 decoding and establish
an explicit catalog-before-journal barrier. No production backend selector is
introduced here.

Definitions are immutable within a catalog epoch. Include policy identity in the
opaque key when policy changes must coexist, or start a new catalog epoch and
retain the old catalog alongside its referencing segments. A changed definition
under the same key fails closed.
