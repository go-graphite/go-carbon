# Name catalog

`NameCatalog` is a durable, append-only table of stable local IDs for series
names and opaque definitions (policy/placement metadata, binary-safe). IDs are
local handles, separate from portable storage keys or placement identities, and
are never reused. Each catalog has a 128-bit epoch.

```go
catalog, err := partlog.OpenNameCatalog(catalogDirectory)
// Handle err and defer catalog.Close().
id, err := catalog.Intern(name, definition)
// Call catalog.Sync() before any record that references id is acknowledged.
```

One owner uses a catalog: no shared writer lock, no concurrent calls, and the
directory must be exclusive. Returned metadata is immutable catalog-owned memory.
`Checkpoint` is synchronous; call it from the owner.

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

Definitions are immutable within a catalog epoch. Include policy identity in the
opaque key when policy changes must coexist, or start a new catalog epoch and
retain the old catalog alongside its referencing segments. A changed definition
under the same key fails closed.
