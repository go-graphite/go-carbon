# Window mutation proofs

The transfer digest endpoint accepts `Mode: "proof-v2"` with explicit positive
Unix-second `From` and `Until` bounds, interpreted as `[From, Until)`. `Step` must
be zero. Each result returns an opaque `Proof`, its current invalidation `Reason`,
and storage/process lineage identifiers. An empty proof means unknown ownership;
it must never certify completed verification. `proof-v1` remains available.

`storectl sync` negotiates v2 for its frozen verification windows and combines
proofs from every holder with the schema and serving archive. Full-history and
sampled-history verification additionally fence `[1, verification horizon)`.
Older endpoints retain the whole-revision/process fence. A changed proof during
verification leaves the affected receipt unclean.

## Storage continuity

The database stores an incarnation and current process marker. `PROOF-LINEAGE`
and addressable `PROOF-<process>` receipts form a hashed predecessor chain.
Publication writes and syncs a temporary file, renames it and syncs the directory.
A close seals the process only after readers and background writers have joined
and the WAL is synced. The next open preserves the proof era only if the database
marker, exact sealed sequence, receipt versions and complete chain agree.

A crash, missing link, unsupported version, interrupted publication, rebuilt
store or database-only rollback starts a new era. A complete rollback of both
the database and every external receipt cannot be detected without a separately
trusted witness. Chain validation is capped at 1024 processes; reaching the cap
conservatively starts a new era. Read-only store opens issue no v2 proofs.

## Mutation scopes

A proof request lazily registers its exact bounds for that metric generation.
The store retains at most 16 scopes and 16 recent range records per generation;
encoded history is bounded to 32 KiB. Scope revision stamps survive range-record
churn. Evicting an old scope can conservatively reopen that scope when requested
again. Registrations share the WAL; the clean process seal makes them durable.

Ordinary exact-block writes record changed old and new point timestamps,
including circular-slot evictions, removals and propagated rollups, atomically
with data and the native revision. A later write outside every registered scope
does not advance those scopes. Deletion, recreation and configuration changes
remain fenced by identity/configuration as well.

The continuation codec refines changed blocks, propagation buffers and input
buckets against before/after archive reads clipped to registered scopes. It
compares at most 65536 slots. Unsupported mutation paths, incomplete range
reporting, excessive ranges or larger comparisons keep the whole-metric revision
fence for that name. Imports, patches and maintenance without exact reporting
therefore remain conservative. These fallbacks never discard the revision fence.

## Intake boundaries still require separate evidence

A clean storage seal proves continuity of committed storage, not lossless input.
This protocol does not certify flushed capture sequence, successor replay
completion, uncertain relay batches, catalog membership or missing late points.
READY restart-gap recovery remains conservative until those independent fences
and affected name/timestamp ranges are implemented. Do not infer a safe input
gap from process wall-clock downtime or from a zero backlog gauge.
