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

## Adopting older verification observations

Old journal rows are the authority for completed comparisons and their holes.
A completed observation without a mutation seal is adopted automatically instead
of reading its data again. Its authenticated journal scope still binds the target,
donors, catalog and verification windows. Stable current metadata supplies modern
seals and per-name clean frontier records; unknown metadata never manufactures a
clean frontier or authorizes READY.

Only recorded target deletion/expiration, overlapping restart gaps and donor
changes reopen adopted observations. The target expiration endpoint supplies
timestamped deletion receipts; existing READY recovery checkpoints supply bounded
restart windows. The journal retains these as `verification-invalidated` rows
with a reason, optional names and optional windows. Destructive events predating
the observation and windows outside its frozen scope do not reopen it. A restart
compares only the overlapping interval against every donor. Original hole results
outside that interval retain their repair intent.

Adopted rows retain separate donor fences and target presence/expiration receipts.
Later target writes do not invalidate their hole-coverage observation. A recorded
delete or a changed bounded donor fence rechecks only affected names. Changing the
donor inventory still requires a new authenticated scope. READY continues to
require every gap to be backfilled and verified, plus its independent all-donor
parity, schema and readback checks.

`progress.json` and Graphite expose cumulative `legacy_observation_adopted` and
`legacy_observation_reverified` counts. Each batch logs `sync_legacy_observation`
once with its range, counts and reasons. Replaying adoption does not increment its
count again. The earlier `legacy_revalidated_names` metric remains readable for
existing journals. Modern non-legacy observations retain their existing mutation
seals, including optional per-name `hole_proofs_v2` entries.

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

## Explicit retirement

Scopes normally register during verification, track writes while verification
is active, and release at terminal sync closeout. Idle expiry provides a safety
net for crashed clients, deleted sync state and older clients that never release.
A restart preserves registration timestamps; it does not renew scopes.

### Idle expiry

```toml
[whisper]
mutation-proof-scope-ttl = "72h" # 0s disables expiry; restart to change
```

Each exact scope stores `LastRegistered` as Unix nanoseconds. Re-registering the
same bounds renews this timestamp without changing its window revision stamp.
Ordinary writes do not renew it. The default 72h allows three days between proof
requests for a given name/window, accommodating long verification rounds and
multi-day pauses. It is an idle allowance, not a limit on total sync duration:
verification (`verifyFrontierBatch` → `proofStampsMetadata` →
`windowProofsVersion`), resume/frontier checks and READY resealing request proofs
again on all holders. Each request renews the scopes it touches. A name not
revisited for more than 72h becomes eligible for conservative revalidation even
if the overall pass is still running. Increase the TTL for longer idle intervals,
or use `0s` to retain scopes until explicit release/reset. `--no-release-proofs`
opts out of terminal release only; it does not disable idle expiry.

Expiry is lazy, with no background scanner. A write that reads history while
holding the metric's existing mutation stripe/reservation removes scopes idle
strictly longer than the TTL. Continuation writes do this before enabling
before/after reconstruction. Changed exact-block writes do it while staging
history. Removing the last scope deletes the entire history key in the same
point/revision batch; partial expiry preserves the maintained stamps for the
remaining scopes. Bookkeeping-only continuation updates commit the stamp or
tombstone even when point data does not change. Failed batches publish neither
expiry nor its counters. Pending imports and staged same-name batches keep their
existing reservation fences, so a prior writer cannot resurrect an old stamp
after an untracked change.

Legacy scopes have no timestamp (or zero): their first registration/write touch
with expiry enabled stamps them, giving them a full TTL grace period. They do not
expire immediately on upgrade. With expiry disabled, registration still records
renewals; write-time expiry/stamping is skipped. Histories remain Version=1 JSON:
the decoder uses `json.Unmarshal`, so old readers ignore the additional field,
and new readers accept its absence and unknown future fields. Rolling back to an
older writer can drop timestamps; a later upgrade grants those records a fresh
grace period. Clock rollback delays expiry conservatively. Missing or corrupt
history never supplies an old selective stamp: the existing revision/UNKNOWN or
error path remains in force.

Expiry changes only proof bookkeeping. An in-window mutation after retirement
still advances the whole-metric revision; re-registration starts at that current
revision. Consumers therefore revalidate conservatively after an idle interval
longer than the TTL, and cannot retain an old clean token across an intervening
change. No saved history is restored.

`storectl sync` releases proof scopes on the target and every donor after the
final comparison and, when requested, READY publication. It first durably writes
`proof-closeout.json`, then streams each participating catalogue in pages of at
most 1000 names, saving a separate byte cursor for each holder after its reply.
Independent READY and recovery catalogues retain their own holder sets. Separately
selected child scopes own their own closeout. State and catalogues remain on disk;
remove them only after cleanup succeeds. There is no release at copy completion,
between rounds, on cancellation, on residual failure, or merely on process exit.
A terminal `READY_EXCEPT` retains that verdict and its exclusion provenance.

An interrupted cleanup resumes with the same `--resume STATE` command. This
cleanup-only invocation finishes the already recorded historical closeout; it
neither republishes READY nor refreshes saved proof tokens. A subsequent normal
pass uses the existing frontier comparison and revalidates changed tokens. A
reply lost before the cursor is saved can cause an idempotent retry. Successfully
checkpointed pages are skipped. `--no-release-proofs` opts out, for example when
coordinating overlapping verification or communicating with an older server.

Release is conservative for other proof consumers: there are no scope owners,
and releasing one name can invalidate another consumer's proof reuse. Sync
releases all scopes on its catalogued names. Coordinate overlapping jobs or use
the opt-out. To retain scopes through longer idle periods as well, increase the
TTL or disable idle expiry.

### Maintenance API

Both operations require a configured JWT secret and an authenticated token with
explicit namespace `*` and operation `update` (or `*`). They are fenced against
whole-store copy and use `POST /v1/maintenance/mutation-proofs`:

```text
POST /v1/maintenance/mutation-proofs?action=release
X-Buckyd-Authorization: <JWT>
{"Names":[{"Name":"example.metric","ID":42,"Generation":3,"From":1700000000,"Until":1700000600}]}

POST /v1/maintenance/mutation-proofs?action=reset
X-Buckyd-Authorization: <JWT>
{}
```

Release accepts 1–1000 entries within a 32 MiB JSON body. Omit both `ID` and `Generation` to select the
current generation; otherwise both must match. Omit both bounds to release all
scopes, or supply one exact positive `[From,Until)` window. Overlap alone does not
match. Results preserve request order and contain `Name`, `Status` (`released`,
`absent`, `conflict`, `error`), `Released`, and optional `Error`. HTTP 200 can
contain per-name errors: callers must inspect every result. Each page uses one
synced atomic batch. Repeated release of absent scopes is harmless. The last
scope's removal deletes the entire history key, including an empty Version=1
record; leaving that record would keep mutation tracking enabled.

Reset returns `{"Finished":"<UTC timestamp>"}` after a synced range tombstone
covering exactly `proof/ranges/`. No keys are scanned, so no record count is
reported. It locks every metric stripe and drains pending import/update
reservations before deleting. Registration uses those same locks. An update
staged before the fence must commit and publish first; it cannot restore its old
history after reset. New stages and registrations start after the tombstone.
Only proof history changes: points, native and transfer receipts, deletion fences,
storage identity and proof-lineage files remain intact.

With no history, proof comparison uses the current whole-metric revision until a
new scope is registered. Every committed mutation advances that revision, so an
in-window change during an untracked interval cannot reuse an older clean token.
No saved history is ever restored. Missing or unusable metadata remains on the
conservative revision/unknown path. Future verification re-registers scopes;
retirement needs no data rollback.

### Counters

Prometheus exposes cumulative process counters:

- `pebble_mutation_proof_tracked_writes_total`
- `pebble_mutation_proof_untracked_writes_total`
- `pebble_mutation_proof_scopes_registered_total`
- `pebble_mutation_proof_scopes_released_total`
- `pebble_mutation_proof_scopes_expired_total`
- `pebble_mutation_proof_histories_expired_total`
- `pebble_mutation_proof_resets_total`
- `pebble_mutation_proof_release_errors_total`

Write counters count continuation update preparations (including no-ops or later
failed attempts) and changed non-continuation updates, classified by the history
seen under their mutation reservation. Scope counters count newly registered and
explicitly released scopes, not a durable inventory. Reset does not enumerate
released scopes. Expiry counters count scopes removed and whole history keys
deleted by successfully committed lazy expiry batches, not current inventory.
Release-error counts include generation conflicts and store
release/reset errors; HTTP admission failures are logged separately. Errors are
logged by buckyd and the client and leave cleanup state available for retry.
Graphite's `storage` module reports deltas as `mutationProofTrackedWrites`,
`mutationProofUntrackedWrites`, `mutationProofRegistered`, `mutationProofReleased`,
`mutationProofExpiredScopes`, `mutationProofExpiredHistories`, `mutationProofResets`,
and `mutationProofErrors`.
