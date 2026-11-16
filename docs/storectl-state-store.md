# Indexed sync state: offline prototype and integration plan

Status: experimental benchmark. The production sync command still uses its existing state files. This change adds an offline consumer so both engines can be assessed on the same input before choosing a production format.

## Why

Large copy units accumulate repeated metric names in catalogs, selection journals and JSON frontiers. Reopening a journal reconstructs historical rows and maps before useful work can continue. A durable store should load the current phase, position, counters and unresolved work directly, without replaying its audit history. Avoiding repeated remote work remains a responsibility of the sync state machine and its durable transfer receipts.

Acceptance criteria: resume in seconds independently of audit history; at least 50,000 unique name updates/s with synchronous commits; atomic outcomes and counters across crashes; at most 25% of the captured legacy core-file bytes; operator queries through standard SQLite tools. The migration must be automatic on first start.

## Architecture

A unit owns one local database. A single writer commits a bounded batch of membership changes, proofs, receipts, phase position and counters together. Readers use a consistent transaction. Remote writes use an outbox intent and an existing stable native operation identity; after an ambiguous disconnect, the client reconciles that receipt before advancing the local cursor. Neither SQLite nor Pebble can atomically commit a local transaction and a remote copy.

```text
legacy state -- locked streaming adoption --> unpublished state database
                                                |
                                     parity + source stamp validation
                                                |
                                     durable active-format publication
                                                |
sync batch -- native durable receipt --> membership + proofs + phase counters
                                                |
                                         one durable transaction
```

Stable catalog IDs are one-based sorted catalog ordinals, bound to an immutable catalog generation and scope. A new discovery generation cannot reinterpret old IDs. Catalog pages contain 4,096 names, encoded as common-prefix length, suffix, source mask and target flag, then Zstandard compressed. Each name is stored once. Membership uses ID ranges or packed ID/bitmap pages. Point proofs use fixed-width records in pages, retaining scope, holder identity, mutation generation and integrity seals. Unknown and torn proof records remain pending.

Proposed production relations:

| Relation | Key / contents |
| --- | --- |
| `unit` | schema, source identity, execution options, catalog generation, adoption state |
| `phase` | scope, phase, round, exact durable position, done/total, holes, quarantines, terminal state |
| `window` | immutable range ID/from/until, holder, selection/transfer/verify status and counters |
| `catalog_page` | first/last ID, searchable first/last name, encoded names/masks/target flags |
| `membership_page` | scope/window/holder/kind and ID range; packed union/missing/pending/residual bits |
| `issue` | sparse ID, holder, range, reason ID, policy/terminal distinction and observed evidence |
| `reason` | interned reason text and category |
| `proof_page` | scope/window/holder and fixed-width proof records keyed by ID page |
| `operation` | stable native identity, generation, exact selected IDs, issued/acknowledged state, receipt |
| `batch` | unique logical batch token and membership digest; counters/outcomes committed atomically |
| `adoption` | source stamps, consumed byte offsets and last committed import position per legacy file |

SQLite uses WAL, `synchronous=FULL`, prepared statements, bounded transactions, a single writer and a busy timeout. NORMAL is insufficient for acknowledged-commit durability after power loss. Database, WAL and shared-memory files are one operational unit; use the backup API or a clean closed copy, never copy just the live main file. The database must be on a local filesystem with working fsync semantics. Pebble uses synchronous WAL commits and indexed batches. Its block cache and memtables are bounded. Do not disable the WAL or fsync for import measurements.

The benchmark deliberately retains historical journal evidence as compressed, ID-normalized records. It is a conservative size measurement, not a claim that an audit archive constitutes a production state reducer. Production will materialize the relations above and retire obsolete audit rows only after equivalence tests. The prototype's generic `records` table and `done/<window>` rows stress one synchronous transaction per 8,192 names; the production representation will pack these into ID pages.

The prototype imports `union.tsv`, `missing.tsv`, `sync.jsonl`, pending selections, residuals and the small configuration/progress/resolution documents. It does not infer missing child receipts. If a capture omits child transfer receipts, per-window transfer completion and old-to-new executable swaps remain unqualified. A zero holes counter does not imply zero pending selections.

## Before / after

| Existing behavior | Proposed production behavior |
| --- | --- |
| Resume reconstructs journal rows and historical maps | Read the current phase and only the active batch/ID pages |
| Repeated names in catalogs and JSON arrays | One paged dictionary; ranges and IDs in every other relation |
| Phase counters, outcomes and frontiers have separate persistence paths | One transaction binds all acknowledged changes |
| Operators combine progress JSON with journal scans and child receipts | Indexed phase/window/issue views plus a bounded name lookup |
| A new binary understands several legacy files directly | First start adopts the files under the unit lock, then opens the versioned store |

## Usage

Build with cgo, matching release builds:

```sh
go build -o storestatebench ./tool/storestatebench
./storestatebench -mode=normalize -input=/offline/state -output=/scratch/state.spool
./storestatebench -mode=import -backend=sqlite -input=/scratch/state.spool -output=/scratch/state.db
./storestatebench -mode=import -backend=pebble -input=/scratch/state.spool -output=/scratch/state.pebble
./storestatebench -mode=query -backend=sqlite -output=/scratch/state.db
./storestatebench -mode=update -backend=sqlite -output=/scratch/state.db
./storestatebench -mode=crash-check -backend=sqlite -output=/scratch/fault
STORESTATE_LEGACY_FIXTURE=/offline/state go test -v ./internal/downsize -run '^TestCopyStateLegacyCost$' -count=1
```

Normalization validates every catalog name's ordering and round-trips each transformed legacy JSON value against its canonical original, preserving large JSON integers and unknown names. Both engines ingest identical framed records and verify a full logical-record count and XOR of per-record SHA-256 hashes. The hash comparison is order-independent; the import spool should also retain its ordinary file SHA-256 in measurement artifacts. Input directories are never modified. Output paths must be private scratch paths; use fresh paths for each comparison.

The query command returns the captured progress record, including phase, done, holes and quarantine counts. Standard tools can inspect small metadata and record labels directly:

```sql
SELECT json_extract(CAST(payload AS TEXT),'$.phase') AS phase,
       json_extract(CAST(payload AS TEXT),'$.done') AS done,
       json_extract(CAST(payload AS TEXT),'$.holes') AS holes,
       json_extract(CAST(payload AS TEXT),'$.quarantined') AS quarantined
FROM records WHERE bucket='progress.json' AND id=0;
SELECT bucket, count(*), sum(length(payload)) FROM records GROUP BY bucket;
SELECT id, label FROM records WHERE bucket='sync.jsonl' AND label='window-retry-visited' LIMIT 20;
SELECT id, CAST(payload AS TEXT) FROM records WHERE bucket='window';
```

Large normalized payloads are compressed and need the tool's codec; SQL can locate and size them. The production schema must expose readable window and reason views and a bounded `storectl state inspect --name/--id` command. The prototype does not yet implement that production operator interface.

## Exact landing plan after phase resume

1. Rebase on the merged exact-phase resume change; retain its phase entry receipts, durable floors and refusal to publish lower phase/done counters. Do not use progress JSON alone to invent completed work.
2. Add `internal/downsize/state_store.go` with a small transaction API and a versioned SQLite implementation. Split catalog, membership, operation and proof codecs into `state_catalog.go`, `state_membership.go`, `state_operations.go` and `state_proofs.go`. Keep the benchmark and both backend tests until the decision is reproducible; only SQLite enters the production dependency boundary if it qualifies.
3. Add `state_adopt.go`. Under the existing exclusive unit lock, authenticate the actual legacy header/options, all catalog generations and child receipts. Import into a `.adopting` database with transactional per-file positions. Restarts resume adoption from those committed offsets. Reject interior journal corruption; handle a torn final record using exactly the legacy rule. Stream large JSON arrays, preserving all named terminal and quarantine reasons. Include quarantine/policy JSONL ledgers, selection child journals, range receipt files, frontier/scope files and READY/gap-recovery children, not just the eight-file measurement fixture.
4. Run the existing legacy reducer and the new importer against the same frozen sources and compare exact per-phase position/counters, every window/group status, pending and residual membership, terminal reasons, all proof bytes, operation identities and READY verdicts. Validate source stamps again. Flush the database and directory, then publish the active format marker with a synced atomic rename. Retain legacy files until durable publication; fail closed on any mismatch. After publication an older binary must refuse the active state; silently reverting to stale legacy files would redo work. A rollback requires an explicit lossless export or a forward-compatible binary.
5. Change `sync.go`, `sync_checkpoint.go`, `sync_stream.go`, `sync_window.go`, `sync_window_retry.go`, `resume_progress.go` and `sync_restart_gap.go` only in that integration change. Replace journal-wide searches with indexed current-state reads, catalog streams with ID-page iterators, and separate progress/receipt writes with transactions. Update `sync_frontier.go`, `sync_quarantine.go`, `sync_redo_guard.go` and READY/recovery adapters so no parallel file remains a conflicting source of truth. Export `progress.json` only as an optional bounded derived view; durability belongs to the database.
6. Add `tool/storectl state` inspection for phase, done, holes, quarantined, per-window status, named terminal reasons and proof summaries. Document ordinary sqlite3 queries and backup behavior in `tool/storectl/README.md`.
7. Test legacy-to-store adoption for every historical journal generation; interrupted discovery/copy/window-retry/verify/READY; mutation fences; out-of-order overlapping worker batches; unchanged terminal reruns; and late successful repairs. Kill at every adoption checkpoint, before/after intent issue, remote acknowledgment, outcome/counter commit, phase change and format publication. Reopen repeatedly and compare with the legacy oracle. Inject short writes, full disk, failed fsync, torn WAL and corrupt records. A process SIGKILL test is necessary but does not simulate power failure or faulty hardware; add filesystem-level power-cut simulation separately.
8. Run the actual old executable on a complete fixture (including child receipts), stop only the offline process, then resume the exact same state with the new executable. Assert exact initial phase/done, no regression, `redo_names=0`, no repeated completed selection/copy RPCs, equal membership/proofs and unchanged READY/NOT_READY verdict. Repeat in every phase and after a second new-binary reopen. A progress-query microbenchmark alone cannot qualify this swap.

## Sources

SQLite documents [WAL behavior and backup constraints](https://www.sqlite.org/wal.html), [FULL versus NORMAL synchronization](https://www.sqlite.org/pragma.html#pragma_synchronous), and [WITHOUT ROWID tables](https://www.sqlite.org/withoutrowid.html). Pebble documents [synchronous write options](https://pkg.go.dev/github.com/cockroachdb/pebble#Sync). Keep the SQLite driver version current with its bundled SQLite WAL fixes before production integration.

## Measurements and decision

Pending real-capture measurements. This Draft must not be treated as an integrated sync state migration or a qualified executable swap.
