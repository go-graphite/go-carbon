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

The prototype now uses `modernc.org/sqlite` v1.62.0, a pure-Go driver. The repository public release workflow sets `CGO_ENABLED=0` for package builds and its Makefile cross-builds386/amd64/arm64. The measured driver comparison below selects modernc without changing that packaging contract. There is one SQLite driver in the dependency graph; the cgo comparison uses the prior prototype executable. Production storectl is still not wired to this experimental store.

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

Build this benchmark without cgo:

```sh
CGO_ENABLED=0 go build -o storestatebench ./tool/storestatebench
./storestatebench -mode=normalize -input=/offline/state -output=/scratch/state.spool
./storestatebench -mode=import -backend=sqlite -input=/scratch/state.spool -output=/scratch/state.db
./storestatebench -mode=import -backend=pebble -input=/scratch/state.spool -output=/scratch/state.pebble
./storestatebench -mode=query -backend=sqlite -output=/scratch/state.db
./storestatebench -mode=update -backend=sqlite -output=/scratch/state.db
./storestatebench -mode=crash-check -backend=sqlite -output=/scratch/fault
STORESTATE_LEGACY_FIXTURE=/offline/state go test -v ./internal/downsize -run '^TestCopyStateLegacyCost$' -count=1
```

Normalization validates every catalog name's ordering and round-trips each transformed legacy JSON value through its persisted, versioned encoding before comparing with the original. The decoder preserves exact JSON numbers and escapes literal objects matching either reserved catalog-ID/object shape. Backend tests also encode, store, retrieve and decode values containing nested reserved objects, unknown names and integers larger than2^53. These remain offline codecs, not the typed production phase/window/proof codecs required below. Both engines ingest identical framed records and verify a full logical-record count and XOR of per-record SHA-256 hashes. The hash comparison is order-independent; the import spool should also retain its ordinary file SHA-256 in measurement artifacts. Input directories are never modified. Output paths must be private scratch paths; use fresh paths for each comparison.

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
4. Run the existing legacy reducer and the new importer against the same frozen sources and compare exact per-phase position/counters, every window/group status, pending and residual membership, terminal reasons, all proof bytes, operation identities and READY verdicts. Validate source stamps again. Flush the database and directory, then publish the active format marker with a synced atomic rename. Use the versioned `config.json` envelope as that marker: legacy `restoreConfig` already rejects versions other than1; the new reader recognizes the store version and its authenticated database identity. Publish the database durably before replacing this config, so a crash before cutover leaves a resumable legacy import and a crash after cutover opens the database. Retaining an unrecognized extra marker beside a version1 config would not fence an old executable. Retain legacy files until durable publication; fail closed on any mismatch. After publication an older binary must refuse the active state; silently reverting to stale legacy files would redo work. A rollback requires an explicit lossless export or a forward-compatible binary.
5. Change `sync.go`, `sync_checkpoint.go`, `sync_stream.go`, `sync_window.go`, `sync_window_retry.go`, `sync_phase_resume.go`, `resume_progress.go` and `sync_restart_gap.go` only in that integration change. Replace journal-wide searches with indexed current-state reads, catalog streams with ID-page iterators, and separate progress/receipt writes with transactions. Update `sync_frontier.go`, `sync_quarantine.go`, `sync_redo_guard.go` and READY/recovery adapters so no parallel file remains a conflicting source of truth. Export `progress.json` only as an optional bounded derived view; durability belongs to the database.
6. Add `tool/storectl state` inspection for phase, done, holes, quarantined, per-window status, named terminal reasons and proof summaries. Document ordinary sqlite3 queries and backup behavior in `tool/storectl/README.md`.
7. Test legacy-to-store adoption for every historical journal generation; interrupted discovery/copy/window-retry/verify/READY; mutation fences; out-of-order overlapping worker batches; unchanged terminal reruns; and late successful repairs. Kill at every adoption checkpoint, before/after intent issue, remote acknowledgment, outcome/counter commit, phase change and format publication. Reopen repeatedly and compare with the legacy oracle. Inject short writes, full disk, failed fsync, torn WAL and corrupt records. A process SIGKILL test is necessary but does not simulate power failure or faulty hardware; add filesystem-level power-cut simulation separately.
8. Run the actual old executable on a complete fixture (including child receipts), stop only the offline process, then resume the exact same state with the new executable. Assert exact initial phase/done, no regression, `redo_names=0`, no repeated completed selection/copy RPCs, equal membership/proofs and unchanged READY/NOT_READY verdict. Repeat in every phase and after a second new-binary reopen. A progress-query microbenchmark alone cannot qualify this swap.

## Sources

SQLite documents [WAL behavior and backup constraints](https://www.sqlite.org/wal.html), [FULL versus NORMAL synchronization](https://www.sqlite.org/pragma.html#pragma_synchronous), and [WITHOUT ROWID tables](https://www.sqlite.org/withoutrowid.html). Pebble documents [synchronous write options](https://pkg.go.dev/github.com/cockroachdb/pebble#Sync). Keep the SQLite driver version current with its bundled SQLite WAL fixes before production integration.

## Measurements and decision

Measured on2026-11-16 on one271,248,522-name captured unit (81,007,246,888core-file bytes), using Go1.27/cgo,16allowed CPUs and idle I/O priority on actual disk. Co-tenant load and filesystem caches were not controlled. Both engines received522,808identical normalized records and passed a full logical-record count/hash scan.

| Metric | SQLite WAL/FULL | Pebble Sync |
| --- | ---: | ---: |
| Closed database bytes | 1,354,366,976 | 1,007,049,459 |
| Fraction of legacy core bytes | 1.672% | 1.243% |
| Backend ingest from normalized spool | 14.082s | 5.681s |
| Full logical-record parity scan | 3.071s | 2.097s |
| Open + captured progress lookup | 1.376ms | 178.654ms |
| Unique durable name updates/s | 267,984 | 41,848 |
| Replay1,048,576IDs: new completions | 0 | 0 |
| Import peak RSS | 143.0MiB | 242.9MiB |
| Update peak RSS | 34.5MiB | 70.5MiB |
| SIGKILL before/after commit | pass/pass | pass/pass |

Recommend **SQLite** for the integration: its measured bytes and synchronous unique-update throughput meet the target, and standard SQL tools can inspect indexed metadata. Pebble imports faster and is smaller here; its per-ID read/insert workload is below the50,000names/s target. Packed production membership pages may change that result.

The shared conversion took390.992s using a2.735s reused catalog; the original catalog construction took87.006s. Substituting that original stage gives475.263s conversion plus14.082s/5.681s engine ingest: estimated489.345s/480.944s first-adoption work, excluding parity. These are sums of separately measured stages, not an uninterrupted adoption benchmark. A direct adopter should avoid the1.104GB intermediate spool. Conversion peak RSS was2,137.7MiB.

Real legacy journal replay took39.218s for419,542rows; a separate real executable startup reached its preserved phase/counter in39.002s. Legacy8,192-name append+fsync took4.112ms plus39.363ms to rebuild the resolution map. The captured database lookup is a metadata microbenchmark, not an integrated new-store resume. Complete child transfer receipts were not captured, so full per-window completion, proof-state size and old-to-new executable swaps remain unqualified.

The update workload is1,048,576unique IDs in128synchronous8,192-ID transactions, binding exact token membership, outcomes and counters. Kill tests cover staged-before-commit and immediately-after-acknowledged-commit boundaries on private databases. Reopening and replay produce the exact unique count. They do not simulate hardware power loss or remote transfer atomicity.

Read-only operator SQL returned phase/done/holes/quarantine in0.646ms, grouped journal kinds in29.309ms, a synthetic window counter in0.067ms and an exact completed ID in0.079ms. `PRAGMA quick_check` returned `ok`. The complete production window/reason/proof views and first-start adopter still need implementation. This Draft is an offline prototype and design plan.



## Pure-Go driver qualification and resumable offline import

The follow-up compares the original cgo executable (`mattn/go-sqlite3` v1.14.53)
with modernc v1.62.0 on the exact same normalized input and unique-update
workload. Both import522,808records with count/hash parity. Both use WAL/FULL,
one connection, prepared statements and128batches of8,192unique IDs.

| Driver | Unique names/s | First open + captured progress | Decision |
| --- | ---: | ---: | --- |
| mattn/cgo | 287,989 | 0.620ms | Comparison executable only |
| modernc/pure Go | 212,919 | 1.151ms | Selected; exceeds50k names/s and meets10ms open threshold |

The follow-up used a private tmpfs, so these numbers establish driver overhead
and process-crash behavior while issuing FULL synchronization. They do not
measure physical storage latency or establish power-loss durability. The earlier
on-disk SQLite results above remain a separate experiment.

The offline `import` command now resumes automatically at a committed frame
boundary. A versioned cursor stores the complete input SHA256, byte offset,
record count and logical-record hash in the same transaction as its batch.
Restarting the same command skips already committed insertions. It still scans
the immutable input to authenticate its digest and compute expected per-relation
counts/hashes, then checks every imported relation against those values. A
changed input, stale cursor, malformed frame, extra record or parity mismatch
fails. Six subprocess SIGKILL cuts cover before/after commit at the first three
batch boundaries. SQLITE_FULL coverage checks that membership, batch token and
counter roll back together and the reopened database passes integrity_check.

This progress does not implement automatic legacy-state adoption in production.
`internal/downsize` retains its existing persistence boundary. The remaining
production gates are the complete typed relations, every child receipt and
READY/recovery state, exact reducer parity, durable format publication and real
old-to-new SQLite executable swaps. The future-version config fence test can be
run against separately built older executables using
`STORECTL_FENCE_OLD_BINARIES`; it proves rejection of that proposed marker, not
that production cutover is complete. Keep this PR Draft until those gates pass.
