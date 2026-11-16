# v2 lifecycle operations

This package ports the immutable-parts operational prototype to the core-first
seal interface. It is an offline integration layer, with no daemon activation.
It never reads a point before writing or deleting, and never changes vendor code.

| Operation | Current semantics | v2 operation |
| --- | --- | --- |
| Retention age-out | Each stored archive has its own step × count horizon; sample time controls query visibility. Reloading schemas does not shorten already stored history. | `ExpiredPartitions` selects only sealed, complete deletion units after exclusive block end + the longest **stored** horizon. `Owner.Reclaim` atomically records retirement and range-deletes the entire scoped partition. Mixed policies wait for the longest; duplicate deletion-unit manifests are rejected; exact physical reclamation needs homogeneous units. |
| Inactivity expiry | Admission/activity clock, not newest sample time. First active regex wins; exclusion skips a rule; grace adds to nonzero TTL; zero exempts; `until` is exclusive. Equality with the TTL expires. Unknown activity initializes before later expiration. Pending input and dry-run prevent removal. | `PlanExpiration` reuses the current parser/matcher. A serial owner checks generation/revision and commits catalog removal plus an incarnation tombstone. No per-name point deletion. |
| Resumed names | A new write can recreate a deleted metric; stale copies must not restore deleted history. | The portable incarnation authority must issue a generation greater than the tombstone. `Visible` masks every covered generation, including unstamped legacy history. The old incarnation is rejected on catalog replay/import. |
| Folded namespaces | Moving a cluster must preserve source prefix expiry and stored retention. | Use the same ordered prefix rules on the destination; retain actual stored policies in manifests. The target fallback must not replace the source overrides. Differential fixtures cover two source TTLs plus destination fallback. |
| Storage quota | Per-namespace limits on immediate namespaces, metric count, schema points and logical bytes; new names at the exact limit are allowed, above it rejected. Existing names/cache skip storage creation checks. `none` observes without dropping. | `Quotas` delegates to one existing host-wide `QuotaIndex`; no separate owner/vshard budget. The host index must include all hosted catalogs and pending names. |
| Throughput quota | Checked before storage/cache bypass. Empty input counts as one; whole batches are accepted or dropped; nested checks precede accounting. Usage rotates on the report interval, not on catalog publication. Limited high quotas retain classic striped approximate accounting. | Same classic index and recorder. Reload and catalog replacement must preserve the recorder for unchanged namespaces. |
| Quota configuration | Graphite glob patterns; last matching quota rule wins. Positive sizes/counts enforce; zero/negative controls do not. Throughput scales by integer `report-frequency / minute`, including its historical subminute behavior. | `classicquota.Rules` ports the previous translation exactly. Finite positive physical quotas are rejected for shared storage unless explicitly ignored; max-int remains the classic unlimited sentinel. |

## Consumers and ownership

`OpenCatalog(db, scope)` is the stopped-owner/offline persistent adapter. It owns
only its namespace, never `db.Close`. Its atomic synced batch includes catalog,
tombstones, revision, retired partition markers and whole-partition range drops.
It must not independently mutate a running core database: core's owner is the
only writer after intake starts.

`NewCoreCatalog(snapshot)` and `CoreCatalog.Plan(upserts, policy, now, pending)`
are the narrow core seal adapter. A core `Hooks.PlanSeal` callback supplies exact
original activity metadata and returns this `SealPlan` (or composes its sorted
files and Commit/Abort callbacks with its other planners). Core ingests that
checkpoint with raw data and its durable log frontier in one transaction. Only
Commit publishes the staged catalog. `ReadCoreCatalog` recovers from the **same
pinned core snapshot** used for data reads/copies. Subsecond arrival times survive
reopen. Tests exercise both SST ingestion and blind batch commits, expiration,
resumption, stale incarnation refusal, planning failure and pinned old snapshots.

The caller owns portable incarnation allocation, serialized plan/commit, admission
and pending-buffer fences, completion of crash replay, and supplying exact activity
updates. Replica-local chunkstore generations and producer-local IDs are not a
portable incarnation authority. Copies must carry the checkpoint, tombstones,
retired partition markers and durable frontier with the corresponding data files.
Readers must apply `Visible` to all raw, archive and legacy origins.

`NewQuotas(index, ready)` accepts the current `CarbonserverListener` directly
through its public `ShouldThrottleMetric` method. Quota parser translation is in the small
`classicquota` subpackage so the catalog owner does not import a serving backend.
The package does not create a listener, multiply budgets or add a writer lock.
The host discovery/index owner must publish catalog removal and refreshed storage
usage while keeping throughput recorders. The caller supplies a readiness fence for recovery and index publication. The
gate fails closed until ready; classic's unready listener would otherwise admit.

## Current integration limits

The current core's rollups live in partition zero and contain physical ring-slot
replacements. Their files are **not** valid time-retention deletion units. The
core seal extension API supports point records but not whole-range tombstones.
`CoreCatalog.Plan` therefore supports catalog/inactivity operations only;
whole-partition `Reclaim` is restricted to the standalone stopped-owner adapter
and real policy/time-partition manifests. The standalone adapter has its own
metadata representation and must never be run against a running or stopped
core or assume a rollup partition-zero file is old history. Physical file
reclamation follows snapshot release and compaction, not the logical drop.

Core currently rejects a custom `Hooks.PlanSeal` when `Config.Rollups` is true.
The adapter is therefore exercised with the raw core; composing it with the
built-in canonical rollup planner still requires a core-owned hook boundary.
Core raw/rollup records also lack portable incarnation stamps. Catalog-only
resumption tests do not prove old-history masking in those readers: stamping
parts and resetting the expired rollup incarnation require the format/core owners.

No daemon receiver, reader, copier or host index is switched over here. In
particular tombstone masking and host-wide dynamic usage publication require the
owning integrations. Full catalog copies/checkpoints per seal are correctness
scaffolding, not a production memory/throughput claim. Tombstone/retired-marker GC
is deliberately absent until replica and log frontiers can prove it safe.

## Validation

```
GOTOOLCHAIN=go1.27.0 go build -mod=vendor ./internal/v2life/...
GOTOOLCHAIN=go1.27.0 go vet -mod=vendor ./internal/v2life/...
GOTOOLCHAIN=go1.27.0 go test -mod=vendor ./internal/v2life/...
GOTOOLCHAIN=go1.27.0 go test -mod=vendor -race ./internal/v2life/...
```

`TestClassicDifferential` uses Go's overlay mechanism to compile the reused
fixtures inside the classic private expirer/trie packages. It changes no shared
source files and exports no test-only production API. The same policy, metadata,
quota limits and inputs drive both sides. All fixtures are synthetic and public.
