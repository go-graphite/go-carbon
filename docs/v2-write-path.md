# V2 write path: integration and migration plan

Status: design, not an implemented configuration or a release qualification.
The [offline prototype](https://github.com/go-graphite/go-carbon/pull/1399)
shows the value of owner-local sorting and blind SST ingestion. Its finite
throughput runs do not establish sustained compaction, complete retention,
populated render latency, or daemon lifecycle correctness.

## Architecture and switch

Keep the existing engine as the default and the behavioral oracle. Introduce a
restart-only `[whisper] write-path = "v2"` selector, with `"legacy"` as default,
a separate `v2-dir`, and an explicit read-only `legacy-store-dir`. These names are
proposed. Enabling v2 requires input logging and a compatible format marker;
paths must be distinct. Never reinterpret the legacy database in place. Reject
unsupported receiver, policy, handoff or copy combinations at startup, before
accepting input, rather than silently running part of the old write path.

```text
all receivers / local stats / recovery
             |
     bounded durable input stream -- group sync, nominally 1 second
             |
     decode, canonicalize, admit, route to stable owner
             |
     owner-local head -> freeze/sort each second or byte limit
             |                       |
     immutable read view       blind parts/SST ingest
             |                       |
             +----- atomic generation + durable replay frontier
                              |
                   seal/compact rollups and retention
                              |
              carbonserver / cache API / storectl adapters
                              |
                 read-only legacy history overlay
```

Owners are single writers of bounded buffers, assigned by canonical metric
identity. A Go worker owns a buffer; this is not reliance on a goroutine staying
on one physical core. Producer queues, decoding, name admission, frozen heads,
read pins, staged copies and compaction all count toward byte budgets. Bound
shared resources without a global per-point reservation bottleneck. Fix owner
routing for an epoch and record it durably; change owner count only through a
restart/repartition boundary. The prototype's private-database advantage does
not settle topology: compare disjoint key ranges in one database with private
owner databases at matched caches and compactor counts. Never multiply a full
block cache by the number of cores accidentally.

Normal ingestion appends and sorts; it never fetches the old archive per point.
Sealing/compaction may read affected history to reproduce exact archive state.
A periodic timer must seal idle buffers too, even if no next batch arrives.
Do not equate physical Pebble compaction with logical Graphite rollup work.

## Integration map

The following are verified source seams, not interfaces already implemented by
the prototype. Refactor only with an immediate consumer and preserve the legacy
adapter. Do not make v2 implement cache internals or virtual-file writes.

| Boundary and current source | V2 integration contract |
|---|---|
| `carbon/app.go`: `startStorage`, `startPersister`, `startReceivers` | Select the engine once. Wire every sink: `Add`, single/batched points, stream fast paths, UDP, pickle and registered plugins. Some paths call `core.Add` directly; changing only TCP callbacks leaves a split pipeline. Copy decoder-owned bytes before retention. |
| `carbon/collector.go`, `api/`, CarbonLink | Local stats use the same intake adapter. Cache-query protocols read the pending immutable overlay; keep response semantics. A minimal cache kept for API compatibility must not retain a second full write queue. |
| `carbon/input_log.go`, `internal/inputlog/`, `internal/recovery/` | Replace cache-owned journal recording with a stream source/sink and durable owner frontiers. The [group-commit change](https://github.com/go-graphite/go-carbon/pull/1392) concerns startup capture; it does not by itself integrate v2 or authorize retiring the continuous log. Reuse framing, checksums, capture ordering and dictionary recovery. |
| `cache/cache.go`, `internal/inputlog/segment.go`, `tags/` | Preserve name limits, tag validation/normalization errors, canonical aliases and source order before owner routing. Equivalent tagged spellings route together. Raw-frame capture may precede admission: persist the admission/config generation needed to replay the same decision, or persist admitted canonical records separately. Raw logged points are not automatically accepted metrics. Preserve `Tags.Add` creation notification currently supplied by the persister. |
| `carbon/app.go`: `configureCarbonserverQuotas`, `listenCarbonserver`; `persister/whisper_quota.go` | Keep exact throttle/drop semantics and count canonical names once across legacy, pending and v2 catalogs. Restored/already accepted input bypasses live quotas as today. Do not charge rejected points twice on replay. Reserve/release quota state with name publication/deletion. Existing logical-size estimates remain logical, not compressed v2 byte counts. |
| `carbon/expiration.go` | Preserve last successful local write activity, dry-run/rule order and startup pending guards. Rejected/no-op points do not refresh activity; imports retain current import semantics. V2 deletion publishes generation tombstones that also mask legacy history and updates quota/index state. Physical part reclamation waits for pins and copy leases. |
| `carbon/index_refresh.go`, `carbonserver/metricstore.go`, realtime index | Build a union catalog of legacy names plus v2 creations minus tombstones. Stable catalog/name generations accompany part publication; pending new names remain discoverable through realtime indexing. Resume catalog scans by cursor. A full scan must not lock intake. Include namespace hashes, list/details/info and quota reporting, not just find. |
| `carbonserver/fetchfromdisk.go`, `fetchsinglemetric.go`, `info.go` | Replace concrete `*chunkstore.Store` coupling with a narrow snapshot-backed read adapter. One request sees compatible policy, catalog, head and part generations. Both HTTP and gRPC retain range alignment, archive choice, NaN behavior and response metadata. Do not apply the old cache overlay a second time. |
| `carbon/collector.go`, `carbon/input_log.go`, Prometheus collectors | Export received/admitted/rejected/replayed points separately from durably published points. Add unsynced log age, per-owner backlog/oldest age, active/frozen/pinned/staged bytes, visible/durable frontiers, parts, rollup lag, expiry and compaction debt/stalls. Keep existing receiver/error/read/copy signals. Preserve compatible counter meaning; add v2 names where old updates/s no longer applies. Register once; collectors never take lifecycle locks. |

## Durability, read visibility and lifecycle

Distinguish **accepted**, **log-synced**, **read-visible** and **part-durable**
frontiers. A TCP write returning is not an application durability receipt; UDP
cannot gain an acknowledgement guarantee. Group sync targets one second during
normal operation, not a hard maximum during disk failure. State precisely what
unsynced suffix may be lost and never acknowledge a durable frontier beyond
synced bytes. Errors are logged, metered and returned; retain replay sources.

Each canonical name needs a collision-free identity; a hash is only a routing
hint, never its durable identity. Every record needs a stable identity (log epoch, partition, frame/offset and
ordinal), source order and policy/apply context. Replaying it must not create a
new correction or refresh activity twice. The durable commit ordering is:

1. Validate/recover the input prefix; route accepted ordered records to owners.
2. Write immutable output and checksum it; sync files and directory entries.
3. Durably commit the part set, catalog/policy/tombstone changes and applied
   frontier as one recoverable publication. For multiple databases, an owner
   commit is atomic locally and a coordinator checkpoint references only complete
   owner commits. There is no assumed cross-database Pebble transaction.
4. Publish the immutable read generation, then release covered head memory.
   Retire a log segment only when **every** dependent owner, capture epoch,
   transfer and rollback consumer has crossed it. Recovery ignores or cleans
   unreferenced staging; it never infers commit from an SST existing on disk.

A reader pins a reference-counted generation containing the frozen head, parts,
legacy snapshot, metadata and tombstones. Swapping parts for a durable sealed
representation must expose neither a gap nor the same point twice. Writers
never modify published memory. Cancellation releases pins and byte admission.
Long readers/copy sessions cannot retain unlimited generations.

One-second disk sealing is not permission to hide accepted points for one
second: today's cache is readable before persistence. The read owner must supply
an immutable head cut that includes all input visible before that read's fence
(for example, an owner message that freezes a small head), without a reader
locking the mutable writer buffer. Specify that linearization point and prove
it against legacy pending-read behavior. The prototype's sealed-only snapshots
do not satisfy it. Preserve today's distinction between pending fine-cache
values and committed coarse rollups until differential tests justify each cut;
never expose newly recomputed coarse values earlier merely because they exist.

| Event | Required behavior |
|---|---|
| Startup/replay | Take socket handoff and begin bounded capture before catalog/checkpoint loading. Recover manifests/owner frontiers, then feed the same owner/seal path from retained history in existing epoch order. Fence live promotion per canonical metric behind its history. Resume predictor/dictionary state before decoding a suffix. Adapt `startupSink`, pending recovery and capture acknowledgements; never call the legacy cache sink in v2 mode. |
| SIGHUP | Validate a complete candidate before publication. Engine, paths, owner topology and log settings require restart. Apply supported policy/settings as immutable generations; existing metric retention/aggregation remains its stored policy unless an explicit migration is supported. Stop/join the old expirer before replacing it; do not recreate a legacy persister. Failed reload keeps the previous active generation. Verify logs for `config reload failed`. |
| SIGUSR2 / shutdown | Keep intake captured while cancelling/joining optional index, migration, expiry and compaction work. Resolve entered work without a lock cycle. Freeze heads, sync log and checkpoint a v2 manifest/frontier; hand over sockets/decoder state using existing capability negotiation. Do not force an entire v2 backlog through legacy cache dumps. The checkpoint format needs an explicit v2 fence; unsupported successors refuse it. On failure retain log/parts and report failure. |
| Incompatible handoff | Negotiate before releasing the predecessor. Use the proven stop/rebind path only when its input-safety contract is met; reject unsupported combinations before activation. Tags/custom protocols currently have narrower capture eligibility than built-in TCP. |

## Existing data and exact merge semantics

At a migration fence, commit outstanding legacy writes and establish a durable
cutover record while newer input is captured. Open the old database through a
**real read-only adapter**: current `chunkstore.Options` does not expose a
read-only open. Simply declining application writes still permits background
maintenance, format upgrades and checkpoint side effects. This adapter is a
prerequisite; never share a writable database between processes.

Keep legacy history beside v2 initially. There is no mandatory eager conversion
or startup rewrite. This can leave a dual reader for years with long retention.
An optional resumable background converter exports complete authoritative
archive state into immutable baseline parts, verifies it, then atomically marks
the covered name/archive/window migrated. Preserve the old files until verified
coverage and rollback retention permit removal. This converter must preserve
coarse values and continuation state; it must not infer them from fine data that
has already expired. A mere format-reencoding converter is not a replacement
for verifying logical history.

The merge precedence is explicit and differs by operation:

| Input to a read/merge | Precedence and constraints |
|---|---|
| Legacy base | Lowest authority for covered legacy archive slots, including authoritative imported coarse archives. |
| Local v2 output | Apply corrections in proven source/apply order, including physical-slot ownership invalidations before query-time filtering and explicit empty replacements. Policy/name generation prevents an old incarnation leaking into a recreated metric. Physical ingest order or timestamp alone is insufficient. |
| Read-visible frozen head | Overlay only the pending semantics supported at that archive/grid, at the pinned visibility frontier. Replay duplicates retain their original identity. |
| Expiration/retention/migration masks | Hide covered older data, including fallback. An occupied NaN preserves its value bits and the oracle's occupancy/XFF behavior; it is distinct from a deletion tombstone or missing slot. None invites legacy resurrection. Retention masks and name deletion are different operations. |
| Historical copy | Existing target values/slot owners win under today's native union rules; donor input has a separate provenance domain. It is not a newer local correction. Future local writes still win normally. |

Mixed legacy/v2 coarse windows need baseline continuation, occupancy and
propagation state. Bootstrap affected windows once during sealing/compaction,
then publish exact replacements with coverage masks. Never recompute an entire
coarse archive from partial post-cutover fine data, merge partial averages, or
replace an old valid coarse slot after an XFF failure if the current engine
would preserve it. The normal point append path remains blind. If the required
baseline state cannot be represented exactly, that policy cannot be activated.

Bit equality covers every float64 bit, timestamp, query grid, null, duplicate,
archive and metadata field. Use the current configured engine as oracle at a
fixed clock and preserve update boundaries where observable. Test all supported
aggregations (average, sum, min, max and last, plus any stored method being
activated), float32 XFF comparisons, sparse/dense
windows, late corrections after fine expiry, future ring collisions, imported
coarse data, tag aliases, signed zero, NaN payloads, infinities and reloads.
Sorting by metric/timestamp/sequence alone is not an equivalence proof.
Rollups run on sealing/compaction, with exact ordered arithmetic and correction
invalidation; never on arbitrary per-part partial averages.

## Streaming copies

Extend `storectl sync` and existing native sessions/receipts; do not introduce a
second copy controller. [Sorted-table transfer ingestion](https://github.com/go-graphite/go-carbon/pull/1391)
and [native history combine](../doc/native-combine.md) already separate staging
from publication. Reuse bounded streams, hashes, hidden staging, durable receipts,
lost-ack resolution, saved state and retry/redial behavior.

Negotiate a versioned logical part envelope: format/policy, canonical-name to ID
mapping, owner epoch, archive/time coverage and physical slot geometry, source frontier/correction order,
tombstones and content digest. Pin the logical manifest, not physical Pebble SST
filenames, which compaction replaces. Seal/freeze the selected snapshot tail
without draining ingestion. Resume by manifest/part identity and byte/piece
cursor; an expired pin is resolved explicitly, never silently replaced by a
new snapshot under the old receipt identity.

Direct whole-SST ingestion is valid only when key ordering, codec, IDs, shard
layout and provenance are compatible. Otherwise stream sorted logical records
through a bounded ID remapper/SST builder; no point-wise archive update loop.
Mixed old/new endpoints use the existing exact exporter/importer until both
advertise parts support. Legacy-to-v2 preserves each archive independently.
V2-to-legacy requires a proven compatibility exporter or remains unsupported;
never advertise success after dropping head/correction/tombstone information.

At the receiver, verify files before ingest, keep staged parts hidden, then
atomically publish target-wins coverage and a durable receipt. Revalidate local
name/policy generation at publication without holding writer locks during IO.
Retries are idempotent; interrupted partial publication resolves from receipts.
Keep all-donor/peer and gap-window verification in `storectl`; part checksums
prove transport integrity, not rendered equivalence. Keep copies running across
restart and require progress to resume within about two minutes of reads
reopening. Do not pause or drain them as a release prerequisite.

## Qualification, canary and rollback

Start with one single-archive shape, verified from the stored catalog as well
as current schema rules. A host label is insufficient: custom rules and fallback
patterns can introduce multi-archive exceptions. It avoids multi-level propagation while
still exercising catalog churn, retention, replay and copy overlap. Qualify
multi-archive shapes independently before enabling them; they are not covered
by a fast single-archive sink result.

| Stage | Gate |
|---|---|
| Offline correctness | Zero differential mismatches for writes, pending reads, sealing, compaction, expiry, copy, reload and restart. Fault-inject every log sync/output sync/manifest/frontier/receipt edge. Race and lifecycle cancellation suites pass. Local Go build/vet/test of touched packages pass before each push. |
| Sustained private benchmark | At least 30 minutes at 1.25 times measured canary peak, then settle debt. Bound process RSS, oldest backlog, read pins and rollup lag; no positive backlog/debt slope over the last 10 minutes. Measure physical disk output including log, metadata, rollups and settled compaction, not only process mean bytes before close. Respect the host's network, disk and CPU budgets. Run populated renders and concurrent copies; report misses separately. |
| Shadow | Same captured input and clock/policy boundaries into separate storage; compare actual find/info/render and per-archive state. Existing writer remains authoritative. A shadow result is not proof of takeover/restart safety. |
| One serving canary | Preserve legacy store, input logs, old binary and rollback mapping. Verify startup capture drains faster than intake, read/find parity, no new errors/drops/retries, stable resources and copy resume. Read p99 is reported; higher latency is acceptable only within the serving deadline with no new retries/5xx. Observe at least one day and exercise retention via accelerated offline clocks first. |
| Expand | One low-rate single-archive host, then highest-intake single-archive host, then a 2–4 archive host after full rollup qualification; expand one deployment zone before other zones. A read-light canary does not qualify read-heavy expiration. |

Rollback is a data operation once v2 has accepted unique writes. First switch
the writer back only with a v2-capable mixed reader and an ordered transition
fence; retain the v2 overlay, masks and read/copy support. Establish precedence
for later legacy writes by epoch/window coverage, or freeze and materialize the
v2 delta before admitting later legacy writes. Never blindly let old v2 values
win forever over the resumed writer.

A pre-v2 binary rollback additionally requires materializing all post-cutover
writes, deletions, coarse state and corrections into a legacy-compatible store
and verifying parity through the rollback frontier. Retained log replay is an
option only after its order, policy, clock and apply-boundary equivalence has
been proved; raw input alone is not assumed sufficient. Retain logs from the
cutover until this guarantee exists, sized for the rollback horizon. If disk
cannot support that horizon, do not activate. Never treat a flag flip or binary
revert as conversion. An old binary must refuse v2 directories/checkpoints.

## Delivery sequence and owners

Owner names below identify campaign workstreams, not runtime services. All
production integration remains pending; benchmark tools are not daemon wiring.

| Order | Owner | Reviewable change and dependency |
|---|---|---|
| 1 | input-log owner | Finish group durability/capture integration and expose stable replay identities/frontiers with crash tests. Preserve current log safety until the v2 checkpoint consumer is proven. |
| 2 | v2-core | Own the prototype base: owner-local buffers, blind ingest, durable publication/frontiers, memory and retention bounds, and a real log-driven fixture consumer. Freeze the parts/snapshot format with the next two owners. |
| 3, parallel on 2 | v2-rollup; v2-read | Rollup owner: exact sealing/compaction, policy/retention/correction state. Read owner: frozen-head+parts snapshots, bounded pins, catalog and render merge. Each includes a differential consumer, not an unused abstraction. |
| 4 | v2-plan | This integration/migration/copy plan; documentation only. |
| 5 | v2-core integration owner | Implement restart-only config, intake adapter and **all** receiver/stats/API/startup paths, plus real read-only legacy access and quota/expiry/catalog wiring. Activate only combinations qualified by owners 2–3. One owner edits shared lifecycle files. |
| 6 | copy owner, assigned by coordinator | Extend current native/storectl protocol with parts streaming, mixed-version fallback, target-wins publication and restart/resume proof. Implement optional converter separately on the same interfaces. |
| 7 | release/config owner, assigned by coordinator | Run sustained/shadow/canary gates, encode accepted config in deployment source of truth, then expand by shape and zone. No production action is authorized by this document itself. |

Keep the prototype and dependent PRs Draft until qualification. Contributors
stack into the prototype branch while its core owner integrates disjoint files.
Merge dependency PRs into that branch intentionally. When the base lands, retarget
remaining PRs to the release base and check reachability; a merge into a retired
stack branch does not deliver the change. Preserve merge commits.
