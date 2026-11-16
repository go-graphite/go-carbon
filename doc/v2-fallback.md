# Classic/v2 fallback

Use one input journal and keep classic current throughout the canary and hold.
No cross-engine write epochs or timestamp translation are needed while both
engines consume the same admitted prefix.

| Mode | Input | Reads | Classic writes | Cutback |
| --- | --- | --- | --- | --- |
| `shadow` | Common v2 input journal, synced once a second | Classic | Yes | Already classic |
| `v2-serving-with-classic-dual-write` | Same journal | v2 plus historical classic | Yes | Restart in `shadow` |
| `v2-only` | Same journal | v2 plus historical classic | No | Requires classic catch-up and verification |

Set `[v2] enabled = true`, an isolated absolute `path`, and `mode = "shadow"`
for a new canary. The existing `enabled = true` configuration with no mode keeps
its previous v2-only behavior. Configure the existing classic backend and its
normal persister. The first dual-write run requires an empty v2 root. Converted
v2 history needs a verified shared starting point before entering this workflow.

After validating ingestion, read parity, quotas and capacity, restart with
`mode = "v2-serving-with-classic-dual-write"`. Keep classic written for an
operational hold. Cut back by restarting with `mode = "shadow"`. Keep the same
classic paths, backend, policy and v2 journal root throughout the hold.

Finalize with `mode = "v2-only"` when the hold is complete. Its durable receipt
closes the rollback window before opening input. It deliberately refuses a later
shadow/dual start. A v2-only gap is the concrete case that needs catch-up; this
change does not invent an epoch protocol to disguise that gap. Restore and
verify classic in a new migration workflow before starting another hold.

All v2 configuration and policy reloads remain restart-only. A failed reload
leaves the running mode unchanged. Check for `config reload failed` after HUP.
There is no live swap of writer ownership or reader handles.

The journal is synced before either engine consumes a batch. A classic callback
submits that same durable prefix to the existing bounded cache and persister;
capacity pressure waits instead of dropping mirrored input. Recovery replays the
retained journal through both paths before opening reads. The classic cache must
drain before readiness, so replayed old points cannot temporarily override
newer persisted data in responses. The journal must remain retained throughout
the hold. Classic replay retains classic's existing retention and batching
semantics; this does not claim exact original Whisper application boundaries.

A classic persistence failure prevents recovery readiness. A journal/v2 failure
stops intake rather than letting the two engines silently diverge. v2 read
failures remain visible in the serving mode; an operator explicitly cuts back.
New classic files are initialized and synced privately, then atomically linked
into place and reopened at their final name. Killing a creator cannot expose a
partial `.wsp` that replay would retry forever. Temporary files have no `.wsp`
suffix and cannot appear as metrics. Existing damaged files remain storage
errors; they are not automatically erased, since their history may predate the
retained journal. A delayed replay drain logs one bounded cache/queue diagnostic.
Initial support excludes tags, concurrent classic migration, metadata prewarm
and expiration workers during dual write. Those policies require their own
cross-engine parity qualification before relaxing these guards. The existing
unseeded multiarchive union guard also remains: serving-dual startup rejects
overlapping multiarchive data before opening reads. Shadow may ingest those
metrics for parity checks; safe multiarchive serving needs seeded archives.

`go_carbon_storage_mode{mode="..."}` identifies the running mode with one-hot
gauges. `go_carbon_classic_mirrored_points_total` includes replay, and the
`v2-storage` internal information endpoint exposes mode and dual-write status.
Monitor the normal classic persister errors and pending cache as well.

VictoriaMetrics publishes immutable part sets and retains replaced parts while
readers reference them; snapshots hard-link immutable files. ClickHouse
MergeTree similarly publishes immutable active parts, retires replaced parts,
and exchanges whole parts for replication. We keep that part model in v2 and
use an ordinary dual-write hold for this cross-engine migration. The sole extra
receipt prevents an operator from treating a stale classic engine as current;
it carries no point epochs or data-format translation.

Sources: [VictoriaMetrics storage partition](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/lib/storage/partition.go),
[mergeset snapshots](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/lib/mergeset/table.go),
[vmstorage snapshot endpoints](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vmstorage/main.go),
[MergeTree parts](https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/MergeTree/MergeTreeData.cpp),
[ReplicatedMergeTree](https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/StorageReplicatedMergeTree.cpp).
