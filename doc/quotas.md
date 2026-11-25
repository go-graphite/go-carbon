# Quotas

In large Graphite (go-carbon) storage installations, it's desirable to have control over how many resources a user can consume.

Go-Carbon addresses this issue by the implementation of pattern-matching based quota design.

Limitations/Caveats:

* The current implementation is based on concurrent/realtime trie-index, so to use quotas, also need to enable those features on go-carbon.
* Pattern matching rules aren't regexp, but like graphite query syntax, using globs.
* Quota and usage metrics are only produced if the the control has non-empty quota values.

## Available controls

* namespaces: how many sub-namespaces are allowed for the matched prefix/namespaces
* metrics: how many metrics are allowed for the matched prefix/namespaces
* logical-size: how many logical disk space are allowed for the matched prefix/namespaces
* physical-size: how many physical disk space are allowed for the matched prefix/namespaces
* data-points: how many data points (inferred using storage-schemas.conf) are allowed for the matched prefix/namespaces
* throughput: how many data points are allowed within a minute interval for the matched prefix/namespaces
* dropping-policy: available values includes `none` and `new`. `none` means doesn't not drop any values. `new` means dropping new metrics. `none` can be used to produce only the quota, usage, and throttle metrics without actually dropping the data.

### Why `logical-size` and `physical-size`

If `whisper.sparse-create` or `whisper.compressed` is enabled, logical size could be much larger than physical size.

Distinguishing them gives us control for that scenario.

With `pebble-chunk`, physical-size limits are rejected because storage is shared
across metrics. Set `pebble-chunk-ignore-physical-quotas = true` in `[whisper]` to
accept and ignore those limits while keeping every other quota control active.
This option defaults to `false`, is reloadable via SIGHUP, and has no effect on
other storage backends. Ignored physical-size limits produce no per-namespace
physical quota or usage metrics.

### `data-points` and `logical-size`

Usually, `data_points` corresponds to `logical-size` as they are both determined by its matching retention policy.

### `throughput`

Throughput controls how many data points are allowed within a minute interval.

This is useful for us to monitor how many datapoints are sent to a specific pattern/namespace. It's useful for cases like some of those namespaces are generating too many datapoints and causing other namespaces to be overflown in the memory cache.

The value is reset every interval specified by `carbonserver.quota-usage-report-frequency`.

If `carbonserver.quota-usage-report-frequency` is 5 minute, then `throughput = 600,000`, then it means only 3M data points per every 5 minute are allowed to be pushed to the matched namespace/pattern.

For `throughput` control, both new and old metrics might be dropped, depending their arriving order.

## Special values

The control values can be set to `max`/`maximum`, which is set to max int64 (9,223,372,036,854,775,807), and it practically means no limit.

If the config is not set or set to empty ``, then no quota and usage metrics will be produced.

## Stat Metrics

When using quota, go-carbon will also generate usage, quota, and throttling metrics for each matched patterns.

```
carbon.agents.{host}.quota.metrics.sys
carbon.agents.{host}.usage.metrics.app
carbon.agents.{host}.throttle.app
...
```

The `carbon.agents.{host}` is from config `common.graph-prefix`.

We can also define a prefix for the generated path, using `stat-metric-prefix` for each matched patterns.

## How to enable Quotas

For go-carbon.conf:

```ini
[whisper]
quotas-file = "/etc/go-carbon/storage-quotas.conf"

[carbonserver]
scan-frequency = "2h"
trie-index = true
concurrent-index = true
realtime-index = 65536
quota-usage-report-frequency = "1m"
```

Quota config example:

```ini
# This control all the namespaces under root
[*]
metrics       =       3,000,000
logical-size  = 250,000,000,000
physical-size =  25,000,000,000
# max means practically no limit
data-points   =             max
throughput    =             max
stat-metric-prefix = "level1."

[sys.app.*]
metrics       =         3,000,000
logical-size  = 1,500,000,000,000
physical-size =   250,000,000,000
data-points   =   130,000,000,000
stat-metric-prefix = "level2."

# This controls the root/global limits
[/]
namespaces    =                20
metrics       =        10,000,000
logical-size  = 2,500,000,000,000
physical-size = 2,500,000,000,000
data-points   =   200,000,000,000
dropping-policy = new
```

## Reloading rules

To reload changed rules automatically, configure a polling interval:

```toml
[whisper]
quotas-file = "/etc/go-carbon/storage-quotas.conf"
quotas-reload-interval = "1m"
```

The default `"0s"` disables polling; negative intervals are rejected. SIGHUP can
change the interval or quota file path. Polling only reloads quotas, leaving the
persister, collector, cache, and listeners running. Unchanged rules are skipped.
Read or validation failures keep the last accepted rules and are retried on the
next poll. Replace the file atomically to avoid reading a partially written file;
an empty file intentionally clears all rules.

Once quota support is enabled at startup, send `SIGHUP` to reload the quota file
without restarting go-carbon. The existing index and listeners stay available.
Rules are applied by the index updater; the log message `quota rules reloaded`
confirms application. Changes do not reset throughput consumption for namespaces
that remain configured. Removing a rule removes its limits, and an empty quota
file clears all rules. Invalid files or glob patterns retain the previous rules.
Newly matched namespaces use their existing indexed storage usage immediately.
Rule application does not wait for a disk scan. Quota/usage metrics still follow
`carbonserver.quota-usage-report-frequency`, independently of the polling interval.

Enabling quota support for the first time, or changing
`carbonserver.quota-usage-report-frequency`, still requires a restart. A reload
signal received during startup is queued until recovery finishes.

## Trimming namespaces that already exceed their metric cap

For `pebble-chunk` stores, a rule can opt into removing stored names until the
matched namespace reaches its `metrics` cap:

```ini
[applications.example]
metrics = 50,000
over-quota = delete-newest
stat-metric-prefix = "cleanup."
```

`over-quota` accepts `none` (the default) or `delete-newest`. The latter requires
a positive `metrics` value and implies `dropping-policy = new`; explicitly
combining it with `dropping-policy = none` is an error. Ordinary ingestion of
unknown names is therefore blocked while at or above the cap, including during
dry-run. Rules without this option keep their existing behavior. A glob such as
`[applications.*]` gives **each matched namespace** its own cap; the last rule
matching the same node wins, as with ordinary quotas. A child without the
option still belongs to an opted-in ancestor's subtree; it is not an exclusion
from that ancestor's cap.

There is also a global opt-in in `go-carbon.conf`:

```toml
[whisper]
quota-trim-enabled = true          # default false
quota-trim-dry-run = true          # default true; set false to delete
store-expiration-check-interval = "1h"
store-expiration-scan-rate = 100000
store-expiration-delete-rate = 100
store-expiration-delete-workers = 8
store-expiration-max-deletes = 10000
```

This requires an enabled persister, `quotas-file`, and enabled carbonserver with
`trie-index`, `concurrent-index`, and a positive `realtime-index`. File storage
and rollback mirroring are rejected. File migration must have completed so
discovery is owned by the store catalog. The global flags reload via SIGHUP. Rules,
including adding/removing `over-quota`, also reload through the quota file poller.
A running pass finishes its captured rule snapshot; removing the option takes
effect on the next pass. SIGHUP joins the old worker before replacing it. Disabling
`quota-trim-enabled` through SIGHUP cancels and joins the active worker.

### Selection and safety

The worker refreshes the index and materializes the same quota assignments that
enforce ingestion. It counts committed catalog names rather than trusting the
periodically refreshed usage counter. When there is excess, a second paced
catalog pass retains at most `store-expiration-max-deletes` candidates across
bounded heaps. Independent namespace trees split that limit in proportion to
their committed excess, rounded with largest remainders (namespace order breaks
ties). Each tree selects its highest catalog IDs within that allocation. An
opted-in descendant shares its outermost opted-in ancestor's allocation, avoiding
double counting overlapping excess. Selection still reserves every matching
cap; protected descendants or concurrent catalog changes can leave some slots
unused, and skipped slots are not redistributed during the pass. Small shares
can round to zero; this is proportional allocation, not round-robin scheduling. Memory is bounded by the matched namespace count plus this candidate
limit; catalog scans themselves visit the entire catalog. Names outside opted-in
namespaces are ignored. The pass reserves each overlapping opted-in namespace's
excess before dispatch, so trimming cannot take those namespaces below their
caps. A child at its cap is protected even if its parent remains over quota;
contradictory parent/child caps may therefore prevent convergence.

There is **no durable metric creation timestamp**. The trie has `firstSeenAt`
(persisted for file indexes by file-list cache v2), but chunk catalog indexing
passes zero: mutable entries use index-observation time and mapped entries can
leave it unset. Rebuilding the index does not preserve a reliable creation time. Store `LastUpdate` is write activity, not creation, and oldest retained points
are affected by retention and backfill. Trimming instead orders by **descending
store catalog ID**, then descending name for a deterministic tie break. Parallel
batch completion can differ from selection order. Ordinary
creation allocates increasing IDs and ordinary writes preserve them. This is
local allocation order, not the original age of the data: migration, import,
restore, native ID reservations/replacement, and deleting/recreating a name can
make an old logical metric look new. Review dry-run before enabling deletion,
especially after migration or restore.

Candidates enter the existing metric expiration executor: the same partitioned
delete workers, batches of at most 32, one delete-rate budget, cache/in-flight
write checks, compaction pressure guards, store identity/generation/revision
fence, atomic deletion receipts, tombstone flush, and index refresh notification.
Each quota receipt identifies the first matching opted-in namespace policy
(in sorted namespace order) separately from TTL
expiration evidence; they do not grant a TTL-expiration exemption to replication.
Replication and administrative imports can reintroduce removed names, which
subsequent passes will consider again.
Deleted catalog names leave discovery and quota usage when the existing index
refresher runs. Already cached query responses retain their normal cache TTL.

A new committed write invalidates the candidate. A point arriving after the
last cache check may race deletion; as for expiration, the cache retains the
point and the persister requeues/recreates a missing metric. Such recreation can
temporarily restore an excess name. Pending writes, conflicts, or pressure cause
skips; the next pass retries rather than deleting older replacements in the same
pass. Independent administrative deletion and TTL expiration retain their own
semantics; the metric cap is not a minimum retention guarantee for those paths.

Trimming runs first, followed by any configured TTL expiration, sharing the
configured maximum successful deletions per pass. Both phases use the expiration
scan/delete rate and pressure settings. With the defaults, trimming attempts at
most 10,000 names per pass and paces deletion at 100 names/s across all workers.
Dry-run logs and counts the bounded selection without initializing activity,
writing receipts, or deleting names. `quota-trim-dry-run` is independent of
`store-expiration-dry-run`, which controls only TTL expiration. Enabling trimming
does not enable a TTL policy.

### Monitoring and rollback

Under `common.graph-prefix`, the collector emits interval counters:

```text
quota.trim.candidates
quota.trim.deleted
quota.trim.skipped
quota.trim.errors
quota.trim.candidates.cleanup.applications-example
quota.trim.deleted.cleanup.applications-example
quota.trim.skipped.cleanup.applications-example
```

The suffix follows quota stat naming: `stat-metric-prefix` followed by the
materialized namespace with dots replaced by hyphens (root uses `root`; long
names use the same shortening/hash rule as quota metrics). Overlapping rules
count a candidate under every affected namespace; global counters count each
name once. Candidates are the planned names for that pass, capped by the pass
limit. Skipped means planned but not deleted, including every dry-run candidate.
A single `quota trim plan` log event, emitted before execution, includes every
materialized namespace's full excess, cap, rule, candidate count and up to ten
sample names, including namespaces assigned zero candidates. Keeping the plan
in one event prevents message-based log sampling from silently hiding later
namespaces. The existing per-namespace outcome events may still be sampled.

The read-only `/admin/info?scopes=quotaTrim` response contains the last published
plan under `quotaTrim.plan`, including `plannedAt`, `dryRun`, `maxDeletes`, all
namespace summaries, and the exact selected `names` with catalog IDs. It reads
an immutable in-memory snapshot; polling does not scan or mutate the catalog.
`plan` is null until the first successful planning phase. Interval stats scrapes
do not clear it. The snapshot survives policy reloads until another plan is
published, so check its timestamp, dry-run flag and captured rules/caps against
the intended configuration. It describes selection, not deletion outcomes.
Names appear once globally, including when several overlapping namespaces count
them. Group the exact candidates per namespace with:

```sh
curl -fsS 'http://127.0.0.1:8080/admin/info?scopes=quotaTrim' > quota-trim-plan.json
jq '.quotaTrim.plan as $p | $p.namespaces[] |
  .namespace as $ns |
  {namespace, cap, excess, candidates, names: [ $p.names[] |
    select($ns == "/" or (.name | startswith($ns + "."))) ]}' quota-trim-plan.json
```

The admin snapshot avoids log transport size limits and retains only the bounded
candidate set. Neither endpoint nor log lists all excess names beyond this pass's
budget. Protect this admin surface as for the existing config/cache diagnostics.
Errors are logged at error level; watch `quota.trim.errors`,
`storage.expiration.errors`, `persister.write_failed_points_total` (no increase),
and index refresh errors.

Start on one store with dry-run, save the exact admin plan, inspect every
namespace's cap/excess/count and candidate names/IDs, and review the age-order
caveats. Dry-run and live mode call the same planner; on an unchanged catalog and
policy their candidate sets are identical. Switching mode starts a new plan,
not execution of a frozen approval: arrivals, imports, deletion or policy changes
can change that set, and cache/revision/pressure checks can make live mode skip
selected names. A previously saved plan is not a guarantee of the next pass.
Then set `quota-trim-dry-run = false` and SIGHUP. Watch deleted counts and
`usage.metrics.<suffix>` converge toward `quota.metrics.<suffix>`, with write
errors remaining zero. To stop, set `quota-trim-enabled = false` and SIGHUP; remove
`over-quota` to remove the policy as well. Keep `dropping-policy = new` explicitly
if new-name admission must remain blocked. Stopping does not restore deleted
data; recovery requires a backup or another replica.

Caps are **per store**: use the namespace quota divided by its replica factor for
each host's cap. If a namespace is stored in two clusters, configure a cap in each
cluster; this feature does not coordinate a cross-cluster quota.
