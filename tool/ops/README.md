# Operator tools

These Python 3.9+ commands use the standard library. Run them from a checkout;
keep this directory together because commands import `common.py` and `queries.py`.
They complement the compiled tools under `tool/`. They do not import go-carbon
packages, need a Go build, or discover a fleet from a naming convention.

All commands have `--help`. JSON reports retain full target identifiers and use
`null` for unavailable measurements. An empty result is not evidence of health.
No command contacts a host through SSH itself. Run read-only HTTP reports from a
controller with access to the configured endpoints. For local commands, copy this
directory to the host with ordinary `scp`, then invoke it through ordinary `ssh`
or a local terminal. For example:

```sh
scp -r tool/ops carbon-a.example:/var/tmp/go-carbon-ops
ssh carbon-a.example python3 /var/tmp/go-carbon-ops/status.py \
  --target http://127.0.0.1:8080 --require-shared
```

Use your normal SSH configuration and authentication. No particular transport,
inventory service, private library, or infrastructure provider is required.

## Feature prerequisites

The identifiers below name feature slices in the storage submission stack, rather
than released go-carbon versions or public pull-request numbers. Verify the named
capabilities in the installed build; a version number alone is insufficient while
that stack is being reviewed. The tools can be reviewed independently of the Go
changes. Their runtime requirements are:

| Tool or report | Required server capability / stack slice |
| --- | --- |
| `prometheus.py health`, `parity` | Standard process/HTTP metrics; U11 incremental migration status for safe peer eligibility |
| `prometheus.py migration` | U11 incremental ownership, U12 deferred reclaim counters |
| `prometheus.py cost` | U29 persister stage timing and backend point counters |
| `prometheus.py gc` | Go runtime metrics; U16 metadata cache and U15 cache accounting for those columns |
| `prometheus.py input-log` | U21 continuous input log and its optional sync histogram |
| `prometheus.py io` | U08 Pebble I/O budgets/metrics; U12 ingestion counters |
| `capacity.py` | No server API; timestamped report rows, process metrics and optional node exporter |
| `status.py` | U10 storage admin scope; U11 incremental receipt; U12 reclaim status; U28 for the separate removal tool |
| `canary.py` | Linux, systemd, `/proc`, `-check-config`, U10/U11 Pebble write counters; U21 for the default input-log guard counter |
| `cleanup.py` | Linux `/proc`, systemd and filesystem permissions; no new server API |
| `nvme_smart.py` | `smartctl` with JSON and NVMe support; optional node-exporter textfile collector; no new server API |
| `expiration.py` | U23 ordered expiration rules with grace and exclusions |

## Fleet health, migration, input logging and cost

`prometheus.py` uses the ordinary Prometheus-compatible `/api/v1/query` and
`/api/v1/query_range` HTTP APIs. The endpoint may include a prefix, such as
`https://metrics.example/prometheus`. Set `PROMETHEUS_URL` or pass `--endpoint`.
For bearer authentication set `GO_CARBON_OPS_TOKEN`, or select another environment
variable with `--token-env`. `--tenant` or `PROMETHEUS_TENANT_ID` supplies the
optional `X-Scope-OrgID` header. TLS certificate checks remain enabled. Redirects
are refused so credentials cannot follow a redirected endpoint.

```sh
python3 tool/ops/prometheus.py --report health \
  --endpoint https://metrics.example/prometheus --selector 'job="go-carbon"'
python3 tool/ops/prometheus.py --report migration --targets-file targets.txt
python3 tool/ops/prometheus.py --report cost --target carbon-a.example:7007
python3 tool/ops/prometheus.py --report gc --host-label host --selector 'job="go-carbon"'
python3 tool/ops/prometheus.py --report input-log --window 1h \
  --start 1793232000 --end 1793235600 --step 60
python3 tool/ops/prometheus.py --report io --print-queries
```

Target files contain exact label values, one per line; blank lines and lines
starting with `#` are ignored. The default target label is `instance`; change it
with `--host-label` to match your scrape/relabel configuration. `--selector`
contains label matchers without braces. Narrow it to the relevant job: selecting
multiple exporters with the same target label may mix unrelated measurements.
A custom `--query NAME=PROMQL` adds or replaces a column and must return at most
one series per target and timestamp, retaining the chosen target label.
`--at` selects an instant evaluation time; all columns use the same instant.
Ranges require Unix-second `--start` and `--end`; `--step` is in seconds.

Missing columns and non-finite values are `null`; duplicate targets, query errors
and partial-result warnings stop the report. No denominator is clamped to make an
idle workload appear healthy. Counters use `rate`/`increase`, which account for
resets. Migration totals cover the current process, not all durable ownership
since conversion began. `status.py` shows the complete receipt when interpreting
migration phase and cleanup state.

Cost columns measure process CPU seconds per persisted point (Pebble by default), points per
update, and elapsed stage seconds per point. Use `--backend whisper` to select
original-file point/update counters. Stage time covers both backends during mixed
conversion; compare completed stores with one active backend. Process CPU includes reads, GC,
compaction and conversion; stage time sums worker elapsed time, so it is not a
subsystem CPU attribution. Compare equivalent traffic over the same window.
The GC report separates live heap, allocation rate, runtime GC CPU and store/cache
memory; RSS includes mapped storage and other memory outside the live Go heap.
The input-log report shows unlogged accepted points separately from write errors:
zero write errors does not prove all accepted points were logged. Optional runtime
or histogram metrics may remain unavailable in some builds/configurations.

For parity, provide an explicit JSON mapping rather than deriving replicas from
hostnames:

```json
{"carbon-a.example:7007": ["carbon-b.example:7007", "carbon-c.example:7007"]}
```

```sh
python3 tool/ops/prometheus.py --report parity --peers peers.json \
  --selector 'job="go-carbon"' --window 15m --max-ratio 1.25
```

Candidates and peers are selected automatically from that file. A comparison is
unknown unless every named peer is up, reports migration status, and has no
in-flight conversion during the window. Candidates must meet the same conditions.
A missing peer or sample never becomes zero. The output compares candidate p99,
CPU and RSS with the arithmetic mean of peer measurements; an average of peer
p99s is a comparison baseline, not a combined traffic p99. A verdict is diagnostic
and does not change services or storage.

## Capacity floors

`capacity.py` reads `prometheus.py` JSON locally and estimates hosts from the sum
of per-target resource quantiles, divided by per-host capacity and the permitted
utilization. It adds the requested spare count to the largest resource floor.
This is not the percentile of aggregate fleet usage. No shrink or rebalance is
performed; replication, ring ownership, traffic skew, failover and conversion
headroom still constrain a deployable layout.

```sh
python3 tool/ops/prometheus.py --report capacity --targets-file targets.txt \
  --start 1793232000 --end 1793318400 --step 300 > capacity-samples.json
python3 tool/ops/capacity.py capacity-samples.json --cpu-cores 16 \
  --memory-bytes 68719476736 --disk-bytes 4000000000000 \
  --quantile 0.95 --utilization 0.7 --spares 1
```

The default memory measurement is process RSS, not total host working set.
Default disk usage uses node-exporter filesystem size minus available bytes at
`--mountpoint` (default `/var/lib/carbon`), including reserved space. Node metrics
must share the report target label with process metrics; typically this requires
relabeling or a custom query. Override columns with `--query` when exporters have
different labels, or to use host memory/device/network measurements. Optional
`read_bytes_per_second`, `write_bytes_per_second` and `network_bytes_per_second`
columns have matching capacity flags. Use one common time grid. Missing targets,
resource samples, duplicate timestamps and incomplete ranges are refused.

## Binary canary with rollback guard

`canary.py` runs on the host. It validates a preinstalled, immutable candidate
binary by SHA256 and `-check-config`, prepares a service drop-in replacement, and
launches a detached systemd guard. The guard performs the swap/restart and checks
that the active MainPID executes the candidate under `/proc`. It restores the
previous drop-in and restarts the service on an error burst, missing required
metrics, a lost process/readiness check, or startup timeout. It also rolls back
when the first candidate sample already exceeds the error budget.

```sh
sudo python3 tool/ops/canary.py run \
  --binary /opt/go-carbon/bin/candidate --old-binary /opt/go-carbon/bin/previous \
  --sha256 YOUR_CANDIDATE_SHA256 --config /etc/go-carbon.conf \
  --drop-in /etc/systemd/system/go-carbon.service.d/90-binary.conf \
  --state-dir /var/lib/go-carbon-canary/trial-001
# Inspect the plan; add --apply to schedule the canary.
sudo python3 tool/ops/canary.py show /var/lib/go-carbon-canary/trial-001
sudo journalctl -u go-carbon-binary-canary.service
sudo python3 tool/ops/canary.py rollback /var/lib/go-carbon-canary/trial-001
```

Create the state parent first, owned by the service administrator. Each run needs
a new state directory. Keep this source directory and both binaries in place
through the guard window. The single `go-carbon-binary-canary.service` unit
serializes trials on one host; clear its failed state after inspecting a failure.
Do not stop/kill the guard to cancel a trial: use `rollback` so the saved drop-in
is restored. A host reboot or forced guard termination requires manual inspection
of the state and saved files; the transient unit does not survive reboot.

The drop-in must have one simple absolute `ExecStart=/path/to/binary ...`, with
`-config /path/to/config` matching `--config`. Quoted/escaped executable paths,
line continuation and systemd executable expansion are intentionally unsupported.
Check that this drop-in controls the effective command; MainPID verification
catches an override that keeps another binary running. The command does not edit
server configuration or conversion controls. Preserve the old binary, and first
establish that it can read the candidate's on-disk format and durable ownership.
A binary rollback cannot reverse an incompatible storage migration.

Defaults are a three-hour guard, checks every 120 seconds, and rollback on more
than 20 new errors per check. `--duration`, `--interval`, `--max-errors`,
`--startup-timeout`, `--restart-timeout` and `--http-timeout` are seconds/count
controls. The default required counters are
`persister_pebble_write_failures_total` and `input_log_write_errors_total`.
For input logging disabled, explicitly replace the defaults with
`--error-counter persister_pebble_write_failures_total`; repeat the flag for any
additional counters. Labeled series are compared independently, including resets,
so a reset in one series cannot conceal another's increase. Series disappearance
or churn fails the guard. Unlogged-point counts are diagnosed separately with the
input-log report; add that counter to the guard if that is your rollback policy.

The metrics base URL defaults to `http://127.0.0.1:7007`; only loopback addresses
are accepted. `--token-file` supplies an optional bearer token to the detached
worker without embedding the secret in state or process arguments. Restrict that
file to the administrator. Dry-run reads local files only. Applying runs local
systemd and the candidate's config check; review any external integration enabled
by the supplied config. External edits to the drop-in or previous binary cause
rollback to refuse overwriting them and leave `rollback-required` in the state
file. The saved original can then be reviewed and restored manually.

## Live status and prune/reclaim verification

`status.py` reads `/admin/info?scopes=storage,cache` for explicit admin base URLs.
It uses the same HTTP authentication options as `prometheus.py` and preserves the
complete receipt/engine response. Keep admin endpoints private and use your normal
ACLs. This command never requests mutation scopes.

```sh
python3 tool/ops/status.py --targets-file admin-targets.txt --require-shared
python3 tool/ops/status.py --target http://127.0.0.1:8080 \
  --require-shared --source-root /var/lib/carbon/whisper --max-entries 100000
```

`--require-shared` exits nonzero unless every receipt explicitly reports
`phase=shared`, `in_flight=0`, `skipped=0` and no error. A missing receipt or required
field fails the check. This is readiness to inspect/prune, not proof that pruning
has finished. `--source-root` is a local, read-only inventory of empty directories,
Whisper/OOO files, lock files, other files and symlinks. It refuses nested
filesystems and marks a walk truncated at `--max-entries`. Do not interpret a
truncated count as a complete scan. Compare complete inventories and filesystem
inode/free-space measurements before and after pruning. Check migration reclaim
queue/rate columns with the migration report. Freed filesystem bytes may reach
an SSD only after discard; see the [storage operations guide](../../doc/pebble-storage/operations.md).

For removal use the compiled `tool/store-prune-empty-dirs` tool from U28. It pins
roots and enforces ownership, locking and no-follow rules. This read-only wrapper
does not replace its mutation safeguards or turn an absence of sampled files into
permission to remove a live data tree.

## Stale artifact quarantine

`cleanup.py` inventories explicit top-level names in a dedicated inactive artifact
directory. Default behavior only prints a plan. On `--apply`, it requires the
local service to stay stopped, checks all Linux process cwd/exe/fd/cmdline/maps
references, rechecks ownership/metadata, and atomically moves each candidate to a
new quarantine directory on the same filesystem. It never recursively deletes
files. A manifest records both planned and completed renames and permits recovery
if the command stops midway. Inspect the actual source and destination as well as
the manifest after an interruption between rename and manifest update.

```sh
python3 tool/ops/cleanup.py --root /var/tmp/go-carbon-artifacts \
  --candidate old-binary --candidate old-fixture --min-age-days 7
sudo python3 tool/ops/cleanup.py --root /var/tmp/go-carbon-artifacts \
  --candidates-file artifacts.txt --owner-uid 1001 \
  --quarantine /var/tmp/go-carbon-quarantine --apply
```

Candidates are literal top-level names, not globs or paths. The default owner is
the invoking UID, minimum age is seven days, and maximum inventory is 100,000
entries per candidate. `--min-bytes`, `--min-age-days`, `--max-entries`,
`--owner-uid` and `--service` override those controls. Every descendant must have
the expected owner and age. Symlinks, hard links, special files and nested
filesystems are refused. Permission errors during reference checks fail closed;
apply normally requires root. Quarantine needs the same filesystem so rename is
atomic and does not double storage usage. Moving data on that filesystem does
not reclaim its blocks; retain it until reviewed, then remove it through your
normal artifact-retention process.

Use only trusted, quiescent artifact directories. Stop other writers and job
schedulers as well as go-carbon. Metadata/reference rechecks are not a lock on
arbitrary external processes. Never select a live Pebble store, source Whisper
tree, input log, checkpoint or configured archive; this command cannot infer
those roles from a directory name. For source-directory pruning use U28 instead.

## NVMe SMART collector

`nvme_smart.py` runs locally with `smartctl -a -j` for each supplied device. It emits
NVMe health values, per-device scrape success, and last-success timestamps in
Prometheus text format. Write an atomic `.prom` file into an existing node-exporter
textfile directory, or omit `--output` for stdout. There is no network output or
fleet discovery. Run periodically through your scheduler and alert on both scrape
failures and stale textfile timestamps.

```sh
sudo python3 tool/ops/nvme_smart.py --device /dev/nvme0 --device /dev/nvme1 \
  --timeout 30 --output /var/lib/node_exporter/textfile_collector/go_carbon_nvme.prom
```

Requires smartmontools JSON/NVMe support and permission to read the devices.
Select controllers/namespaces explicitly to avoid double counting one device.
Counters retain SMART data units; one data unit represents 1,000 units of 512
bytes, and rounding/firmware reporting still apply. Temperature is Celsius.
SMART exit bits 0–2 indicate failed retrieval; higher bits describe health problems
and their measurements are retained. A failed device emits `scrape_success=0`
and no current health values, replacing stale values in the output file. Command
failure exits nonzero even when other devices succeed. SMART reads do not modify
drive settings. Use the vendor's interpretation for endurance and error fields.

## Expiration policy from find rules

`expiration.py` turns reviewed `find` commands extracted from cron entries into
native ordered INI policy on stdout. It does not execute commands, install a
policy, restart a service, or enable deletion. Its input is a JSON array:

```json
[
  {"name": "short_lived_jobs", "find": "find /srv/whisper/jobs -type f -name '*.wsp' -mtime +7 -delete"},
  {"name": "default", "find": "find /srv/whisper -type f -name '*.wsp' -mtime +30 -delete"}
]
```

```sh
python3 tool/ops/expiration.py find-rules.json --data-root /srv/whisper \
  > storage-expiration.conf
```

Supported predicates are `-type f`, `-name '*.wsp'`, one `-mtime +N` or
`-mmin +N`, optional absolute `-path`/`-wholename`, optional negated path predicates,
and `-delete`. Literal traversal roots and path predicates must stay under
`--data-root`, with predicates fenced by the traversal root. `*` and `?` cross path
separators just as in `find -path`. Character classes, literal dots in path
components, alternate expressions, shell wrappers/redirection, root globs,
`-exec`, cron prefixes and other predicates are refused. Unsupported rules must
be translated/reviewed explicitly rather than silently broadened.

The generated TTL includes the extra whole-unit rounding from `+N`: `-mtime +7`
becomes `expiration=168h`, `grace=24h`. `+0` becomes a positive one-unit TTL because
native `expiration=0s` means keep forever. Rules are sorted by effective TTL so
independent find jobs' earliest applicable deletion wins under native first-match
semantics. Exclusions remain per rule. Cron scheduling and filesystem mtime are
not identical to native last-write activity; compare real eligibility before
enabling anything. Explicit exemptions/deadlines can be added by hand using
`expiration=0s` and `until`; place them before generated rules.

Start with `[whisper] store-expiration-dry-run=true` and the U23 pressure/rate/delete
caps. Validate the combined server config with the matching binary's
`-check-config`, inspect candidate counts against the original rules, and verify
historical activity preservation/backfill before enabling deletes. An unmatched
name follows the configured global fallback; use `store-expiration="0s"` to keep
unmatched metrics. Source-file crons do not expire converted shared-store data.

## Offline checks

```sh
python3 -m unittest discover -s tool/ops -p 'test_*.py' -v
```

Tests use temporary small fixtures, fake HTTP responses, fake systemd commands,
fake clocks and fake device JSON. They require no hosts, credentials, sockets or
network and do not invoke the destructive paths of external utilities.
