# Store controller

`storectl sync` discovers, copies, verifies and fills metric histories in one
resumable command. Build with
`GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build ./tool/storectl`.
`storectl --version` reports the release build revision (or the Go VCS stamp).
The owning donor and receiver daemons must also contain the native engine.

## Controller progress metrics

Every `storectl` command with a `--state` sends Graphite plaintext to `127.0.0.1:2003`
by default. Set `STORECTL_GRAPHITE_ADDR=receiver:2003` when the controller
runs elsewhere. The receiver must accept and route this namespace. No daemon
or scrape configuration change is required when a local receiver already exists.

Series are `tools.storectl.<runner>.<run>.<command>.<phase>.<metric>`:
`done`, `total`, `names_per_second`, `active` and `heartbeat` (Unix seconds).
Runner punctuation becomes underscores; the run is a stable 12-character
SHA256 prefix of the absolute, cleaned state path. Resumes and client/binary
replacements reuse the same series; separate state paths distinguish concurrent runs.
PID, start time and stream settings do not enter the identity. Snapshots are emitted at
startup, every 15s and on exit. A phase transition clears the previous phase’s activity and rate.
`active=0` records termination, not successful
verification. Consult the command exit status and summary for success/residuals.
Disconnected receivers produce a stderr warning and bounded retries at the next
snapshot; telemetry never changes transfer admission or the command result.

Sync uses the exact snapshot also written to progress JSON, including
`done` and `total` for the current phase/batch. Durable cumulative counters and
the rate retain their existing meanings. Nested copy snapshots cannot replace
the parent sync snapshot. Commands without a progress file emit only lifecycle
heartbeats. Phase appears in the series name.

These metrics require installing this tool version and launching a new process.
An already running binary cannot start exporting them, and history is not backfilled.


## One end-to-end command (`sync`)


`sync` discovers the source/target union, copies missing names, verifies coverage,
fills gaps and reverifies in one resumable run. Existing finite target values win.
There is no plan directory or per-name byte estimate. Names absent on the target
go directly to transfer after batched source retention checks; coverage comparison
runs after transfer, when both stores hold the name.

```sh
export BUCKYD_TOKEN=...  # read+update JWT, supplied by the operator
storectl sync \
  --source http://donor-a:4242 --source http://donor-b:4242 \
  --to http://target:4242 --state ./block-state --streams 0 \
  --window closed:1794592020:1794592680 \
  --window shard:1794595620:1794597120 \
  --window dump:1794600000:1794603600
```

All three gap windows are selected, copied, verified and filled in this invocation;
no separate gap commands or shell receipt chain is needed. Repeating this exact
command resumes the durable discovery cursor, verification batches and native
receipts. Windows are half-open `[FROM, UNTIL)` in Unix seconds. Keep their order,
`--verify-batch` and `--settle` unchanged on resume.

- Full-history copy/fill is enabled by default, including runs with windows. Every
  selected window is verified for every name; additional full histories are sampled
  (`--sample-every 1` by default, 1=all, 0=none). Set `--no-full-history` with windows to
  restrict all writes to those windows using the native target-wins combiner.
  `--full-history` remains an accepted compatibility spelling.
- `--no-copy` makes verification read-only: missing coverage stays residual.
  `--no-verify` stops after discovery/copy and does not prove readiness.
- `--streams` is the total per-host transfer budget. Zero resolves advertised
  source/target capacity (the smallest advertised per-host cap for end-to-end sync). Verify and copy are serial phases, so each can use the
  entire budget; native window copies currently use one serial range stream.
  `effective_config`, `sync_start` and `progress.json.effective` state the actual
  engine, stream allocation, batch limits and window/full-history scope.
- `--copy-streams` and `--verify-streams` no longer create independent pools: a different positive value is
  rejected; an equal value or zero warns to remove it. In native window mode,
  `--copy-streams`, `--max-batch-bytes` and other metric-engine-only settings are
  rejected. Explicit inactive `--sample-every` warns. `--transport-retries` has
  been removed and is an unknown flag.
- Use repeated/comma-separated `--source URL`; `--from URL` remains a compatibility
  alias in both spellings. This is distinct from legacy `run --from TIME`.
- `--names FILE` means UTF-8 text, one metric name per line, byte-sorted and unique
  (`LC_ALL=C sort -u`), **not a JSON array**. It restricts discovery while retaining
  actual donor membership through bounded existence probes. It also retains
  explicitly requested names absent from every catalog, so verification can
  classify them. The file hash binds the sync journal. Inventories stream from
  disk, including very large lists; unrelated catalogs are not scanned.
- Names whose every archive retains at most three days are excluded from sync
  unless `--include-short-retention` is set. `--settle 10m` excludes newest points.
  These exclusions are part of the requested coverage, not a READY guarantee.

### Existing copy journals

Plain sync adopts an existing copy journal for the same source/target and scope:

```sh
storectl sync --source http://donor:4242 --to http://target:4242 \
  --names sorted-names.txt --state copy.jsonl --streams 0
```

The original importer, real-byte batching, retry handling and journal identity are
unchanged. Completed donor/name ranges resume without transfer, existing target
points win, and unfinished ranges use the same bulk copy engine and stream budget.
After copying, verification/fill continues in `FILE.sync/`; copy progress remains
in `FILE.progress.json` and sync progress is in `FILE.sync/progress.json`.
`--no-verify` stops after the resumed copy, and `--no-copy` skips all writes,
including the legacy copy phase. No phase selector is needed.

A new `.jsonl` state path also uses this journal format. Other new runs use a
state directory directly. Windows may accompany a legacy journal with full
history enabled; `--no-full-history` requires a directory because the legacy
copy phase transfers complete histories. Source/target and scope must match the
old journal; a mismatched identity fails rather than reusing unrelated ranges.
`--max-batch-bytes` defaults to 16MiB, capped by target `max_body_bytes`; a single
name may use the full target cap. `--max-batch-names` defaults to8192.

Window sync defaults are one contract for every build of this code. Previously,
master copied windows only unless `--full-history` was set, while older forks
verified windows and copied complete histories. The shared default now includes
full history, following the all-phases-by-default contract. Merge this CLI and its
window dispatch into older branches together; use `--no-full-history` to retain
an existing window-only journal, and `--full-history` (or no scope flag) for an
existing full-history journal. Scope is bound to the journal identity, so an
incompatible resume fails rather than silently widening old work.

## Resize / merge baseline

After the campaign owner pauses the existing controller, retains its journal and
approves the concrete binary/plan through the campaign's PLAN-READY/WRITE-OK flow:

```sh
storectl run --plan /path/to/existing-plan --state /path/to/existing-journal \
  --engine bulk --bulk-only --apply
storectl status --plan /path/to/existing-plan --state /path/to/existing-journal
```

Do not create another journal or change the operation identity. The existing
journal lock excludes overlapping controllers. Existing completed ranges are
skipped; an incomplete native intent resolves its daemon receipt before replay.
`--max-batches 1` bounds a canary's scope. The default concurrency comes from
receiver CPUs and the existing independent plan streams; `--streams` can specify
concurrency explicitly. Batch bytes/name pacing, transfer slots, tuner, debt,
cache-age and read-p99 guards do not govern native work.
Pending-intent retries and resolution of a durable lost acknowledgement each
consume that same range budget; a max1 retry cannot also import a new gap.

A pinned donor snapshot exports native stored blocks through the existing v2
framing. Queued/recovery source names and revisions changed since the pin are
conservatively deferred. No cache flush, Whisper conversion or expiration gate
runs on this path. The receiver reserves IDs, writes namespace/time-bucket SSTs,
ingests hidden IDs, then atomically publishes only names still absent under the
normal creation lock. Existing/live-created names remain deferred. The atomic
range receipt binds the plan/name digest, copied bitmap, reserved ID mapping,
stream digest and SST bounds/size/entry identities. Ordinary live writes continue.

Native sessions become ready when restart recovery finishes loading, without
waiting for saved-history writeout. Receiver presence includes restored,
queued and in-flight cache names as well as the catalog, checked both before
export and again under the creation lock at publication. These names remain
for the existing importer/phase 2. Whole-store checkpoint and expiration
readiness still wait for their original recovery boundary.

ID reservation and catalog/receipt publication release the creation lock after
Pebble applies the atomic batch, then wait for WAL sync before acknowledging.
The per-range lock keeps receipt queries and duplicate imports behind that
durability wait. `PublishHoldSeconds` excludes this sync;
`PublishSyncSeconds` reports the separate wait.

Durable native receipts, not client responses alone, authorize completion. A lost
acknowledgement is resolved by receipt query without importing history twice.
Unpublished reserved IDs and private SST directories recover on daemon open.
Write/ingest errors, integrity failures, crashes and less than 10% free disk stop
native work. Native pins expire after inactivity and are released on controller
exit. No store lifecycle, routing or configuration action is performed by this CLI.

`--bulk-only` leaves deferred work for a reviewed handoff. Without it, the same
controller switches to the existing metric importer after the native baseline.
Resuming `--engine metric` also handles only unfinished deferred ordinals. An old
controller must not resume a mixed journal: it fails on the empty-proof native
intent; update the controller/status reader first. `completed_names` and
`native_deferred_names` are the native-aware status counters. Counting entire
`verified` ranges as completed is incorrect for mixed journals.

### Range size, explicit name lists and idle transfers

The daemon rejects native ranges above 4096 names (`chunkstore.NativeMaxRangeNames`).
The controller sends every native plan stream as consecutive sub-ranges of at
most 4096 names, so plans with larger segments (the old 100,000-name default,
or `config --mode plan --segment-metrics 10000`) also work. Resume skips the
sub-ranges that already completed. `storectl plan --segment-metrics N` sets
the names per plan stream segment and defaults to 4096.

`storectl plan --names FILE --source URL --target URL --out DIR --apply` plans an
exact name list (one name per line, byte-sorted and unique: `LC_ALL=C sort -u`)
from one buckyd origin to another. The result is the same plan, byte for byte,
that `config --mode plan` writes for a one-member old ring, a one-member new ring
and a `lines` inventory of that file. Use it for gap, deferred and missing-point
fills with `run --engine metric`. `--estimated-bytes` (default 65536) is the
estimate **per name** that the importer batches by.

A spec input's `estimated_bytes` is also per name for `lines`, `buckyd` and
`transfer` inventories, not the inventory total. Planning rejects values above
64 MiB. With an inventory total there (1.2 GB per name in the 13 Nov user/vm
deferred specs), the importer sent one name per transfer. `run` treats a
committed per-name estimate above 128 MiB as unknown. It logs one
`implausible_estimate` error event and batches those rows at 64 KiB per name.
The target's HTTP 413 still splits any batch whose real export exceeds
`--batch-bytes`.

`run --transfer-idle-timeout` (default 2m, 0 disables) bounds each metric-engine
pull, import and verify request. When no request or response bytes move for that
long, the controller aborts the request and logs a `transfer_idle_timeout` event
with `"level":"error"`. A batch of several names splits in half and retries. A
single name gets 3 attempts, then the run aborts and keeps its journal. Verify is
read-only and retries on its own. A native-deferred child is not retried: its
bulk intent stays uncertain and fail-closed, as with any other lost response.
Re-pulling a plain metric task is a target-wins union, so existing target points
still win.

### Metric importer governor thresholds

The metric importer pauses or halves admission when a target's read p99 rises
above:

```
limit = min(--read-p99-ms, max(--read-p99-floor-ms, --read-p99-ratio × baseline))
```

| Flag | Default | Meaning |
| --- | ---: | --- |
| `--read-p99-ms` | 100 | Absolute ceiling (ms); 0 leaves only the relative limit |
| `--read-p99-floor-ms` | 50 | The limit is never below this (ms) |
| `--read-p99-ratio` | 1.5 | Multiple of the pre-copy read p99 baseline (>= 1) |

The baseline is the median of the valid read-p99 samples taken during the
pre-copy observation window (read-isolated samples and samples with fewer than
20 reads are skipped), not the single last reading. A 1.26 ms baseline therefore
gives a 50 ms limit, not 2 ms. Later baselines (after read isolation, or before
whole-store work) use the latest sample. The cache-backlog ceilings
(`--backlog-hard-ratio`, the 90% host-memory estimate), write errors and the
other hard guards still abort the copy. A process restart or counter reset is
not a guard: the host is re-baselined (`rebaseline` event), a host whose helper
or status does not answer is parked (`host_unavailable`, then `host_back`), and
transfers that fail transiently re-send the same batch. `--stall-timeout`
(30m) fails the run only while both hosts of a transfer are up.

Resuming an existing journal: the read-p99 thresholds and the backlog thresholds
(`--backlog-soft-ratio`, `--backlog-hard-ratio`, `--backlog-growth-samples`,
`--cache-bytes-per-point`) are saved in the journal. A resume without these flags
keeps the saved values. A resume that passes one of them overrides the saved
value, raising or lowering it, without a new journal or re-copy. Each override
is printed as a `policy_override` event and a `WARNING` line on stderr, saved in
the journal before any transfer is admitted, and listed with the effective
policy under `backlog_policy`, `read_p99_policy` and `policy_overrides` in
`storectl status`. `--backlog-baseline-window` is fixed: it describes how the
saved baseline was learned. Journals written before the read policy was saved
adopt this run's read flags (default floor 50 ms) and record the replaced 2 ms
floor as an override.

```sh
storectl run --engine metric --plan SAME_PLAN --state SAME_STATE \
  --health-map SAME_HEALTH_MAP --apply --read-p99-floor-ms 80 --backlog-soft-ratio 4
```

Phase 1 is copy-only. Retirement and a writer-drained final delta reconciliation
still require the campaign's separate existing approval. Overlap combining is
currently the reviewed importer and retains its existing admission policy;
phase 2 will replace that combine stage with exact native unions.

### Preserve the campaign's maximum-speed metric policy

The native baseline and deferred metric importer have separate admission paths.
`--engine bulk --bulk-only` remains governor-free and does not inspect metric
policy or host-JWT-renewal environment variables. For a campaign already using
the owner's maximum-speed amendments, a compatible `--engine metric` handoff
must explicitly restore that same policy instead of falling back to the older
metric governor. Preserve the original plan, state, operation, metric flags and
health map; set these bindings to the existing immutable host-local artifacts:

```sh
export RESIZE_MAX_SPEED_DECISION=/campaign/resize-MAX-SPEED-DECISION-v3.json
export RESIZE_MAX_SPEED_DECISION_SHA256=EXACT_RAW_DECISION_SHA256
export RESIZE_MAX_SPEED_WRITE_OK=/campaign/existing-WRITE-OK.json
export RESIZE_MAX_SPEED_WRITE_OK_SHA256=EXACT_EXISTING_WRITE_OK_SHA256
export RESIZE_HOST_JWT_REFRESH=1
storectl run --engine metric --plan SAME_PLAN --state SAME_STATE \
  --health-map SAME_HEALTH_MAP --apply # retain the original metric arguments
```

The decision requires the explicit 14:50 debt removal, 15:20 relative read
thresholds and 15:45 cache-headroom amendments. Its existing copy-only WRITE-OK
must bind the actual manifest ID and SHA256. Host JWT renewal is optional; when
enabled, it reads only the fixed host secret path, keeps key/JWT material in
memory, renews hourly or after401, and grants only read/update. Delete, read
isolation, whole-store and checkpoint-local modes cannot use this override.
There is no new admission baseline or warm-up. Saved normal envelopes and MAX
cache/read/write/process state survive resumes. A real inherited destination
cache pause can be adopted once with `RESIZE_CACHE_PAUSED_ON_START=1`; an already
initialized recovered guard is never rearmed by a retained startup flag.

Metric work retains the exact MAXv3 behavior: pair-local write/process pauses,
destination free-disk and sustained one-minute read guards, cache/age hysteresis,
CPU/NIC/disk/L0/write-stall throttles, no compaction-debt or unlogged throttle,
and measured server transfer limits. Independent source pools and releasing
admission before a429 retry keep a busy source from starving other pairs. The
former90-second response-header deadline is disabled only in this explicit
metric mode; the ten-minute overall request bound and contexts remain.

Only native-deferred bitmap ordinals reach the metric importer. Native-copied
names are never replayed as ordinary empty-proof tasks. An acknowledged native
range resolves its exact durable receipt first; a missing/mismatched receipt
requires native recovery. A deferred child's known proof is checkpointed before
verification and reused after a transient verification failure. If a crash or
lost response leaves only its `bulk-intent` without a proof, metric retry/resume
fails closed instead of importing it twice. Integrity splits retain exact native
ordinals; every permitted skip is separately journaled. Source absence still
requires authenticated HEAD404 plus a fresh exact-prefix catalog absence proof,
bounded to eight concurrent proofs per source, with no MAX count/ratio budget.

Old policy/decision rows, original baselines and unknown campaign audit fields
remain byte-preserved. Mixed/MAX journals therefore retain their history instead
of compacting unknown fields away. This controller changes no daemon API or
storage behavior and includes no optional receipt-reuse or initial-copy-marker
accelerator. Normal metric behavior is unchanged when MAX has never been enabled;
an audited MAX metric resume cannot silently drop its explicit decision.

## Proof input and profiles

These commands write private proof artifacts and stores, never a live destination.
Use a root read token in `BUCKYD_TOKEN`; no token is written to capture metadata.
Check free disk before capture, and remove private captures/profiles afterward.

```sh
storectl capture-native --source http://DONOR:PORT \
  --names planned-sample-names.json --out /private/scratch/native.json
storectl bulk-eval --native --native-capture /private/scratch/native.json \
  --scratch /private/scratch --batch 100000 --workers 32 \
  --overlap-percent 50 --profile-prefix /private/scratch/native-proof
```

The evaluator seeds its donor through the reviewed importer, compares the native
candidate with that importer, includes native build/ingest/publication in timing,
and compares every archive's retention and point bits. Deferred combines run
outside native timing. `--memory` uses virtual Pebble files and must not be reported
as physical disk/NIC performance. Classic `--fixtures` inputs retain real archive
values but do not prove original production native layout. `--repeat` is reported
as replication and is allowed only for classic fixtures.

The private donor defaults to a 1 GiB block cache with 128 data shards, matching
production export concurrency rather than the old 32 MiB / seven-shard fixture.
`--source-cache-bytes` and `--source-cache-shards` permit matched controls.
`--source-scratch` places the donor on a separate disk when available. Native
daemon sessions use the owning daemon's configured cache directly.

An opt-in Linux diagnostic compares both cache topologies on synthetic native
histories and checks every point bit; it does not qualify production throughput:

```sh
GRAPHITE_NATIVE_CACHE_EVAL=/private/evidence TMPDIR=/private/scratch \
  GOTOOLCHAIN=go1.27.0 go test -mod=vendor ./internal/downsize \
  -run '^TestNativeExportCacheScaling$' -v -count=1
```

`GET /v1/native/stats` reports native wire/SST bytes, build/ingest time and creation
lock wait/hold/max-hold. CPU/mutex/block profiles and Linux device/NIC/CPU counters
must establish what binds before claiming sustained physical throughput.

## Time-window gap fills

Native export, transfer and combine accept an optional **[from, until)** window
of archive timestamps. Both endpoints are required, in Unix seconds or whole
second RFC3339. Use the target-wins engine for existing names:

```sh
storectl plan --spec gap-spec.json --out gap-plan --apply \
  --from 2026-01-01T00:00:00Z --until 2026-01-01T01:00:00Z
storectl run --plan gap-plan --state gap-window.jsonl \
  --engine bulk --bulk-only --combine-existing --apply
```

Alternatively supply `--from` and `--until` on `run` with an existing range plan
and a separate gap-fill journal. The window is hashed into new plans and range
identities, checksummed into v3 transfer headers, and saved in the journal domain.
Resumes inherit saved endpoints; changing them requires a separate operation.
All peers must advertise `NativeTimeWindow`. Windowed runs require `--bulk-only`;
metric fallback, whole-store replacement and `--merge-session` are refused.
Without a window, the original v2 protocol and full-retention behavior remain.

Each archive keeps the same normalized, ring-selected source values as the full
native copy. Equal target timestamps win, including signed zero. Target points
outside the window retain their exact timestamp and IEEE-754 value bits, including
NaN payloads. A ring alias that would evict an outside target point fails closed. Sources with
newer ring owners a full retention span beyond `from` also fail closed, since
those omitted owners could invalidate target timestamps inside an expired window.
An overlapping leading bucket has its timestamp before `from` and is skipped.
A coarse bucket starting inside the window but ending after `until` is copied
only when its entire source interval has elapsed and every immediate finer
sample is present and non-NaN. Its stored coarse value stays authoritative;
it is never recomputed. Other partial boundary buckets are skipped. Receipts
persist `WindowSkipped` (ordinal, archive, timestamp, reason), also emitted as
`boundary_skipped`; inspect these before declaring the gap covered.

Only window points go over the wire. The source still checks retained groups for
ring ownership and integrity; the receiver may read/reassemble the target history.
Existing per-name and worker memory limits continue to apply. This reduces network
bytes, without promising throughput proportional to the byte reduction.

Local Apple M4 Max benchmark (32 names, 2M physical points per archive on both
sides, real target-wins publication, in-memory Pebble, three 1× runs):

| Mode | Median names/s | Wire bytes/name |
| --- | ---: | ---: |
| Full retention | 17.48 | 1,060,362 |
| 1-hour window | 17.80 | 2,201 |

The throughput ratio is 1.02×; wire reduction is 481.8×. This measures the existing
heavy synthetic fixture, not production disk/NIC throughput. Run with
`go test -p 1 -parallel 1 ./internal/chunkstore -run '^$' -bench
'^BenchmarkNativeWindowHeavy$' -benchtime=1x -count=3`.

Sync also accepts `--max-batch-bytes` and `--transfer-idle-timeout` and
forwards them to every missing-name and hole-fill transfer, including probes
and retries. For example, resume a busy target with smaller batches:

```sh
storectl sync --source http://source:4242 --to http://target:4242 \
  --state ./sync-state --max-batch-bytes=16777216 --transfer-idle-timeout=80s
```

If a long-name catalog page exceeds the HTTP decoder byte limit, discovery
retries the same cursor with fewer names and retains the smaller page size.
Existing sync and single-source copy journals remain reusable.

Missing-name retention checks use `--streams` bounded parallel batches and
checkpoint each completed `--verify-batch` in the sync journal. Each bounded
wave is copied before checking the next wave, so large catalogs start importing
without waiting for every source name to be checked. A batch `/info` 404 falls
back to recursive halves until missing names are isolated; healthy sub-batches
remain batched. An absent name can be tried on a later source. Before resolving
an empty name, sync checks the target and every configured source, including
peer survivors, over all native retention archives. Proven empty names receive
`absent_all_sources` in `sync.jsonl` and `resolved.json`, and do not remain residual.
Failed or incomplete proofs retain machine-readable reasons in `residual.json`.
Fill rounds continue while the remaining hole/skip count decreases; use
`--max-rounds N` to impose a limit (`0`, the default, converges). Stalled fills
retain `persisted_after_fill` with source, step, count and timestamp evidence.
Resume with the same batch and stream settings to reuse the copy wave journals.
If a metadata request ends in EOF or a timeout, the client halves its name
batch and retries the same cursor. It remembers the smaller cap per source
for the rest of the run; single-name failures still use `--host-wait`.

### Recover a recent gap in short-retention metrics

Sync normally skips names whose archives all retain at most three days. For a
recent gap, explicitly include those names so a two-day archive is compared and
filled over the requested window too:

```sh
storectl sync --source http://peer-a:4242 --from http://peer-b:4242 \
  --to http://target:4242 --window incident:1794595620:1794597120 \
  --include-short-retention --state ./incident-fill
# Independent verification: use a new state directory with the same options.
storectl sync --source http://peer-a:4242 --from http://peer-b:4242 \
  --to http://target:4242 --window incident:1794595620:1794597120 \
  --include-short-retention --state ./incident-verify
```

Existing target points still win. The opt-in is part of the journal identity:
use a fresh state directory when enabling it after a default run, so earlier
skipped comparisons cannot be reused as verification.

### Window-only sync copies

With `sync --window`, missing names are selected by source native exports over
those windows. Names with no window points are skipped; selected names are
copied immediately in bounded batches through the existing native window
exporter and target-wins combiner. Hole fills use the same window restriction.
The catalog still has to be scanned, but full-history metadata and transfers
are avoided. Sources must support native time windows, and the
target must support native window combine; missing support fails explicitly.
No full-history transfer fallback is used. Incomplete trailing coarse buckets
omitted by the native window exporter are reported as failed names/residuals,
so a partial boundary copy cannot be reported as complete.

```sh
storectl sync --source http://source:4242 --to http://target:4242 \
  --state ./gap-state --window incident:1794592020:1794592680
```

Existing target values and points outside the windows remain unchanged. The
usual `--settle` cutoff and short-retention policy still apply. Full-history
sampling is disabled in window-only mode. To request the previous full-history
missing-name copies, fills and `--sample-every` behavior, add `--full-history`.
Without `--window`, behavior is unchanged.

An existing window sync journal can resume in window-only mode: completed
full-history work already covers the windows, while new selections and native
receipts have separate journal records. Switching to `--full-history` requires
a fresh state directory. Keep `--verify-batch`, the windows and the settle
cutoff unchanged when resuming a partially completed window transfer.

A catalogued source name whose native snapshot omits retention metadata is unresolved (for example, pending writer state). Window selection records that name as an unresolved residual and continues copying healthy peers in the batch. Skipped selections are retried on fresh native pins when resuming and again after the missing-name scan. Only a complete window selection, followed by a successful window-only copy when points exist, writes a durable resolution. Original selection and native copy intents stay unchanged. Unresolved source coverage still prevents a successful exit even if another replica or serving-archive verification finds no holes; it is never treated as empty history.

### Durable discovery and progress snapshots

Rerun the same command with the same `--state` after SIGINT, a deadline, or a
crash. Sync appends its union and missing-name files in bounded chunks, fsyncs
both, and journals their byte offsets and each source's last consumed name.
Recovery discards only bytes after that checkpoint and continues each source
with `after=<saved name>`. Prefetched pages never advance the saved cursor.
Completed verify, retention-check, and fill batches keep their existing journals.
Existing state directories remain valid; earlier completed listings are reused.
Old interrupted listings without checkpoints require one fresh discovery.

Plan-free `copy` also saves each source's catalogue in fsynced pages beside its
state journal. On resume, saved names are replayed through completed transfer
ranges before listing the remaining pages. This keeps discovered but not yet
copied names pending. The copier and background name counter share this log,
so counting does not relist the source. Keep these sidecar files with the journal.
Only one process may use a state at a time.

Watch `STATE/progress.json` for `sync`, or `STATE.progress.json` for `copy`
(where copy's `STATE` is a journal filename). The file is atomically replaced
at startup, every three seconds, and on exit. It includes `phase`, `round`,
`cursors` keyed by source origin, `names_discovered`, `names_verified`,
`names_copied`, `names_filled`, `holes`, `names_per_second`, `last_update`,
`status`, and an optional `error`. Counts come from durable journals: discovery
counts the union for sync and source catalogue entries for copy; verification
and hole observations accumulate across rounds. `holes` counts observed missing
points, including observations repeated in later rounds. The rate counts newly
checkpointed work per second since this invocation started. Journals remain
the source of truth; the snapshot is for monitoring. No tokens enter these files.

Resume verification with more parallel reads while keeping copy batch receipts stable:

```sh
storectl sync --from http://source:4242 --to http://target:4242 \
  --state ./sync-state --streams 16 --verify-streams 128
```

Keep the original `--streams` and `--verify-batch` when resuming. `--verify-streams`
changes verification workers only; zero keeps the original shared worker count.

### Pre-cutover merge from a live donor

Use an explicit session only when a later backfill owns the donor's cached and
post-snapshot delta:

```sh
storectl run --plan plan --state merge-state.jsonl \
  --engine bulk --bulk-only --merge-session precutover-history --apply
```

Both donor and destination must advertise `NativePinnedMerge`; upgrade hosts
before using this mode. It exports the donor's committed MVCC snapshot even for
names already in cache or revised after the pin. A name that exists only in cache
has no committed history to export and remains deferred. Every target name is
still created only if absent at publication, including a name created while the
import was being prepared. Existing names are skipped. Combine, metric fallback,
deferred combine, deletion and reconciliation are refused in this mode.

The controller fsyncs a `NativeSource` journal row before the first pull from each
pin. It contains the donor origin, merge session, source pin ID,
`SnapshotStartedAt` (before the pin), `SnapshotPinnedAt` (after the pin),
`BackfillFromUnix: 0` and `BackfillFullRetention: true`. The explicit safe event
lower bound is Unix 0, so no queued or historical timestamp can fall before it.
Retain every row across resumes and use the earliest start per donor for the
post-cutover delta. This is a **commit boundary**, not an event-time watermark:
queued points and late input can carry earlier timestamps. Backfill all retained
history for those names (or use an independently proven earlier event bound),
then require complete timestamp/IEEE-754 parity. Do not truncate backfill to the
wall-clock pin time. A restarted controller takes a new pin and records another
boundary; completed range receipts remain authoritative.

Without `--merge-session`, live cached/revised names retain their existing
conservative deferral behavior.

## Local maintenance

The same binary provides maintenance commands; no separate maintenance binaries
are installed. Run `storectl maintain ACTION --help` for action flags.

| Command | Use |
| --- | --- |
| `storectl maintain cold-pack` | Pack, unpack or downgrade an exclusively opened offline store; see [cold-pack](../../doc/cold-pack.md). |
| `storectl maintain convert` | Import complete Whisper archives with identity-bound resume and verification; see [conversion](../../doc/shared-exact-blocks.md#bulk-conversion). |
| `storectl maintain prune-empty-dirs` | Remove empty original directories while checking daemon readiness; see [pruning](../../internal/storemaint/prune/README.md). |
| `storectl maintain reclaim-orphans` | Inspect orphaned staged imports offline; `-apply` publishes reclaimed ownership. |
| `storectl maintain configs` | Compare old/new schemas and aggregations against a file-list cache; see the [example](../../README.md#useful-toolscommands). |

Offline actions retain the store's exclusive-open and source-binding checks.
Stop the daemon before cold-pack, conversion into its store, or orphan reclaim.
Pruning checks the running local daemon and retains its health and filesystem
guards. Existing maintenance checkpoints and receipts retain their data format.

## Input journal inventory

`storectl inventory inputlog -manifest snapshot.json -output candidates.jsonl.gz`
reads an immutable, exact-length input journal prefix. It verifies the full
captured SHA256 and every payload CRC, then writes sorted candidate names for
the manifest's timestamp windows. It never opens a running journal or writes
metric data. See the [manifest and limits](../../internal/inputlog/inventory/README.md).
The candidate list alone does not authorize filling; use `storectl sync` with
the appropriate acknowledged source and gap windows for recovery.

`storectl maintain anchors -secret-file /path/to/secret -journal dry-run.jsonl`
inspects archive anchors through buckyd. Applying requires `-apply -plan PLAN
-write-ok WRITE-OK`, with the plan SHA256 in WRITE-OK. See
[anchor maintenance](../../internal/storemaint/anchors/README.md).

`storectl inventory sealed -sst-dir SNAPSHOT -out inventory.json` inspects
immutable SST classes and ordinary eligibility; see [limits and interpretation](../../internal/sealedinventory/README.md).
