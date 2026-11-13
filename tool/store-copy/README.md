# Store copy

`store-copy` grows the existing `downsize` controller. Both entrypoints call the
same code and use identical plans, manifest hashes, operation IDs and journals.
The resize/merge native baseline is the first consumer; replacement, gap repair
and remaining legacy frontends migrate after its canary.

Build with `GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build ./tool/store-copy`.
The owning donor and receiver daemons must also contain the native engine.
Existing metric commands keep their behavior unless `--engine bulk` is selected.

## Plan-free copy (`copy`)

`copy` discovers names and copies them. There is no plan directory, plan ID,
segment or per-name estimate:

```sh
store-copy copy --from http://donor:4242 [--from http://donor2:4242 ...] --to http://target:4242 \
  [--names FILE | --prefix P] [--ring ring.json] --state copy.jsonl \
  [--streams N] [--max-batch-bytes B] [--max-batch-names N] [--stall-timeout 30m]
```

### Usage: one command

One invocation copies a whole block and runs until it is done, hours if needed.
Every retry, wait for a restarting host and resume happens inside the process:
no wrapper loop, no relaunch.

```sh
export BUCKYD_TOKEN=...   # read+update JWT
store-copy copy \
  --from http://donor-a:4242 --from http://donor-b:4242 --from http://donor-c:4242 \
  --to http://survivor:4242 --ring resize-spec.json \
  --state /var/lib/carbon/copy/block-1.jsonl
```

- All donors feed one worker pool; `--ring` keeps only the names the survivor
  owns. Journal rows carry their donor.
- A donor or survivor restart is waited out: refused connections, transport
  errors, 408/429/502/503/504, a target 500 caused by a refused source export,
  and export 409/404/500 answers during a donor's restart window are retried
  with backoff (`--retry-interval` to `--max-backoff`). The batch in flight is
  re-pulled whole; existing target points win, so the redo is safe.
- A host that does not answer means wait, without limit. The copy fails loudly
  (`copy_stalled` ERROR, non-zero exit) only when every source and the target
  answer their status endpoint and the calls still fail for `--stall-timeout`.
- Killed or stopped: rerun the same command; the journal skips done ranges.
- One `progress` line per `--progress-interval` (30s): done/total names (the
  total is counted in the background), names/s and MB/s over the interval, ETA,
  the current limiter (a host down and for how long, target 429 backpressure
  with its compaction debt/L0/write stall, transient retries, busy transfer
  streams, name discovery), retries and skipped names. `copy_done` ends the run
  with the summary.
- A name that keeps failing never stops the run: it is skipped (`name_skipped`),
  retried once at the end (`retry_skipped`), and the names still failing are
  listed in `<state>.skipped` (source, name, HTTP status, reason) and reported
  by a non-zero exit. `--max-failures N` (default 0, off) aborts above N.
- The watchdog fails the run (`copy_stalled`, non-zero exit) when no batch has
  succeeded for `--stall-timeout` while every host answers and work is in
  flight.

- Names come from the source catalog (`/v1/transfer/list` pages, optionally
  under `--prefix`) or from `--names` (byte-sorted unique, `LC_ALL=C sort -u`).
- `--ring` takes a resize spec (its `new` ring) or a bare topology
  (`{"hash":"jump_fnv1a","nodes":[{"id","node","endpoint"}]}`). Names whose
  owner is not the `--to` endpoint are skipped.
- Batches are cut by real exported bytes: the controller learns bytes per name
  from each transfer's `X-Transfer-Bytes` and fills 75% of
  `min(--max-batch-bytes, target max_body_bytes)`, up to `--max-batch-names`.
  A 413 splits the batch in half. A single name may use the whole
  `max_body_bytes`; a name above that is journaled as failed and skipped.
- Streams default to `min(64, sum of source MaxTransfers, target MaxTransfers)`
  from the start; no slow-start ramp. 429/502/503/504 (target storage pressure,
  write stalls, transfer slots) back off and retry without aborting.
- `--names` takes a single `--from`; with several donors use catalog discovery
  (optionally `--prefix`). `--transport-retries` is deprecated and ignored.
- `--state` is an append-only journal: a header with the copy identity (from,
  to, prefix, names/ring file hashes), `done` rows with the first/last name of
  each completed batch, and `failed` rows. Rerunning the same command skips done
  ranges and retries failures. Target points win in the importer, so a batch
  redone after a kill is harmless.
- The command exits non-zero while any name is still skipped after the final
  retry.
- Only `--engine metric` is wired; `--engine bulk` and host-helper guards
  (`--health-map`) still need the legacy `plan`/`run` pair.

## Resize / merge baseline

After the campaign owner pauses the existing controller, retains its journal and
approves the concrete binary/plan through the campaign's PLAN-READY/WRITE-OK flow:

```sh
store-copy run --plan /path/to/existing-plan --state /path/to/existing-journal \
  --engine bulk --bulk-only --apply
store-copy status --plan /path/to/existing-plan --state /path/to/existing-journal
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
sub-ranges that already completed. `store-copy plan --segment-metrics N` sets
the names per plan stream segment and defaults to 4096.

`store-copy plan --names FILE --source URL --target URL --out DIR --apply` plans an
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
`store-copy status`. `--backlog-baseline-window` is fixed: it describes how the
saved baseline was learned. Journals written before the read policy was saved
adopt this run's read flags (default floor 50 ms) and record the replaced 2 ms
floor as an override.

```sh
store-copy run --engine metric --plan SAME_PLAN --state SAME_STATE \
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
store-copy run --engine metric --plan SAME_PLAN --state SAME_STATE \
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
store-copy capture-native --source http://DONOR:PORT \
  --names planned-sample-names.json --out /private/scratch/native.json
store-copy bulk-eval --native --native-capture /private/scratch/native.json \
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
store-copy plan --spec gap-spec.json --out gap-plan --apply \
  --from 2026-01-01T00:00:00Z --until 2026-01-01T01:00:00Z
store-copy run --plan gap-plan --state gap-window.jsonl \
  --engine bulk --bulk-only --combine-existing --apply
```

Alternatively supply `--from` and `--until` on `run` with an existing range plan
and a separate gap-fill journal. The window is hashed into new plans and range
identities, checksummed into v3 transfer headers, and saved in the journal domain.
Resumes inherit saved endpoints; changing them requires a separate operation.
All peers must advertise `NativeTimeWindow`. Windowed runs require `--bulk-only`;
metric fallback and whole-store replacement are refused.
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
store-copy sync --from http://source:4242 --to http://target:4242 \
  --state ./sync-state --max-batch-bytes=16777216 --transfer-idle-timeout=80s
```

If a long-name catalog page exceeds the HTTP decoder byte limit, discovery
retries the same cursor with fewer names and retains the smaller page size.
Existing sync and single-source copy journals remain reusable.
