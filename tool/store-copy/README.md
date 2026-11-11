# Store copy

`store-copy` grows the existing `downsize` controller. Both entrypoints call the
same code and use identical plans, manifest hashes, operation IDs and journals.
The resize/merge native baseline is the first consumer; replacement, gap repair
and remaining legacy frontends migrate after its canary.

Build with `GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build ./tool/store-copy`.
The owning donor and receiver daemons must also contain the native engine.
Existing metric commands keep their behavior unless `--engine bulk` is selected.

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
