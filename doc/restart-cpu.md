# Restart CPU allowance and history

With metadata prewarming and a positive metadata cache size enabled,
`[whisper] store-restart-cpu-headroom` multiplies saved steady CPU consumption
to set the whole-process restart allowance. It defaults to `1.05`, preserving
the previous multiplier, and accepts finite values from `1.0` through `4.0`.
`store-metadata-cache-prewarm-cpu-cores` supplies a **fallback** when history
is unavailable. Both settings are startup-only: SIGHUP accepts config changes
but the active allowance changes only on the next process start.

The selection order is:

| Available history | Configured value | Allowance | Logged source |
| --- | --- | --- | --- |
| Fresh host-local steady sample | Any | `store-restart-cpu-headroom` × saved steady CPU cores | `history` |
| None, invalid or stale | Positive | Configured cores | `fallback` |
| None, invalid or stale | `0` (default) | Host CPU count / 12, rounded down, at least 1 and at most GOMAXPROCS | `fallback` |

For example, a fallback of `48` provides headroom on a first start. Once the
process records a steady value of `16` cores, its next start uses `16.8` cores,
with the default headroom, even while the configured fallback remains `48`.
Setting headroom to `1.5` instead gives `24` cores for the same history. Headroom
never multiplies either fallback. The startup `restart CPU allowance` log
includes `source`, configured `headroom`, and selected `cores`; on the fallback
path the logged headroom is unused.

A sample expires after 24 hours. History from another host or the future is
rejected. The older 30-sample median format remains readable within its original
freshness window and uses the same configured headroom.

The allowance includes user and system CPU from the entire process. Receivers,
reads and input logging proceed independently. Catch-up above the writeout
floor and optional warming share the remaining allowance. Writeout at its floor
is mandatory durability work: a low or stale CPU history must never stop every
writer. The allowance selection remains unchanged, including fractional saved
history; raising the configured fallback is not needed to recover writes.
Zero never means unlimited CPU.

Optional catalog warming and validation yield to waiting writers, required
cold-state prefetch, and any observed writeout-floor shortfall, even when CPU
credit remains. Bulk checkpoint loading and validation needed by upcoming cold
writes run alongside catch-up writeout. The checkpoint worker
pool gets at most half the allowance, capped by
GOMAXPROCS and an explicit `store-metadata-cache-prewarm-workers` setting.
Zero workers selects concurrency from the allowance. Each worker owns a duty
pacer, so fractional allowances still work with one worker and overlapping
loads share the same aggregate allowance. When process CPU leaves less room
than that share and writeout is below 95%
of its floor, the pool share reduces fourfold; spare CPU keeps the full
share. The exact floor still controls warnings. One 5 ms sampling slice of the
checkpoint share
is reserved so writers cannot repeatedly consume every refill before the next
checkpoint batch. Required queue construction, cold prefetch and validation
retain dependent-work priority. If CPU debt would block a due writeout floor, these prerequisites get
one shared progress window per 100ms. Their existing bounded batches and queue
windows still apply. Legacy checkpoint validation also gets a shared required
window per 100ms without a writeout floor. Optional catalog warming cannot use
that escape. Bulk checkpoint workers retain their duty-paced share even when
whole-process CPU exceeds the allowance; unrelated CPU debt cannot reduce their
admission to zero. This bounded mandatory work may exceed the process allowance
alongside mandatory intake/reads and floor writes. Batch wall
time is a conservative CPU estimate, including dependent reads and lock waits.
Workers close the active window before waiting for another I/O buffer, so time
blocked on the reader/writer queue is not charged as CPU. Tiny duty
delays accumulate into 5 ms windows so timer rounding cannot multiply the load
time. Already admitted bounded batches can briefly overlap writes; admission does not preempt
running work. Estimated work costs are reserved immediately and reconciled
against whole-process CPU every 5 ms, with at most 5 ms of saved CPU credit.

The checkpoint accelerator attempt has a ten-minute deadline after input
readiness, including admission waits. A timed-out partial load remains safe:
missing metadata uses ordinary durable-store lookups and provisional legacy
records still require validation. This bounds the attempt, not guaranteed full
population under sustained CPU saturation, and cannot interrupt a blocked
filesystem syscall. No writeout gate is added.

Deferrable store maintenance waits for both the warming attempt (including any
fallback scan) to finish and restart pacing to end. This covers history relayout,
cold-pack, expiration sweeps, compact-once, catalog membership construction and
invalid-name purging. The wait is bounded to 35 minutes from store startup,
including store opening and restoration. A timeout starts maintenance with a
warning even if warming or backlog drain is stuck. The checkpoint's mandatory
share still applies if the two overlap. Each worker logs when waiting and when
starting, including its release reason; shutdown cancels pending waiters.
SIGHUP keeps the original gate and deadline when changing maintenance policy.
Expiration additionally waits for pending-point restoration before scanning.
Mandatory WAL/flush work, ordinary Pebble compaction, foreground operations and
explicit offline maintenance calls keep their existing behavior. With metadata
prewarm disabled or no metadata cache, no restart maintenance wait is installed.

During fixed-mode restart pacing, the rate-token ceiling is three times the
greater of saved steady throughput and queued metrics divided by max delay, replacing
`max-updates-per-second`. Without saved throughput, a finite configured cap
is raised to at least three times the queue floor; zero stays unlimited.
A growing queue can raise this rate ceiling; the CPU allowance stays fixed.
Normal settings resume when pacing ends. The writeout floor is the greater of
saved steady throughput and queued metrics divided by max delay. A separate
floor-rate admission bucket allows those writes despite process CPU debt. Every
admitted update, including CPU-funded catch-up, debits that bucket, so the floor
cannot fund extra catch-up. Idle credit is capped at 40ms of the floor, with at
most one admitted batch beyond it; earlier catch-up cannot build unlimited floor
debt. CPU-funded retries retain their original timing. All work still charges
the whole-process CPU meter, so optional work sees the cost of mandatory writes.

Reaching the floor also requires eligible work, working storage, enough workers
and physical CPU capacity for those updates. When the floor itself cannot fit
the saved allowance, only that mandatory rate can exceed it. Catch-up above the
floor remains subject to the original process allowance. Mandatory intake and
reads remain independent. Automatic rate tokens and their configured maximum
still apply, including when lower than the restart floor.

Automatic pacing (`max-updates-per-second = -1`) retains its own rate tokens,
bounded by `max-updates-per-second-auto-max`, throughout restart. Its CPU cost
reservations use three times the greater of the current automatic rate and
saved steady throughput, refreshed on SIGHUP and on automatic rate changes.
Without throughput history, the default write cost remains in use. This gives
reservation sampling enough headroom to reach the token rate without using low
predecessor throughput as a second rate limit, or making low auto demand slow
startup warming. It does not raise the automatic token cap or the process CPU
allowance. SIGHUP updates compatible persister settings in place; structural changes
rebuild the persister. Both paths retain the shared CPU budget.

Warm-first prefetch keeps one queue reader per persister worker. Eligibility
checks can wait for a cache shard lock, so a single reader would make one
contended shard starve the entire writer pool. This matters when restart
timestamp ordering ends and the configured queue order resumes, while automatic
rate sampling continues scanning the cache. The raw and ready windows remain
bounded, with at most one additional name per queue reader. Prefetch continues
after restart pacing ends; rate tokens and CPU admission still belong to the
workers, and readers share the queue's existing rebuild callback.

`restart_cpu_granted_cores` and `restart_cpu_grants_total` include consumer
`checkpoint_load` alongside `warming` and `writeout`. Grants describe admission
shares, not measured CPU attribution. A released writer retains a reported
share while waiting for rate tokens or names. The shares add to no more than
the available allowance. `restart_cpu_checkpoint_loading` shows a pending or
running load, and `restart_cpu_checkpoint_load_bytes{kind="completed"|"total"}`
shows its stream progress (total is zero until known; the last known progress
is retained after failure). Compare `restart_cpu_process_cores` against
`restart_cpu_allowance_cores`, and confirmed `restart_cpu_writeout_updates_per_second`
against `restart_cpu_writeout_floor_updates_per_second`. The
`restart_cpu_writeout_ceiling_updates_per_second` metric reports the CPU
reservation rate: fixed mode also issues tokens at that rate, while auto uses
its separate `persister_auto_update_rate` token limit. A zero ceiling selects
saved throughput or the default write cost for CPU reservations. The shortfall
warning also logs the ceiling, worker count, each consumer's grant, and
`floor_grants`. `restart_cpu_writeout_floor_grants_total` counts batches admitted
despite CPU debt; these do not imply spare CPU headroom in the grant gauges.

Shortfall diagnostics require eligible backlog or an in-flight write, so normal
deferred batching does not count as a stall. The first WARN is emitted after
more than 10s below the floor; reminders are limited to one per five minutes.
After three continuous minutes below half the floor, one ERROR
(`restart writeout stalled`) is emitted per escalation.
`restart_cpu_writeout_stalled` is one while that severe shortfall persists during
pacing, and `restart_cpu_writeout_stalls_total` counts escalations across persister
reloads. Recovery to at least half the floor, or disappearance of eligible and
in-flight work, clears the severe episode. A later episode can escalate again.
The mandatory floor admission recovers CPU starvation immediately; diagnostics
also expose failures that CPU admission cannot repair, such as storage errors.

Enabled checkpoint loads that finish invalid or with an error log WARN with
runtime, entry counts, bytes, timeout status and the error. They increment
`restart_cpu_checkpoint_load_failures_total`; deadline errors also increment
`restart_cpu_checkpoint_load_timeouts_total`, including legacy validation.
Missing first-start checkpoints count as failures. Explicitly disabled
checkpointing makes no load attempt. These counters survive persister reloads.

Increasing headroom trades extra CPU for faster recovery; it does not guarantee
a recovery time or cache peak. A rough spend estimate at a fixed steady
load is `(headroom - 1) × steady cores × recovery minutes` extra core-minutes
while the process consumes the full allowance. For example, `1.25` with a
16-core baseline permits about 20 extra core-minutes over five minutes. Verify
writeout, cache growth and total CPU after changing it.

CPU and confirmed update rates are saved in `restart-cpu.json` in the shared
store directory. Sampling requires two consecutive minute ticks after metadata
warming and restoration finish, without cache pressure, and no increase in
queued points across that minute. A steady process can therefore record its
first sample within two minutes of becoming eligible. Growing queues, warming,
dump loading, and pending restored writes prevent sampling. CPU history can
refresh during quiet batching phases. Saved throughput updates only when a
complete minute of confirmed writes meets at least 95% of the writeout floor;
quiet minutes preserve the last qualified rate. Until one is recorded, the
configured writeout ceiling applies. This avoids shrinking the next restart's
catch-up ceiling because of gaps between writeout phases.

Normal batching can keep more points than were present when receivers opened,
and max-delay writeout can have quiet phases. These do not require a return to
the initial point count before recording history. Restart admission continues
for at least 30 minutes and until warming finishes, dump restoration completes, and saved
recovery records are confirmed durable. It does not wait for live points to
return to the intake-ready count: ordinary buffering and workload growth can
keep that old target out of reach indefinitely. There is no absolute timeout
for genuine unfinished warming or restoration; those remain restart work.
History is saved each eligible minute and at shutdown; shutdown joins the sampler before a successor can use its file.

Checkpoint load and preparation retain the v1/v2 file framing. A sequential
reader cuts the stream into roughly 4 MiB sections at record boundaries and
hands bounded buffers to parallel workers. Clean-fenced records copy directly
into cache arenas without allocating a name per entry. Legacy v1 records still
use revision validation before promotion. Cache fills never evict live state.
Preparation copies independent cache shards in bounded windows and computes
checksums in workers; one writer appends large sequential buffers. It uses a
fresh CPU meter with the same allowance after writers stop. The clean stamp,
sequence check, file sync, rename and directory sync remain the publication
boundary. A predecessor/rollback can read the resulting file directly.

The checkpoint contains continuation state as well as fixed metadata: names,
retentions, manifests, headers and any cached tail state. Large entries reflect
that payload. `store-metadata-cache-pack` controls the existing per-state Snappy
packing (default false in the application). It preserves the same checkpoint
format, but only helps compressible state; the fixed frame adds just eight bytes
per entry. Changing packing or retention of cached state is independent of the
parallel checkpoint pipeline.
