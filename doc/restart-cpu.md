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
reads and input logging proceed independently; optional warming and post-gate
writeout share the remaining allowance. A configured fallback should leave
headroom above mandatory CPU consumption. Zero never means unlimited CPU.

Optional catalog warming and validation yield to waiting writers, required
cold-state prefetch, and any observed writeout-floor shortfall, even when CPU
credit remains. Bulk checkpoint loading and validation needed by upcoming cold
writes run alongside catch-up writeout. The serial checkpoint
stream gets at most one core of duty time (or allowance / 8 on smaller processes).
When process CPU leaves less room than that share and writeout is below 95%
of its floor, the stream share reduces fourfold; spare CPU keeps the full
share. The exact floor still controls warnings. One 5 ms sampling slice of the checkpoint share
is reserved so writers cannot repeatedly consume every refill before the next
checkpoint batch. The serial stream retains its duty-paced share even under
whole-process CPU debt, so unrelated maintenance or receivers cannot starve it.
Legacy checkpoint validation gets a separate required window per 100ms under
CPU debt, including without a writeout floor. Optional catalog warming and queue
preparation retain their existing admission paths. This bounded mandatory work
can exceed the process allowance alongside mandatory intake and reads. Batch wall
time is a conservative CPU estimate, including I/O and lock waits. Tiny duty
delays accumulate into 5 ms windows so timer rounding cannot multiply the load
time. Already
admitted bounded batches can briefly overlap writes; admission does not preempt
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
saved steady throughput and queued metrics divided by max delay. Rate tokens
and between-sample cost reservations cannot hold writeout below
a known predecessor floor; reaching it also requires ready work, enough workers
and CPU capacity for those updates. CPU admission limits catch-up within the
process allowance; mandatory intake and reads remain independent.

Automatic pacing (`max-updates-per-second = -1`) retains its own rate tokens,
bounded by `max-updates-per-second-auto-max`, throughout restart. Its CPU cost
reservations use three times the greater of the current automatic rate and
saved steady throughput, refreshed on SIGHUP and on automatic rate changes.
Without throughput history, the default write cost remains in use. This gives
reservation sampling enough headroom to reach the token rate without using low
predecessor throughput as a second rate limit, or making low auto demand slow
startup warming. It does not raise the automatic token cap or the process CPU
allowance. A SIGHUP replaces the persister but retains the shared CPU budget.

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
share while waiting for rate tokens or names. The consumer shares add to no more
than the configured allowance; checkpoint admission can exceed remaining process
headroom. `restart_cpu_checkpoint_loading` shows a pending or
running load, and `restart_cpu_checkpoint_load_bytes{kind="completed"|"total"}`
shows its stream progress (total is zero until known; the last known progress
is retained after failure). Compare `restart_cpu_process_cores` against
`restart_cpu_allowance_cores`, and confirmed `restart_cpu_writeout_updates_per_second`
against `restart_cpu_writeout_floor_updates_per_second`. The
`restart_cpu_writeout_ceiling_updates_per_second` metric reports the CPU
reservation rate: fixed mode also issues tokens at that rate, while auto uses
its separate `persister_auto_update_rate` token limit. A zero ceiling selects
saved throughput or the default write cost for CPU reservations. The shortfall
warning also logs the ceiling, worker count and each consumer's grant.

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
the initial point count before recording history. Restart admission retains
its separate drain policy: it continues for at least 30 minutes and until
warming and the startup backlog finish. History is saved each eligible minute
and at shutdown; shutdown joins the sampler before a successor can use its file.
