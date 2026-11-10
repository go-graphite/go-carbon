# Adaptive recovery and persistence capacity

Recovery can stop below an automatic writer's pressure threshold. The old
recovery predicate stopped at half the configured cache, whereas automatic
writeout used 70% occupancy. More replay workers could not resolve that wait;
a fixed automatic rate ceiling also prevented the writer from using idle hardware.

## Selecting the writer policy

```ini
[cache]
writeout-max-delay = "1800s"

[whisper]
max-updates-per-second = -1
max-updates-per-second-auto-min = 1000
max-updates-per-second-auto-max = -1
```

The floor is an operational choice; keep an established durability floor when
enabling feedback. `auto-max = -1` selects Linux host/storage feedback. Positive
values remain fixed overrides. Zero retains the existing unlimited demand
model and remains the programmatic default. These settings are reloadable with
a binary that understands the new value; older binaries reject `auto-max = -1`.

The demand controller still determines how often writes are needed from
population, backlog growth and batching deadlines. An actual denied recovery
admission now contributes pressure immediately and releases batching holds for
ten seconds after the last denial. Neither a small successful batch nor a
completed worker can clear another worker's pressure. Steady sparse input does
not cause growth just because hardware is idle.

The hardware ceiling starts at 500 updates/s per worker, bounded by the floor
and hard envelope. Switching from a fixed policy seeds it from the previous
rate. Every five seconds, it grows by `max(1000, 10%)` after two healthy samples
with eligible work, token waiters, demand at the ceiling, and confirmed output
near the installed token rate. CPU idle must exceed 25%, memory headroom 10%,
disk idle time 30%, and GC CPU must remain below 10%.

CPU/GC/device pressure or missing observations reduce optional catch-up by 30%,
while retaining at least the demand controller's smoothed steady need times
its 1.03 margin. This protected minimum applies even when measured CPU/worker
capacity estimates are lower. Only active/new write stalls, write failures,
disk below max(1 GiB, 1%), valid memory headroom below 5%, or the L0 stop threshold
may cut below steady demand, down to configured auto-min. High CPU use or slow
reads alone must not starve writes needed to keep the cache steady.

Read p99 measures per-series storage fetches, excluding glob expansion,
response assembly/network sends and separate point-cache waits. It works without
Prometheus. Eight buckets per power of two bound quantization error to 12.5%.
The reference is the median of quiet-write interval p99 samples over the last
hour (at most one per minute, excluding the 30 seconds after a capacity increase).
It can rise as well as fall; a single cheap interval cannot ratchet it down.
The relative guard is `max(5 ms, 1.5 * reference)`, with no absolute 100 ms trip.
No completed reads means no read-latency sample. A new pool discards the first
histogram snapshot as history rather than treating it as a fresh interval.

Read pressure holds increases by default. Within 30 seconds after an optional
increase, it can undo that one probe if confirmed write rate rose by more than
5% and read p99 rose 50% against both the pre-probe value and rolling reference.
The protected steady minimum still applies, and that probe cannot trigger
repeated cuts. This is a correlation guard, not proof of causation; a request
mix change can still cause one bounded rollback.

The runtime envelope is at most 2 million updates/s, 10,000 per worker and
10,000 per execution thread, refined using whole-process CPU per confirmed
update and measured worker service time, subject to the protected minimum above.
Disk pressure takes the busiest visible device to protect neighbours and
stacked devices. The existing mandatory restart durability floor remains
independent and can also exceed the optional catch-up ceiling.

Resource sampling and filesystem calls run outside the rate-controller lock.
Reload discards samples from an obsolete configuration generation. Structural
persister rebuilds retain controller history, while sampling the new pool's own
write counters. Worker counts are unchanged.

## Cache admission hook for capture/recovery owners

The application installs `cache.SetRecoveryMemory(hostcapacity.ReadMemory)`
before Linux intake. The sampler uses MemAvailable, Go memory-limit headroom
and visible cgroup ancestors, including `memory.high`, leaving a 5% or 64 MiB
reserve at every boundary. Other platforms keep the legacy admission policy.

`HasRecoveryCapacityFor(metric, count)` reserves a named batch from a five-second
window containing one quarter of available memory. Its byte estimate is at
least 64 bytes per point or 1.25 times observed cache bytes/point. An absent
active metric also reserves its name plus 1024 bytes. Existing series do not
pay another name/header reservation per batch. The remaining headroom protects
live intake, runtime/native buffers, sampling delay and bounded check/add races.
Configured full `max-size` and `max-bytes` remain admission checks. Concurrent
check/add callers can cross these checks by bounded in-flight batches, as in
the existing predicate; the shared memory credit bounds aggregate reservations.
Unknown memory stops new grants. Abandoned credit expires at the next sample.

Call it once per batch, retrying only denied checks, then add the exact batch:

```go
for !core.HasRecoveryCapacityFor(p.Metric, len(p.Data)) {
    // Wait with the recovery owner's cancellation/deadline policy.
}
core.AddRecovered(p)
```

Do not call the reserving API to probe capacity, or reserve again between a
successful check and AddRecovered. Split batches that cannot fit the configured
full cache ceiling. Capture/spool/decode buffers retain their own bounded memory
budget: they are outside cache ownership. The count-only
`HasRecoveryCapacity(count)` intentionally preserves the half-cache policy for
legacy callers, while still publishing denied-admission pressure. Continuous
input-log replay uses the named API. Startup capture and legacy tail replay
owners must adopt it separately; no startup-capture/receiver code changed here.

## Observation and rollback

Admission window/ceiling changes log at INFO. Writer rate logs require a 25%
change relative to the last logged value. Rate and capacity logs each emit at
most once per minute for a stable reason; reason transitions emit immediately.
Metrics retain every actuator change, and structural reload retains log cooldowns.
Metrics include:

- `cache_recovery_admission_{window,ceiling,remaining}_bytes`
- `cache_recovery_admission_blocked` and `cache_recovery_admission_rejections_total`
- `persister_auto_capacity_updates_per_second`
- `persister_auto_capacity_hard_ceiling_updates_per_second`
- `persister_auto_capacity_read_p99_seconds` and `persister_auto_capacity_read_reference_seconds`
- `persister_auto_capacity_minimum_updates_per_second` and `persister_update_token_waiters`
- Existing `persister_auto_update_rate{,_target,_reason}` and adjustment counters

Compare these with confirmed output, admission waits, backlog age/slope, read
p99/errors, GC/RSS and device pressure. An increasing limit is not proof that a
serialized storage stage became faster. A positive auto-max restores an operator
ceiling on the capable binary. There are no point/format changes.

Automatic restart CPU admission is also adaptive when
`store-metadata-cache-prewarm-cpu-cores = 0` on Linux. Its independent AIMD loop
keeps a 25% execution reserve, bounded debt and mandatory progress. Positive
core settings retain the fixed/history policy. Continuous input-log replay reads
frames on existing ordered partition workers; stream ordering and the 32-partition
wire layout remain unchanged.

## Ramp calibration

With 96 workers the seed is 48,000 updates/s, or the protected minimum if higher.
At 10% growth per ten seconds it reaches 66k at 40s, 72/75k at 50s, 80k at 60s,
100k at 80s and at least 144k (3 times seed) at 120s, provided demand, token
waiters, confirmed output and spare hardware persist. The test uses modeled
CPU/service costs; those times are simulated, not a fleet throughput promise.
The 2026-11-06 read-only canary cross-check confirmed operator/effective ceilings
of 66k, 72k, 75k, 80k and 100k, with spare CPU/device capacity. Keep the modest
seed and growth rate; a soft latency signal must not undo steady demand.
