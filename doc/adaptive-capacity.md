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

It decreases by 30% on CPU/memory/GC/device pressure, missing observations,
insufficient disk space, write failures/stalls, excessive L0/debt growth, or
interval read-p99 regression. Read p99 uses bounded doubling buckets and the
best observed baseline (minimum 1 ms): the guard is `max(5 ms, 1.5 * baseline)`
with a 100 ms absolute bound. It covers HTTP/gRPC requests recorded by
carbonserver's existing request timing hook, independently of Prometheus.
No completed reads means no read-latency observation, rather than a fabricated
zero-latency baseline.

The hard envelope is at most 2 million updates/s, 10,000 per worker and 10,000
per execution thread, refined using whole-process CPU per confirmed update and
measured worker service time. Disk pressure takes the busiest visible device;
this conservatively protects neighbours and stacked devices. Free space must
exceed both 1 GiB and 1% of the filesystem. The configured automatic floor wins
over this optional catch-up ceiling. The existing mandatory restart durability
floor is independent and can also exceed it; do not configure an unsafe floor.

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

Every changed admission window/ceiling and writer capacity/rate logs at INFO.
Metrics include:

- `cache_recovery_admission_{window,ceiling,remaining}_bytes`
- `cache_recovery_admission_blocked` and `cache_recovery_admission_rejections_total`
- `persister_auto_capacity_updates_per_second`
- `persister_auto_capacity_hard_ceiling_updates_per_second`
- `persister_auto_capacity_read_p99_seconds` and `persister_update_token_waiters`
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
