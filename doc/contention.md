# Detecting mutex bottlenecks

Both telemetry exporters read `/sync/mutex/wait/total:seconds` directly from
`runtime/metrics`, independently of startup and application locks:

* Prometheus: `go_sync_mutex_wait_seconds_total`.
* Graphite: `<graph-prefix>.runtime.mutexWaitSecondsTotal` (cumulative).

`rate(go_sync_mutex_wait_seconds_total[2m])` or Graphite's
`nonNegativeDerivative` gives **goroutine wait seconds per wall second**. This
can exceed the CPU count: 190 goroutines blocked for a second contribute about
190 seconds, even on an idle machine. The counter is an estimate, includes Go
runtime locks and completed waits, and works with mutex profiling disabled.
Use sustained growth alongside input/backlog/latency to distinguish a throughput
ceiling from a brief burst; it is not a CPU utilization percentage.

`go_sched_latency_seconds` is the runtime's sampled runnable-to-running latency
histogram, coarsened to powers of two nanoseconds. The sum is estimated using
runtime bucket midpoints. Graphite exports the same cumulative distribution as
`runtime.schedulerLatencyBuckets.le<nanoseconds>Ns`, plus
`schedulerLatencySamplesTotal` and `schedulerLatencySecondsTotal`.

`[pprof].mutex-profile-fraction` defaults to **10000**, sampling one in 10000
contention events. `[pprof].block-profile-rate` defaults to **0** (disabled);
its unit is average nanoseconds blocked per sample, not a fraction. Both existing
knobs apply on start and configuration reload, even if the pprof listener is
disabled. An explicit zero mutex fraction still disables profiling. With a pprof
listener enabled, holder call sites are available at `/debug/pprof/mutex` without
changing the sampling setting during an incident. A sparse site may need a
smaller fraction for useful samples. Sampling affects profiles, not the wait
counter's availability.

At fleet scale collect the same mutex and runtime profiles from each affected host.
It requires neither `/metrics` nor a Go installation on stores. Goroutines show
**waiters now**; mutex profile deltas show **holders at unlock**. A call site is
not a lock address: unrelated shard locks at one call site appear together.
Disabled profiling is explicitly reported as unknown, not zero contention.

Collectors must never wait for lifecycle work. Migration collection uses
`TryRLock`/`TryLock`, snapshots live component data before emitting, and reports
`migration_metrics_skipped_total` when busy. Runtime telemetry remains present
on those scrapes. Heartbeat windows and automatic-rate observations are
published atomically; tag queue lag comes from the queue worker (at most one
worker iteration behind). Expiration rule collection snapshots or skips.
Input-log, phase CPU, malloc, handoff, cache and receiver metrics use counters
or published snapshots. Histogram and request-percentile collectors snapshot
or skip their short statistics locks. Store telemetry uses the existing
background controller's one-second Pebble/cache observation and reads live
application counters; it never enters Pebble's manifest lock. The
`store_metrics_snapshot_age_seconds` gauge (Graphite
`storage.metricsSnapshotAgeSeconds`) exposes a stalled sampler. Store control
and inventory state are copied with try-locks; a busy state skips that scrape.

Avoid holding several mutation stripes while waiting for the next one.
`internal/lockset` releases an incomplete set and waits on the conflicting stripe
alone, retaining atomic batch semantics after the complete set is acquired.
Use copy-then-unlock for external visitors. Sidecar inventory publication follows
successful storage commits; unrelated names never share a mutex across WAL I/O.

Run the optional production-concurrency guards in an isolated CI job:

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor GO_CARBON_CONTENTION=1 \
  go test -p=1 ./internal/contentiontest ./cache ./receiver/tcp ./persister ./carbonserver \
  -run 'ContentionBudget|WaitBudgetDetects' -count=1
```

Each path uses 256 producers/readers at GOMAXPROCS=8 and a 48 wait-seconds/second
budget. Setup, fixtures and GC are excluded; joins are included. These are
process-wide runtime counters: do not use `t.Parallel`, co-schedule unrelated
work in the same test process, or treat a laptop timing as a production capacity
forecast. A controlled global lock must exceed the budget, verifying that the
runtime measurement can detect serialization. The guards take seconds, not
minutes; they are opt-in to keep ordinary/race suites light.

`TestContentionDifferential` hashes timestamps, raw value bits and storage
metadata for single, grouped and transfer flushes. Run the same test unchanged
against the base and candidate sources before deploying data-path changes.
It includes special floats, late and duplicate samples, rollup routing and
concurrent writers with deterministic per-metric input order.

Profiling overhead benchmark (receiver parser → cache → persister → Pebble;
socket setup excluded):

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go test ./persister -run '^$' \
  -bench '^BenchmarkContentionPipeline$' -benchtime=262144x -cpu=8 -count=1
```

It alternates off/on/on/off and reports process CPU per point and allocations,
using 256 producers and bounded cached data. Compare paired blocks, not a single
wall-time sample from a shared host.
