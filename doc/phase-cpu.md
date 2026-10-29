# CPU by subsystem

Enable the optional phase CPU estimator at startup:

```toml
[common]
phase-cpu-profile-interval = "60s"
phase-cpu-profile-duration = "1s"
```

The default is off. The default duration is one second; the interval must be at
least 60 times the duration. Both values require a restart when changed. This
keeps the sampling duty below 2% including the ±5% period jitter. The standard Go CPU profiler samples at 100 Hz for the configured short window
per interval, with subsequent periods jittered by ±5% to reduce synchronization
with periodic jobs. Each window is weighted by its period. It cannot run simultaneously with another CPU profiler: a window
that loses ownership to Pyroscope or `/debug/pprof/profile` is counted as a failure
and skipped. It never stops somebody else's profile. On-demand requests may also
find the CPU profiler busy during a window.

`go_carbon_phase_cpu_seconds_total{phase}` extrapolates each successful window's
CPU over the interval. These are statistical estimates, not exact accounting for
the gaps between windows. A burst outside a window can be missed; take rates over
many intervals. Interrupted shutdown windows contribute only their measured CPU.
Do not add wall-time counters to CPU or interpret mutex wait seconds as CPU.

For a dashboard, stack:

```promql
sum by (phase) (rate(go_carbon_phase_cpu_seconds_total[10m]))
```

Compare its total with `rate(process_cpu_seconds_total[10m])`. The phase total is
not normalized to process CPU. On platforms without the Prometheus process
collector, use OS user+system CPU; the opt-in local end-to-end test uses getrusage.
Inspect these alongside it:

```promql
rate(go_carbon_phase_cpu_profile_seconds_total[10m])
increase(go_carbon_phase_cpu_profile_failures_total[10m])
sum(rate(go_carbon_phase_sampled_cpu_seconds_total[10m]))
```

`go_carbon_phase_cpu_accounted_seconds_total` reports wall time represented by
successful estimates; a short end-to-end run can end between windows, so compare
estimated CPU per accounted second with OS CPU per elapsed second.

The first is actual profiling coverage (about 1/60 at a 60s interval); the last is
CPU actually sampled, without extrapolation. Skipped windows do not fill their
gaps with an old phase distribution. `other` retains unlabeled runtime work,
GC, scheduler work, syscalls and any uninstrumented subsystem, so no sampled CPU
is discarded. Background GC is not apportioned to the subsystem allocating it.

Phases have a fixed vocabulary. Receiver phases include transport/decompression
and parsing per protocol. Cache insertion is recognized from its existing stack frames;
writeout includes queue scanning and persister dequeue. Persister workers retain
the existing sampled `persister_stage` labels; CPU is aggregated under `persister`
to avoid presenting its 1-in-8 stage labels as a full stage CPU distribution.
Pebble's native `pebble` labels take precedence and distinguish flush, compaction,
WAL sync, table-cache and garbage cleanup. Request workers label find/render/info;
their newly spawned goroutines inherit those labels. The remaining labels cover
catalog/index work, quota refresh, buckyd transfers, deferred reclaim, metadata
prewarm, restore/input-log replay, migration/backfill, and the phase profiler.
The existing state-cache/record-codec stack frames also identify metadata cache
CPU inside its callers; those samples join `metadata_cache` without adding a hot
label switch to every cache hit. The phase also covers prewarm and membership
loading. Other metadata routing work retains its caller phase.

Labels are set at connection/worker/background entry points and are also useful
in on-demand profiles while the estimator is off. Cache insertion and writeout
queue CPU are resolved from fixed function frames in each sampled stack. The
leafmost recognized frame wins, with Pebble's native labels taking precedence.
No phase checks, clocks, atomics, callback wrappers, pprof.Do calls or context
allocations are added to the per-point path. The existing 1-in-8 persister stage
instrumentation remains shared with its wall-timing metrics. Profile decoding
and accumulation happen on the profiler goroutine after the window ends.

The earlier sampled parse/cache wall and call counters were removed to keep the
hot path clear. Phase CPU is the supported budget; use existing receiver/cache
and persister counters for workload rates.

Reproduce the local checks (each invocation is intentionally bounded):

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go test -v -timeout=120s ./internal/phasecpu
PHASE_CPU_E2E=off GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go test -v -count=1 -timeout=120s ./carbon -run '^TestPhaseCPUEndToEnd$'
PHASE_CPU_E2E=on GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go test -v -count=1 -timeout=120s ./carbon -run '^TestPhaseCPUEndToEnd$'
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go test -run '^$' -timeout=120s ./cache -bench '^BenchmarkIntakeShardContention$' -benchtime=1s -cpu=1,8 -mutexprofile=/tmp/intake.mutex -blockprofile=/tmp/intake.block
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go tool pprof -top /tmp/intake.mutex
```

The contention benchmark bounds its backlog to small batches. It demonstrates
that a hot series serializes producers; it does not identify production lock
owners. Mutex profiles attribute contention to lock holders/unlock stacks; block
profiles show waiting stacks. Cross-check both with CPU, never sum waits into CPU.

## Enable mutex and block samples without restarting

```toml
[pprof]
mutex-profile-fraction = 100
block-profile-rate = 1000000
```

Both settings default to 0 (disabled), are process-wide, and are applied at startup
and on SIGHUP. Fraction 100 samples roughly one in 100 mutex contention events.
The block rate is **nanoseconds of blocking per sample**, so 1000000 means 1ms;
it is not a fraction. Negative values are rejected before changing runtime rates.
The settings work independently of `pprof.enabled`; the HTTP debug listener must
already be available to retrieve profiles. Changing its address or enabling the
listener itself still requires startup configuration. A Prometheus listener also
uses the existing debug HTTP mux.

Start with mutex fraction **100** and block rate **1000000** for a temporary
capture. Reload with SIGHUP, collect a bounded mutex/block profile, then set both
back to 0 and reload. Fraction 1 records every contention event and is for short
controlled investigations; fraction 5 also captures substantially more events than
100. Existing profile samples remain after disabling sampling, so use the pprof
HTTP delta window (`?seconds=30`) or subtract a before profile for a bounded view.
Do not compare cumulative profiles collected over different uptime windows.

`[pprof]` is authoritative for go-carbon, including when buckyd's optional
Pyroscope integration starts. That integration's standalone defaults do not
override the configured go-carbon rates. Changing rates does not enable or stop
CPU profiling; the phase sampler's CPU-profiler ownership rules still apply.

The following small kernel benchmark is a contention diagnostic; use the full
pipeline A/B below for overhead conclusions. It uses a synthetic Graphite series mix (constant, counter,
gauge and floating values, with timestamp jitter, duplicates and late points):

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go test -json -count=5 -timeout=120s ./cache -run '^$' -bench '^BenchmarkIngestProfiles$' -benchtime=500ms -benchmem -cpu=4
```

It compares fractions 0/1/5/100 and block sampling off/1ms, with bounded 128-point
writeouts. Report CPU per point as well as wall time per point: other agents on a
shared machine can change the latter. See the session RESULT for measured values.

## Grafana rows

`tool/fleet-rollout/dash/phase_cpu_rows.json` uses the grafana-lib split-row format:
28 fixed phases, 12/12/4 panels per line, 2x4 grid, per-host CPU and the adjacent
collapsed replica average. The replica query averages per-host results while
retaining cluster and location labels. A ten-minute rate smooths the short CPU
windows. Datasource and selection variables match the reference Pebble spec.

The row DSL replaces existing rows. The companion `phase_cpu_rows.py` seeds a new
collapsed row from the existing process-CPU panel, then delegates layout, query
construction and replica averaging to grafana-lib. Offline preview:

```sh
python3 tool/fleet-rollout/dash/phase_cpu_rows.py --dashboard /tmp/go-carbon-before.json --output /tmp/go-carbon-planned.json
```

After the build is deployed, the same command with `--apply` validates expressions
and uses grafana-lib's version/folder guards and original-document backup before
saving. The current task only generates and checks the offline spec; no Grafana
read or write is needed. The existing style catalogue recognizes `runtime CPU: *`
for a 12-panel grid but does not yet recognize the new `runtime CPU by phase: *`
row names; preserve this spec's explicitly requested 2x4 grid in later style sweeps.

## Pipeline overhead A/B

Build the standalone local harness with its benchmark-only 1µs p99 histogram:

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -tags phasecpubench -o /tmp/phase-cpu-bench ./tool/phase-cpu-bench
python3 tool/phase-cpu-bench/run.py --binary /tmp/phase-cpu-bench --dir /tmp/phase-cpu-fixture --output /tmp/phase-cpu-results
```

The harness seeds and verifies at least one million realistic metric names through
plain TCP into the shared Pebble store. The sender is a separate process, excluded
from server CPU. GOMAXPROCS stays fixed at four. A bounded TCP feedback window
keeps the cache stable while throughput follows the persist pipeline. The driver
interleaves unprofiled controls with phase accounting and independent mutex/block
settings at 1000/100/10, five repetitions each. It records uptime before/after each
60s measurement and waits when load exceeds eight. Contaminated attempts remain
in the raw JSON and are retried. The default phase interval is 60s with one 1s
window, fully inside measurement after five seconds of settling.

The `phasecpubench` tag adds a 512KiB microsecond histogram at the existing update
observation point. The ordinary-build hook is empty and inlined away. This avoids
claiming sub-bucket p99 changes from the production doubling buckets. The results
include CPU seconds per million committed points, persisted/received points per
second, p99 and its exact 1µs bucket bounds, cache backlog, drops, and actual
profile-window coverage. CPU accounting accuracy is a separate check; it is not
an overhead measurement.
