Run only on private local stores, with vendored dependencies:

```sh
GOFLAGS=-mod=vendor GOTOOLCHAIN=go1.27.0 GOPROXY=off go build -o /tmp/metadata-prewarm-bench ./tool/metadata-prewarm-bench
GOMAXPROCS=4 /usr/bin/time -l /tmp/metadata-prewarm-bench -mode build -dir /tmp/prewarm-source
GOMAXPROCS=4 /usr/bin/time -l /tmp/metadata-prewarm-bench -mode age -dir /tmp/prewarm-source
```

The default fixture holds 1M metrics, `1s:2d,60s:28d` retentions (from
`BenchmarkContinuationUpdatePath`), and 30-point batches. Creation order differs
from catalog order. Build compacts the initial data; age writes a newer layer
and adds late points/sidecars to 1/8 of the metrics, then flushes. This models
restart metadata work at scale; it does not model a production store's complete
history, LSM depth, traffic mix, hardware, or filesystem cache misses.

Make a fresh copy of the closed source for each mode (APFS example):

```sh
cp -cR /tmp/prewarm-source /tmp/prewarm-cold
GOMAXPROCS=4 /usr/bin/time -l /tmp/metadata-prewarm-bench -mode steady -metadata-bytes 4294967296 -dir /tmp/prewarm-cold
cp -cR /tmp/prewarm-source /tmp/prewarm-on
GOMAXPROCS=4 /usr/bin/time -l /tmp/metadata-prewarm-bench -mode prewarm -metadata-bytes 4294967296 -dir /tmp/prewarm-on
cp -cR /tmp/prewarm-source /tmp/prewarm-background
GOMAXPROCS=4 /usr/bin/time -l /tmp/metadata-prewarm-bench -mode background -metadata-bytes 4294967296 -dir /tmp/prewarm-background
```

`steady` reports a cold fill cycle, then a second cycle with write-through state.
`prewarm` finishes a paced catalog prewarm before the first cycle to measure
its coverage and cost. `background` overlaps that same prewarm with the first
cycle; `-gate 120s` waits for prewarm or that deadline before starting writes. `cold` runs only the cold cycle.
`-workers` controls bounded catalog concurrency (zero uses GOMAXPROCS/12,
clamped to 2..16). `-rate` and `-byte-rate`
cap aggregate reads; both default to zero (unpaced), matching startup.
`-writers 8 -window 120s` runs eight concurrent writers with a minimum cycle
length; the window starts after any gate. It does not simulate intake during
the gate or an unpaced writeout burst when a backlog is released.
`-floor` primes metadata read-only before measuring the identical first update;
it avoids using a later update with different tail/rollup work as the floor.
`-pending-catalog` selects pending names through the catalog membership filter;
`-priority` retains the targeted restored-name path for comparison.
Each `sample` row reports one-second process CPU deltas, update counts, stage
seconds and cumulative block-cache counters. Divide CPU by `interval` to get
cores; exclude the final short interval when comparing one-second peaks. Each cycle updates every metric exactly once, using
`Store.Metadata` for routing and the ordinary stage sampler (1/8 updates).
Stage seconds are sums across updates; CPU is process user+system time, including
concurrent compaction. Combined CPU/wall includes prewarm and updates; do not add
overlapping background rows. Logical reads count store lookups/prefix walks,
including the prewarm when concurrent, rather than physical I/O. `/usr/bin/time`
includes open/close and reports process lifetime peak RSS. A fresh process gives
cold Pebble caches; the OS page cache is left intact.

`-mode backlog -window 120s -gate 80s -adaptive-gate -pending-catalog -pending-every 8`
queues 30 points for every metric, then drives the real cache queue, startup
coordinator, rate ticker and persister through prewarm and the entire catch-up.
One in eight names has first-pass priority; the remaining catalog finishes too.
The cache-owned point backlog is included in memory/CPU samples. `-gate` is the
adaptive cap here; omit `-adaptive-gate` for the fixed deadline. The default update
limit is metrics/window. `-updates-per-second` sets an independent limit; use a
limit above that steady rate to expose deadline-induced bursts. Batching defaults
to 2000 points and an ordinary delay spread of 0.25, with explicit startup/restore
spreading off (`-backlog-min-points`, `-backlog-delay-spread`,
`-backlog-startup-spread`). Held work must still span the full release-relative
window with those production settings. `-legacy-catchup` keeps process-start phases and models the
old pending-recovery rate lift. `-floor` primes metadata before measurement and
runs the same backlog/first input without a scan. Output includes gate-release
and drained records; all metrics must be confirmed, and sixteen stored series
are checked against every input point. This remains a scaled local fixture,
not a model of the fleet's full ingestion distribution or retention history.

For example, use `-mode backlog -window 120s -gate 30s
-updates-per-second 41667` to drain 1M metrics at a nominal 8333 updates/s while
the configured ceiling allows five times that rate. `backlog-drain-profile`
reports time to the first confirmed write and per-second/minute confirmed writes
relative to gate release. Sum every consecutive 60 seconds to check rolling
minute peaks; fixed minute buckets alone can hide a burst across a boundary.

`-background-cores 1 -cpu-cores 1` consumes the whole optional CPU allowance
with mandatory in-process load. Use `-floor -warm-timeout 45s -gate 5s` to start
with warm metadata while a real, optional catalog scan remains running across
gate release, then ends at 45s. Writeout must keep draining in both phases.
The sampler includes consumer CPU grants, admitted jobs, confirmed update rate
and its floor. While warming is unfinished, writeout and optional warming share finite
process CPU credit, with writeout taking priority. After warming finishes,
writeout returns to its ordinary limits. Unavoidable background CPU needs
headroom within the allowance; a throughput shortfall warns after ten seconds. This is an admission policy, not per-consumer CPU attribution.
`-backlog-stop-after 170s -observe 180s` bounds a failing starvation control and
reports its remaining points rather than waiting indefinitely for a full drain.
Use fresh clones, GOMAXPROCS=4 and a process timeout below 5min for each case.

`-mode pending-fixture` creates a predecessor's real block-compressed cache
dump, compact captured input and published recovery index. Run it after cloning
the store: the manifest attests that clone's path/device/inode. By default it
saves 92 points/metric, with about 680 new read-index names per million.

`-mode backlog -pending-restore -backlog-min-points 960 -window 120s -gate 30s`
then attaches that checkpoint with prepared names, starts the real queue before
recovery, calls `RecoverPending` alongside live claims, and admits 90 more
points/metric during the gate. It uses noop ordering, restore-spread on,
max-size 10 billion points, a real input log (max-age twice the window), and
rare recovered/dump arrivals with duplicate timestamps. Input is paced in
4096-metric chunks over 90% of the gate. This scaled million-metric case holds
182 million points and averages about 3.3M live points/s while gated. The
preparer is a separate process, so its dump cost belongs to the predecessor.
Use `-restore-points` and `-gate-input-points` to vary those counts.

Metadata can be primed with `-floor` to isolate queue liveness from cold reads;
`-no-prewarm` then skips the optional metadata scan while the gate still holds
recovery/intake. `-checkpoint` exercises the first-restart missing metadata
snapshot and saves a new one at close. Completion checks pending-source
retirement and every restored/live value for sixteen stored series.

`-mode restore -checkpoint` measures checkpoint loading without an update cycle;
`-mode restore` measures a fresh catalog walk. `-workers` and `-batch` also apply
to eager checkpoint validation (default batch 1024, bounded at 4096), with one
read view and revision iterator per batch. `-lazy-checkpoint` streams provisional
records and defers validation to first use, so compare its first update too.
Use identical closed-store clones in fresh processes, with the store larger
than `-block-bytes`. On systems where the filesystem page cache cannot be
cleared, report that limit; fresh processes only guarantee cold Pebble caches.
Checkpointing and lazy validation are both explicit opt-ins in the service.
