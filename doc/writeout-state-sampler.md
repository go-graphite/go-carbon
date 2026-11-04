# Automatic writeout state sampling

The automatic rate controller samples `Cache.WriteoutState` every five seconds.
Sampling reads each shard's metric count, published point count plus unpublished
delta, and two counted time indexes. It never reads queued point buffers or walks
metric slots during ordinary polling. The controller and its locking boundary are
unchanged: the state callback runs outside `autoUpdate.mu`.

Each index groups seconds relative to cache creation into 256-counter pages.
Using the same monotonic clock as arrivals preserves residence time across wall
clock corrections. Eight directly addressed
pages cover the ordinary time window; colliding pages use a compressed binary
radix tree whose branches carry subtree totals. A prefix count or minimum inspects
eight direct pages and at most 56 branches and 512 counters, independent of metric
count, configured delay, and how
long overdue work remains cached. Thus sampling is O(shards), including when the
writeout queue is stalled. Nodes and empty pages are reused; memory accounting
includes retained index storage.

The deadline index counts items by their batching deadline. The arrival index
counts their first residence times, excluding untracked restored items. Insertion,
eviction, pop, requeue/merge, and recovery claims update both indexes under the
existing shard lock. Appending points only changes a deadline at a point threshold,
a sparse-to-dense transition, or an immediate restore. Ordinary appends do no tree
work. Exact queued point totals reuse the existing batched size accounting, adding
no counter update to the ordinary append path. Ordinary items carry no sampler metadata.

## Precision and controller behavior

Metrics and points are exact for each shard as it is sampled. As before, a sample
does not represent one atomic instant across all shards under concurrent mutation.
For a stable cache, at observation time `t`:

* `exactEligible(t - 1s) <= Eligible(t) <= exactEligible(t)`. Nonzero deadlines round
  up to the next whole second; immediate eligibility remains immediate. Only
  deadlines in the last second can be omitted, and no item is reported early.
* `exactOldestAge(t) <= OldestAge(t) < exactOldestAge(t) + 1s`. First arrival rounds
  down. Sample timestamps, including historical and future points, do not affect
  residence time.
* A paused startup reports no non-immediate eligibility and zero residence age.
  Release shifts the query clock; it does not rebuild the indexes.

The steady-demand, growth, capacity-pressure, floor, and ceiling inputs remain
exact. Only the deadline/recovery terms can differ. If `U` deadlines fall in the
last second and `L >= 5s` is the original remaining catch-up window, the deadline
term lies between `(E-U)/L` and `E/max(5s,L-1s)`. This is a time/count bound, not
a universal percentage bound for synchronized bursts. At integral-second sample
and arrival times both counts and ages are exact. The existing steady-state and
restart simulations retain their rates/CVs; cache-driven automatic reloads retain
their minute-by-minute writes. No queue eligibility decision uses these rounded
indexes: `WriteoutReady` still checks the exact deadline.

## Settings reloads

Changing minimum points, maximum delay, spread, phase, sparse behavior, startup
behavior, or the startup clock requires a one-off O(metrics) reindex on the next
sample. Capacity, tags, input-log diversion, restore-spread settings, and startup
release do not require it. Reloads preserve the existing items' arrival times.

A replacement index visits at most 32 hash-table slots per shard lock acquisition,
then releases the lock. A temporary per-shard set identifies copied items; concurrent
insertions, updates, removals, and requeues mirror their changes into the replacement.
If the table rehashes, the cursor restarts and the set suppresses duplicates. The
completed replacement is installed under the shard lock. Samplers serialize with
each other while ingest and writeout continue between rebuild batches.

## Reproducing the checks

Run with the repository toolchain and vendored dependencies. Put `TMPDIR`,
`GOTMPDIR`, and `GOCACHE` in a scratch directory with sufficient free space.

```sh
go test ./cache -run 'Test(WriteoutState|WriteoutIndex|TimeCounts)' -count=1
go test ./persister -run 'TestAutoUpdate(Simulation|RestartSimulation|.*Cache.*|ScanDoesNotBlockReaders)$' -v -count=1
go test ./cache -run '^$' -bench '^BenchmarkSamplerIngest$' -benchmem -count=12
SAMPLER_METRICS=4000000 go test ./cache -run '^TestWriteoutStateLoad$' -v -count=1
```

The opt-in resident fixture has 1024 shards and 17 points per metric, with first
arrivals spread across a 1200-second window. It measures idle sample wall/process
CPU time, shard work duration, concurrent ingest and pop/confirm/reinsert throughput,
and writer lock probes with/without sampling. Its 100ms poll interval deliberately
amplifies sampler contention; the application still polls every five seconds.
`SAMPLER_PROFILE` optionally records CPU during idle sampling, excluding fixture
construction and concurrent workers. `SAMPLER_RELOAD=1` also times one batching
reload after the load phases. Normal tests allocate no resident fixture.
