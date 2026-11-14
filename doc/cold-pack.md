# Cold continuation packing

Cold packing is an optional background maintenance task for exact, incremental,
time-bucketed shared Pebble storage. The default is off. It joins existing
128-point continuation groups into 4096-point sealed groups after their newest
sample leaves a configurable write window. The normal ingestion writer stays
exact. Native archive boundaries, aggregation, XFilesFactor, authoritative writer
headers, live propagation buffers and sidecars stay under the existing writer.

Use exact mode for strict numerical requirements, including aggregated datasets.
Decimal mode can save more space for sparse and dense floating-point datasets,
with the precision contract below. Compact headers and live decimal writes are
not part of this feature.

## Stored point counts

`store_points_stored` is the sum of native range descriptor point counts for
fully counted metrics in the last completed cold-pack pass.
`store_points_stored_metrics` reports how many metrics contributed, including
empty compressed metrics. `store_points_stored_timestamp_seconds` is that pass's
completion time, or zero when no completed count is available. All three are
gauges; an unfinished pass leaves the previous result visible. A dashboard can
divide `pebble_disk_bytes` by `store_points_stored` when the denominator is
positive, while checking coverage and freshness alongside the ratio.

Every native descriptor consumed by exact, decimal or unpack maintenance
contributes, including hot, already sealed, legacy non-bucketed and oversized
ranges that are not rewritten. Page read-ahead is counted only when the cursor
consumes it. Metrics with errors, read-budget exhaustion or publication conflicts
are excluded in their entirety, as are unsupported layouts and unpublished
migration candidates. Coverage counts metrics, not the percentage of stored
points: a missing large metric can still materially bias the ratio.

Counts are exact for the descriptors observed, rather than a simultaneous
whole-store snapshot: ongoing writes, expiration and range reuse can change
earlier observations before a pass finishes. Live propagation buffers, sidecars,
pending ingestion and non-native storage are excluded. Native points still
present physically count even if they are outside the current fetch window.
There are no new ingestion operations or descriptor reads. Maintenance adds
integer accumulation and fixed-size counters to its existing bounded page.

Totals and the matching contiguous cursor progress share the existing checkpoint
record and sync schedule. Compatible restarts restore the last completed result
immediately and resume the unfinished count. Later parallel completions replay
without double counting. A new store or an incompatible/missing checkpoint starts
at zero until a pass completes. An older checkpoint without point counts can
provide only partial coverage on its first resumed pass; the next full pass
counts all supported metrics. Changing mode or write window resets unfinished
counts while retaining the last completed result within the running process.

Cold packing defaults to off; its write window does not prevent descriptor
counting, but its interval and pressure gates delay fresh results. With cold
packing disabled, no existing guaranteed descriptor-reading sweep is available:
metric expiration may inspect catalog metadata alone. No additional scan or
sampler runs for these gauges. The cheapest independent alternative would be a
clearly labelled estimate from a bounded catalog sample: for example, start at
most 64 randomly selected metrics per hour, with a hard 2 MiB/hour total read
budget, the same 32-descriptor pages, and CPU pacing. Unfinished samples would
carry over under the same budget. It would need metric selection and coverage
weighting, and could be biased by very uneven metric sizes. Its useful accuracy
and total cost have not been established, so it is not enabled automatically.

## Configuration and reload

In `[whisper]`:

```toml
store-cold-pack = "off" # off, exact, decimal, unpack
store-cold-pack-tails = false # whole cold stranded native heads; v9 compatible
store-cold-pack-workers = 1
store-cold-pack-bytes-per-second = 1048576
store-cold-pack-cpu-fraction = 0.02
store-cold-pack-write-window = "24h"
store-cold-pack-interval = "1h"
store-cold-pack-min-free-bytes = 1073741824
store-cold-pack-max-compaction-debt = 67108864
```

Enabling requires `storage-backend = "pebble-chunk"`, `store-encoding = "exact"`,
`store-incremental-migration = true` and `store-time-buckets = true`, plus their
normal prerequisites. Zero or omitted budgets select the defaults above. CPU
fraction must be between 0.001 and 1, and workers between 1 and 64 (zero selects
one worker). Byte rate must be positive. The write window
must be nonnegative and the sweep interval at least one second. Zero durations
select defaults. All settings reload on SIGHUP; invalid settings fail the
reload. Turning off joins all current bounded operations and prevents subsequent
packing. It retains the upgraded reader fence and existing sealed data.

By default, only time-bucketed (v7 layout) blocks larger than one 128-point
group are eligible. Relayout moves older native blocks into the history bucket,
but leaves each archive's newest block metric-prefixed. A series that stops
writing can leave this newest native block stranded until expiration.

`store-cold-pack-tails = true` additionally packs that non-bucketed newest block
when its whole archive is cold. It reads authoritative writer headers, stored
block/encoder/buffer bounds and pending rollup windows from finer buffers. An
unknown bound, or a sample interval reaching the write-window cutoff, prevents
packing. The native tail's last point must also precede the cutoff. Short tails
of 128 or fewer points are allowed, subject to the same strict encoded-size
benefit check, 65,536-point maximum, read/batch budgets and revision fence as
other packing. The complete native range moves atomically into the existing
history bucket with ordinary v9 sealed frames and the existing packed descriptor.
There are no v10 keys or fence. Release v9 readers/writers can reopen the store,
correct or resume the series through the existing thaw path, and reset reused
circular slots. Active long-step heads remain ordinary; closed-prefix packing is
outside this option.

This option defaults to false and reloads on SIGHUP. Changing it resets unfinished
sweep progress and its compatible checkpoint policy so newly eligible tails are
visited. It has no effect while `store-cold-pack = "off"`. Packing uses the chosen
exact/decimal mode and existing cold-pack counters. Disabling the option leaves
already sealed tails readable; `unpack` handles them even with tails disabled.

`unpack` restores ordinary 128-point groups. This is also paced, and leaves the
reader fence in place during online use. Changing exact/decimal/unpack policy
restarts the cursor. Worker-count and budget changes retain the cursor and join
the old worker group before the new policy can publish. Decimal mode does not
rewrite blocks already sealed in exact mode; explicitly unpack first if changing existing cold blocks is desired.

## Resource bounds and publication

The worker group seeks through catalog pages of at most 64 names. It keeps only
one ordered page and assigns up to `store-cold-pack-workers` distinct unfinished
metric names per wave. Each worker seeks through at most 32 stored physical
descriptors per step and prepares at most one native range per transaction.
The wave joins before admitting more work, pacing, checkpointing or reloading.
An uneven page can use fewer workers while its last metrics finish. The sweep
never collects or sorts the whole catalog and never probes unallocated
descriptor slots. The default path reads manifests for layout without loading authoritative
header pages; the tails option additionally reads the header and buffer bounds.
Native ranges above 65536 points are skipped and counted. A decoded native range holds at most 1 MiB of points. Snapshot reads are
limited to 2 MiB and mutations to 256 KiB plus the revision record. Oversized
headers/ranges are skipped rather than splitting a native writer boundary.

Unpublished migration candidates are ignored until ownership is published.
A metric failure logs at ERROR through the `storage` zap logger, including its
name, descriptor cursor and error. It increments both `store_cold_pack_errors_total`
and `store_cold_pack_skipped_total`, then advances past that metric. The next
pass retries from descriptor zero. Cancellation retains unfinished work;
store-wide failures (for example a checkpoint failure) still pause the worker.
`store_cold_pack_failed` latches any non-cancellation error in this process and
stays set until restart, including when later metrics and passes succeed.
Benign publication conflicts and bounded skips keep the pass dirty and retry on
the next sweep without setting that gauge. Their most recent reason is retained
in `ColdPackStats.LastIncomplete`; `ColdPackStats.Error` records real failures only.
An offline `RunColdPack` completes the pass before returning a metric failure.
Any skip, conflict or failure prevents that unpack pass from authorizing downgrade.

Relayout also logs and skips metric planning and apply-preparation failures, counts them in
`store_relayout_errors_total`, finishes the other metrics, and retries after an
hour in the background. Its durable cursor stays before failed work until the
pass ends, then resets for a rescan; a restart cannot forget failed metrics.
Already moved blocks need no new copies. `store_relayout_failed` also latches
until restart. Store-wide ingestion, publication, compaction and cursor failures
abort that run and are logged through zap before retrying.

Snapshot reads, decompression and encoding happen outside the metric stripe.
Publication uses TryLock, rechecks ID, generation and revision, and abandons a
stale plan. A busy stripe is retried in a later sweep. Metadata validation that
already consumed 2 ms abandons publication. The only remaining locked work is
one bounded WAL batch and cache invalidation. Source groups are removed with bounded point deletes; maintenance adds no range tombstones to the shared memtable fragmentation cache. Reads use consistent snapshots and
never wait for this stripe. A late correction or append atomically thaws the
native range before following the existing exact writer and rollup path.

The shared byte pacer charges total snapshot payload reads (including failed
plans) and committed WAL batch bytes across all workers. Raising workers does
not multiply the byte rate. The CPU fraction is the duty fraction of the worker
group: `workers * cpu-fraction` is its conservative core budget. For example,
8 workers at 0.25 allow up to 2 cores; 8 at 1.0 allow up to 8. The pacer charges
the entire wave wall time, including IO waits and any worker imbalance, so this
may underuse that budget. With the default one worker the pacing is unchanged.
It sleeps outside locks and snapshots; idle time does not earn a later burst. Short steps share accumulated pacing debt: waits below 10 ms are
deferred until that debt reaches 10 ms, avoiding a timer wakeup for each empty
metric or descriptor scan. The allowed burst is one bounded step per worker plus
less than 10 ms of shared budget debt. Memory and transaction headroom scale
with workers. CPU and byte debt are tracked independently and actual sleep
time repays both, including timer overshoot. Reload and shutdown wake a
sleeping worker. This is a conservative worker budget, not a process-wide CPU
ceiling: asynchronous Pebble flushes, compactions and GC are additional work.
Existing compaction pacing remains independently configurable.

Maintenance pauses when Pebble's estimated compaction debt exceeds
`store-cold-pack-max-compaction-debt` (64 MiB by default), two memtables are full,
L0 reaches half the write-stop threshold (at least two sublevels), or the free
disk reserve plus 8 MiB per configured worker cannot be maintained. All workers
stop admission at the next wave boundary when any gate fails. These are admission
checks, not reservations against concurrent writers. A busy store that normally carries
several GiB of debt pauses on every step at the default; raise the threshold to
just above its usual debt, and watch `store_cold_pack_pressure_pauses_total`.
WAL, flush and compaction output require
transient disk space before old tables are reclaimed. Bounded publication cannot
place a hard deadline on filesystem syncs or a Pebble write stall that starts
between admission and commit; monitor the maximum lock metric and read latency.

The sweep keeps an advisory checkpoint in Pebble's own metadata keyspace at
`z/cold-pack-cursor`. It stores the low watermark metric name, descriptor cursor, completed
pass count, metrics visited in this pass and any unpack-pass failure. The
versioned record has a checksum. It is WAL-synced at bounded step boundaries
no more often than every 30 seconds, with additional checkpoints on pass
completion, configuration changes and clean stop. Unchanged state is not
rewritten. Checkpointing runs outside metric stripes and never on the ingestion
write path. Stores that have never enabled cold packing need no checkpoint writes.

Workers can finish out of order. Only the contiguous completed prefix advances
the name watermark and visited count; the oldest unfinished metric may retain
its last published descriptor cursor. Later completed names remain in the page
until that prefix finishes. A crash can replay them, and never skips the oldest
unfinished metric. A pass completes only after every page entry finishes and
the subsequent catalog seek is empty. Checkpoints use the same version-1 record
as serial sweeps; changing workers preserves compatibility.

Restart resumes when the saved mode, write window and `CHUNKSTORE` format marker
match. Changes among off/exact/decimal/unpack, write-window changes, marker
changes, or missing/corrupt/unknown-version checkpoints start from the beginning.
Turning off invalidates the prior position even if no worker step runs. Byte/CPU
budgets, pressure limits and the sweep interval can change without discarding
progress. Corrupt and unknown-version checkpoints emit a warning.

Syncing a checkpoint also makes all preceding packed-range publications durable,
including when ordinary writes use periodic WAL sync. A crash may replay work
since the last checkpoint; a checkpoint cannot get ahead of published ranges.
Every range remains independently atomic and readable, and repeated work is
idempotent. The unsaved tail can exceed 30 seconds during a long bounded step or
pacing/pressure wait. A deleted cursor metric continues at the next catalog key.
New/recreated metrics and corrections behind the cursor are visited on a later
sweep. Unpack failures persist across restart; a resumed failed pass cannot
authorize downgrade. The interval is a pause after the complete paged sweep,
not after every page; the between-pass timer itself is not persisted.

## Exact and decimal values

Exact mode preserves every float64 bit, including signed zero and NaN payloads.
Decimal mode tries twelve significant decimal digits with a shared exponent and
keeps the exact encoding unless the complete decimal block is smaller. Each
finite nonzero value is checked after reconstruction: relative error per encode
must be at most **5.1e-12**. Timestamps, point counts and archive layout are exact.
NaN, infinity, negative zero, subnormals and unsafe dynamic ranges fall back to
exact encoding; zero stays exact.

There is no global absolute-error bound. Large integer-like sentinel values may
change by millions while remaining within the relative bound. Repeated codec
cycles are tested, but arbitrary sequences of updates, rounding and aggregation
have no universal relative-error guarantee: cancellation can amplify errors.
Keep decimal mode off wherever this contract is unsuitable. Unpacking cannot
recover original values rounded during earlier decimal sealing.

## Compatibility, transfers and downgrade

Native exact combine can raise the minimum reader to v12 for immutable encoded
history references. That fence cannot be lowered by cold-pack or float-codec
unpack/downgrade. Keep a reference-capable reader for serving and recovery after
such a publication; see [Native history combine](native-combine.md).

Enabling exact or decimal packing synchronously upgrades `CHUNKSTORE` to
`go-carbon-chunks-v9` before any new block can publish. Upstream v6/v7 and
experimental v8 builds refuse this marker. Upgrade all readers, recovery tools and
transfer peers before enabling. Readers from this change understand both sealed
encodings even with maintenance off. A failed or interrupted upgrade can leave
an unnecessary v9 fence, but cannot expose v9 blocks under an older fence.

Per-metric exports require a receiver advertising `store_format=9`; the response
includes `X-Store-Format: 9`. The pull path advertises the target's current store
format, so upgrade/enable the target first. A v7 target rejects sealed descriptors
before ingestion. Ordinary v7 transfers retain their existing wire v1/v2 framing.
Whole-store copy clients advertise `X-Accept-Store-Format: 9`; v9 checkpoint
manifests use version 2 and carry `store_format`. Old clients are refused, and
also reject version 2 manifests. Checkpoint creation serializes with format
changes so the marker cannot understate its data's format.

To return to an older upstream build, stop the server, preserve its normal
backup/input-log recovery procedure, and use an exclusively opened offline store:

```sh
make storectl
./storectl maintain cold-pack -store /var/lib/carbon/store -source /var/lib/carbon/whisper \
  -mode unpack -downgrade
```

Omit `-source` for stores without an incremental source binding. Keep the server
stopped until the command exits successfully. The tool shares the background
worker's pacing, disk reserve and transaction limits; flags allow changing the
budgets, including `-workers` (default 1, maximum 64). After a complete clean
unpack sweep it flushes the WAL, then atomically syncs the v7 marker. Any skip, conflict, failure or interruption prevents that
marker change. Retry after correcting the cause. A crash before the marker switch
leaves a readable v9 store; a crash after it leaves only v7-readable blocks.
Never edit the marker manually. An interrupted unpack can be resumed safely.

## Observability and qualification

`store_cold_pack_*` Prometheus metrics expose blocks, points, logical input/output
bytes, snapshot reads, WAL batch bytes, pages, completed sweeps, skips, conflicts,
pressure pauses, worker seconds, maximum stripe lock duration, active work and a
latched failure flag. Logical input minus output is the encoded byte saving,
not immediate filesystem reclamation. `active_workers` counts executing metric
ranges; `active` covers the whole wave.

Lifecycle counters separate sealing from restoring writable groups:

| Series | Meaning |
| --- | --- |
| `store_cold_pack_blocks_total`, `store_cold_pack_points_total` | All sealing work, including repacks; administrative unpacking is excluded. |
| `store_cold_pack_repacked_blocks_total`, `store_cold_pack_repacked_points_total` | The subset sealed after a previous thaw. |
| `store_cold_pack_repacked_input_bytes_total`, `store_cold_pack_repacked_output_bytes_total` | Logical bytes before/after sealing that subset, with the same accounting as the existing input/output counters. |
| `store_cold_pack_thaws_total{reason}` | Completed codec thaws by writer entrypoint: `append` (append to an existing range), `patch` (replace stored values), `rewrite` (rewrite a range suffix), `unpack` (administrative unpack mode). Patches/rewrites can come from late writes or rollup correction. |
| `store_cold_pack_thawed_points_total` | Points restored from sealed groups to ordinary 128-point groups. |
| `store_cold_pack_thawed_bytes_total` | Sealed group bytes read plus ordinary group bytes rewritten; includes logical group keys/values, excludes descriptor bytes and batch framing. |

First packs are `blocks_total - repacked_blocks_total`, with the full
`store_cold_pack_` prefix on both names (similarly for points and input/output
bytes). Before these lifecycle counters, blocks/points also included unpacking;
avoid mixing versions in a comparison window. The administrative status API
continues to report all maintenance blocks/points, with unpacked subsets.
Sealing/repacking counters count published batches. Thaw counters count completed
codec work in this process across stores; a later error, revision conflict or
transaction failure can discard that work. They are work rates, not a durable
count of currently thawed ranges. Circular slot reuse replaces the old range
directly and performs no thaw.

Thaw history is durable without extending the descriptor: thaw sets bit 31 of
its bucket and writes ordinary groups under that full bucket address. Repacking
keeps it; replacing the range from its first point with a fresh descriptor clears
it. Hour/day buckets cannot naturally reach that bit within Whisper's uint32
timestamp domain. Old v7/v9 decoders preserve all bucket bits and route reads,
writes, transfers and deletions to the same address, so rollback to a v9-capable
binary is safe. Do not mask the bit when constructing group keys. The existing
unpack/downgrade procedure still applies to binaries predating v9. Thaws performed
by an older binary do not gain the marker and cannot later be identified as
repacks; data correctness is unaffected. Ordinary Update paths add no metric
operations, IO, locks or allocations.

`pending_metrics` counts unfinished names in the discovered page, including
in-flight names, not an estimated full-store backlog. The administrative status API
also includes the mode, name/descriptor cursor, counters, most recent error and
incomplete reason. The offline tool reports both `last_error` and `last_incomplete`
when retries cannot complete a clean unpack and downgrade.
Errors are logged with `ERROR`; successful sweeps log cumulative progress.
`store_cold_pack_resumed_total` counts restored positions in this process and
resume logs one `INFO` line with mode, metric, descriptor and pass position.
`store_cold_pack_metrics_visited` counts completed catalog entries in the current
pass, including restored progress; it resets at the end of a pass or a policy
change that restarts the cursor. `store_cold_pack_persisted_metrics_visited` shows
that count at the latest durable checkpoint, and
`store_cold_pack_checkpoints_total` counts checkpoint writes in this process.
Completed pass counts survive compatible restarts; other work counters remain
process-local. Visited counts indicate progress through the catalog, not an
estimated percentage or the number of metrics with eligible blocks.

Fast oracle tests cover exact/decimal values, corruption and allocation bounds,
physical mutation paths, atomic failed plans, crash/restart, concurrent ingestion,
corrections/rollups, revision conflicts, paging, pacing, reload and transfer fences.
Use `GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor GOMAXPROCS=4` and a per-package
`-timeout=150s`; race checks can use two processors. The optional
`TestColdPackCorpus` in `internal/storemaint/coldpack/bench_test.go` operates on private clones
of settled ordinary corpus stores and writes a JSON receipt. Set
`COLD_PACK_BENCH_DIR`, `COLD_PACK_BENCH_RESULT`, `COLD_PACK_BENCH_CLOCK` and
`COLD_PACK_BENCH_MODE`. It includes a full archive oracle, a twenty-series native
one-hour read mix before/during/after paced maintenance, an isolated CPU trial,
and fresh tiny-block-cache reads. OS cache eviction is not simulated. For a time-matched
control, `TestColdPackPairedReads` uses independent reader processes with separate
GC heaps, identical requests and alternating request order throughout a full
paced sweep. Set `COLD_PACK_PAIRED_DIR` to a private ordinary clone, plus the same
clock, mode and result variables. Its `reference` directory and peer logs are
created beside the clone. This optional qualification is separate from fast
package tests; each process has a bounded benchmark deadline.

`TestColdPackSyntheticSweep` builds a settled v7 store without a corpus. Set
`COLD_PACK_SWEEP_METRICS=4096` to opt in: fifteen of every sixteen metrics have
only 64 points, and the others have 1800 points, all outside the write window.
Each trial starts from a fresh checkpoint, runs the real paced driver, then
compares every archive bit-for-bit against the ordinary fixture. Receipts include
points/s, process CPU, worker time, disk-reserve sampling and codec time.
`COLD_PACK_SWEEP_RESULT` selects a JSON output path; `COLD_PACK_SWEEP_PROFILE`
selects a prefix for per-trial CPU profiles and execution traces, excluding setup
and the oracle. `COLD_PACK_SWEEP_TRIAL` selects `raised` (8 MiB/s, 0.25 CPU),
`default` (1 MiB/s, 0.02 CPU), or `unlimited`; unset runs all three. Larger
fixtures should run one trial per invocation to keep the 150-second test limit.
Keep `TMPDIR` and `GOTMPDIR` in a private scratch directory.

For a dense scaling trial, set `COLD_PACK_SWEEP_DENSE=1`,
`COLD_PACK_SWEEP_TRIAL=unlimited` and `COLD_PACK_SWEEP_WORKERS` to 1, 2, 4,
8 or 16. Every metric then has 65536 points (about ten native blocks), and
receipts include metrics/s, blocks/s, conflicts and pressure pauses. Dense trials
use 64 MiB memtables and a 32-sublevel write-stop threshold so the sparse
fixture's tiny buffers do not dominate scaling. Expected publication conflicts
are reported and retain the normal next-pass retry semantics; they do not
change the full readback oracle. Use one worker count per invocation
and check disk space before creating fixtures; clones and fixtures are removed
by the test. `COLD_PACK_SWEEP_METRICS=512` is a useful local trial size.

Before a canary, validate the actual retained cold fraction and free-space
headroom. Start exact mode on one representative host with ample disk and low
budgets; compare matched read p50/p99, accepted/persisted points, correction errors,
WAL/flush/compaction rates, CPU, disk and maximum lock duration. Exercise restart,
transfer and unpack using backups before upgrading additional hosts. Turn off on
read regressions, new errors or reserve pressure; stopping maintenance does not
require downgrading the binary. Qualify decimal mode separately against the
application's numeric requirements and longer-history/churn workloads.

### Exhaustive float selection

`store-cold-pack-float-exhaustive = true` requires
`store-cold-pack-float-codec = true` and defaults to false. It compares all
existing v11 value codecs on each eligible complete group, retaining the
smaller original frame when repacking. It also revisits existing v11 frames
in `float-repack` mode. Selection runs only in the paced maintenance worker;
`store-cold-pack-cpu-fraction` charges the extra encode time to that worker's
duty budget. Normal updates and decoders are unchanged. Changing the selector
invalidates the saved sweep cursor. The offline tool accepts
`-float-codec -float-exhaustive`. Disable the selector to restore prefix
selection for future packing; existing v11 frames require no conversion.

## Production measurements: 2–4 November 2026

Historical observations from a production fleet. D1–D14 are anonymous dataset
labels, and store labels identify comparisons only. Values describe the measured
release and workload, rather than current defaults or universal savings.

Measured 4 Nov 2026 ~10:15 CET unless stated otherwise. Window for disk changes:
3 Nov 09:00 (just before the rollout) → 4 Nov ~10:15.

### What cold-pack does

Packing works one metric block at a time. A block is one archive's native time
range of one metric. A packed D1 block averages about 13,400 points.

Ordinary (v7, time-bucketed) data is stored as 128-point groups, each under its
own Pebble key. Cold-pack rewrites a block's history into 4,096-point sealed
groups with a stronger encoding. The rewrite and the deletion of the old group
keys happen in one atomic batch, and the rewrite is only committed if the new key
plus value bytes are smaller than the old ones. Only time-bucketed blocks larger
than one 128-point group are eligible; ranges above 65,536 points are skipped.

### Per-block "metadata", before and after

From the layout in the code (`internal/whisperio`, `internal/chunkstore/sealed.go`).

| Item | Before (128-point groups) | After (4,096-point groups) |
|---|---|---|
| Block descriptor | ~33 B key + 8 B value (point count, time bucket) | ~33 B key + 12 B value (adds "packed, groups of 4,096") |
| Group keys per 13,356-point D1 block | ~105 keys × ~42 B (store prefix, time bucket, metric id, archive, block, group number) | ~4 keys × ~42 B |
| Per-group header | 6 B (version, flags, CRC32) | 16 B (version, flags, count, first timestamp, step, CRC) |
| Per-group slot bitmap | 16 B (which of the 128 slots are filled) | none |
| **Overhead per D1 block** | **≈ 6.8 KB** (105 × 64 B + descriptor) | **≈ 0.28 KB** (4 × 58 B + descriptor) |
| Timestamps | one delta varint per point (1 B at a regular step), then zstd if the body is ≥ 128 B | one run (start + step) for regular series |
| Values | integer delta varints, or 8 float byte planes | constant run, linear run, or integer delta / delta-of-delta varints; other floats as 8 byte planes; zstd over the body |

Every 128 points paid about 64 bytes of key, header and bitmap before any point
data. Cold-pack does **not** change the per-metric metadata records (writer
header pages, about 1.2–4.5 KB per metric) or the metadata cache; those were
reduced separately by metadata pack (`store-metadata-cache-pack`).

### Measured per block (last 24 h, all cold-pack hosts)

Logical key + value bytes before Pebble's own compression, from
`increase(store_cold_pack_{blocks,points,input_bytes,output_bytes}_total[24h])`.

| Cluster | Hosts | Blocks packed | Points per block | Before → after per block | Before → after per point | Cut |
|---|---|---|---|---|---|---|
| D1 | 18 | 1,417M | 13,356 | 18.6 → 10.2 KB | 1.39 → 0.77 B/pt | −45% |
| D2 | store-D2-1 | 64.8M | 14,288 | 13.9 → 6.1 KB | 0.97 → 0.43 | −56% |
| D3 | store-D3-1 | 74.0M | 14,612 | 16.1 → 7.7 KB | 1.10 → 0.53 | −52% |
| D8 | store-D8-1 | 65.6M | 15,936 | 17.1 → 7.5 KB | 1.07 → 0.47 | −56% |
| D5 | store-D5-1 | 46.7M | 11,931 | 17.4 → 9.9 KB | 1.46 → 0.83 | −43% |
| D6 | store-D6-1 | 35.5M | 12,115 | 20.7 → 13.5 KB | 1.71 → 1.11 | −35% |
| D7 | store-D7-1 | 34.9M | 11,432 | 24.4 → 16.7 KB | 2.14 → 1.46 | −32% |
| D4 | store-D4-1 | 16.1M | 25,298 | 26.6 → 12.3 KB | 1.05 → 0.49 | −54% |
| D9 | store-D9-1 | 17.7M | 13,215 | 13.1 → 4.9 KB | 0.99 → 0.37 | −63% |
| D10 | store-D10-1 | 3.0M | 30,393 | 27.8 → 10.8 KB | 0.92 → 0.36 | −61% |
| D11 | store-D11-1 | 2.8M | 23,620 | 20.7 → 7.6 KB | 0.88 → 0.32 | −63% |
| D12 | store-D12-1 | 4.1M | 22,256 | 14.9 → 3.2 KB | 0.67 → 0.15 | −78% |
| D13 | store-D13-1 | 3.9M | 11,955 | 15.3 → 8.3 KB | 1.28 → 0.70 | −46% |
| D14 | store-D14-1 | 3.2M | 5,379 | 6.1 → 2.6 KB | 1.13 → 0.49 | −57% |

#### Where the saving comes from

Estimate from the layout above:

- D1: of the 8.4 KB saved per block, about **6.5 KB (~77%) is removed
  overhead** (keys, headers, bitmaps) and about **1.9 KB (~23%) is better
  encoding** (11.8 → 9.9 KB of point data).
- D2: overhead is ~88% of the saving (7.2 → 0.28 KB overhead, 6.7 → 5.8 KB data);
  its blocks carry less point data, so the fixed per-group cost was a bigger share.
- D7 and D6 gain least: their values are float-heavy, and the encoding
  helps floats less than integers.

### Disk: Pebble bytes before and after

`pebble_disk_bytes` at 3 Nov 09:00 vs 4 Nov ~10:15.

#### Worked example: store-D1-1

| | Before (3 Nov 09:00) | After (4 Nov 10:15) |
|---|---|---|
| Pebble disk | 1,860 GB | 1,271 GB (**−589 GB, −31.7%**) |
| L6 | 1,627 GB | 1,167 GB |
| L5 (overlap) | 187 GB | 65 GB |
| L4 | 24 GB | 17 GB |

In 25 h cold-pack on this host rewrote 86.9M blocks (1.16 trillion points); their
logical size went from 1.62 TB to 0.89 TB (−0.73 TB). On disk that showed up as
−0.59 TB, after a day of new data had also been added.

#### Per cluster

| Cluster | Cold-pack host(s) | Change since 3 Nov 09:00 | vs peers without cold-pack, now |
|---|---|---|---|
| D1, 16 hosts | 16 production stores | −30 to −32% (1.74–2.08 → 1.20–1.42 TB each) | no unpacked D1 left |
| D1 | store-D1-2 | −26% (1.69 → 1.26 TB; was already v7) | |
| D1 | store-D1-3 | −22% (1.72 → 1.35 TB) | |
| D1 | store-D1-4 | +7% (1.16 → 1.23 TB; packed since 2 Nov, this is growth) | |
| D8 | store-D8-1 | −39% (1.19 → 0.73 TB) | −43% |
| D9 | store-D9-1 | −46% (0.25 → 0.13 TB) | −47% |
| D5 | store-D5-1 | −36% (0.98 → 0.62 TB) | −36% |
| D2 | store-D2-1 | −28% (1.11 → 0.80 TB) | −35% |
| D4 | store-D4-1 | −15% (0.53 → 0.45 TB) | −23% (peers grew 10%) |
| D10 | store-D10-1 | −30% (0.12 → 0.08 TB) | −26% |
| D3 | store-D3-1 | −22% (1.63 → 1.27 TB) | −23% |
| D6 | store-D6-1 | −18% (1.13 → 0.93 TB) | −20% |
| D11 | store-D11-1 | −47% (0.08 → 0.04 TB) | −33% |
| D12 | store-D12-1 | −42% (0.08 → 0.05 TB) | −34% |
| D13 | store-D13-1 | −22% (0.11 → 0.08 TB) | −26% |
| D14 | store-D14-1 | −62% (0.04 → 0.02 TB) | −43% |
| D7 | store-D7-1 | +10% (1.54 → 1.68 TB) | +10% (first pass still running over relayout copies) |

**Net on the 31 cold-pack hosts: −11.2 TB in about 25 h**, roughly 10 TB of it
on D1.

#### Why disk drops ~31% when the block bytes drop 45%

1. **Pebble already compresses.** Shared key prefixes and Snappy SST blocks mean
   the per-group key overhead that cold-pack removes never cost its full logical
   size on disk.
2. **Some data is not packable:** the recent hot tail until it ages out,
   per-metric metadata records and the name index, blocks with more than 65,536
   points, and old-layout (v6, not time-bucketed) blocks, which need relayout
   first.
3. **New data keeps arriving**, about a day's worth inside this window.

Compaction also contributes on its own: the L5 overlap shrinking on store-D1-1 is
part of the drop. On an uncompacted host compaction alone is about −10%
(store-D1-5 reference, 2 Nov: L5 178 GB + L4 22 GB of overlap).

#### Like-for-like check (2 Nov 2026)

Both stores fully compacted with `store-compact-once`, same logical load (active
series 8.63M vs 8.70M, 2.84M points/s each):

| | store-D1-4 (v9, exact cold-pack) | store-D1-2 (v7, no cold-pack) | Delta |
|---|---|---|---|
| `pebble_disk_bytes` | 1,167.2 GB | 1,559.3 GB | −25.1% |
| L6 | 1,145.5 GB | 1,546.3 GB | −25.9% |
| Bytes per active series | 135.3 KB | 179.3 KB | −24.5% |
| df used | 1,328.1 GB | 1,733.1 GB | −23.4% |

After a full relayout (so older v6 history becomes packable) and a second
compaction, store-D1-4 reached 1,136.9 GB; with zstd on cold levels
(`store-compression-cold = "zstd"`) 1,089.4 GB (−30% vs store-D1-2). zstd is
parked: it cost 2–2.7 cores (≈ +15–18% process CPU) for −4.2% disk, and CPU binds
in sizing.

### Cost

#### While passes run

D1 medians at the same time of day before the rollout and on 4 Nov morning
before the release:

| | region 1 | region 2 | region 3 |
|---|---|---|---|
| CPU (cores per store) | 14.8 → 17.7 | 12.4 → 18.4 | 12.8 → 19.4 |
| Read p99 | +0.1 to +0.7 ms in the morning (region 2 2.3 → 3.0 ms); up to +1.4 ms at night in region 2 | | |
| Updates/s | unchanged | | |

Over the last 25 h cold-pack on D1 read **101.8 TB to write 21.3 TB**
(store-D1-1 alone read 6.2 TB to repack 1.6 TB). The excess is repeat passes
re-reading already packed history every interval (1h default). The worst cases
were the non-D1 rollout hosts during repeat passes: store-D2-1 read 23.5–29.9 MB/s
to pack 4.8–5.5 blocks/s (4.3–6.2 MB read per packed block), with read p99 28–41
ms vs ~15 ms on its peers; store-D6-1 reached 5–7× its peers' p99.

Mitigation used on 4 Nov 2026: increase the pass interval to `24h` on 29
stores after the first pass (08:05–09:15 CET). The interval gates the next pass,
so it does not stop a running pass. A historical per-metric-summary prototype
showed no repeat-pass read saving on a frequently updated dataset: pass 2 read
70.7 GB over the first 4M metrics, versus 66.5/69.4 GB previously, with zero
summary skips. Hourly writes changed every metric revision. The 256 KB per packed
block observed on pass 1 measured backlog work, not repeat-pass efficiency.

A leftover pass at 64 workers and 1 GiB/s raised read p99 from 20 to 35 ms at
about 580 req/s (15–18 ms on previous days). Packing accounted for 52% of one
store's process CPU, mostly history reads. Setting one worker and 1 MiB/s by
SIGHUP reduced that store's p99 from 41 to 13.5 ms within 8 minutes; that setting
was applied to the dataset at 17:37 CET. These are historical observations;
current cursor/watermark behavior is described above.

#### Steady state

Reading packed blocks once passes are done costs **0.16% of process CPU**
(store-D1-1, 30 s CPU profile 4 Nov 09:55: sealed decode 0.70 of 426.8 CPU-s; no
Repack samples).

### Re-measuring

Reproduce the metric-derived numbers with your Prometheus-compatible datasource
and the appropriate job and instance selectors:

```promql
# per-block before/after, per host (sum by cluster; divide by blocks or points)
increase(store_cold_pack_blocks_total[24h])
increase(store_cold_pack_points_total[24h])
increase(store_cold_pack_input_bytes_total[24h])    # logical bytes before
increase(store_cold_pack_output_bytes_total[24h])   # logical bytes after

# read cost of a pass (read per packed block = read rate / blocks rate)
rate(store_cold_pack_read_bytes_total[5m])
rate(store_cold_pack_blocks_total[5m])

# first packing (all sealing minus repacking)
sum by (cluster) (rate(store_cold_pack_blocks_total[5m]))
  - sum by (cluster) (rate(store_cold_pack_repacked_blocks_total[5m]))
sum by (cluster) (rate(store_cold_pack_points_total[5m]))
  - sum by (cluster) (rate(store_cold_pack_repacked_points_total[5m]))

# thawing: blocks/s by writer entrypoint, points/s and logical read+rewrite bytes/s
sum by (cluster, reason) (rate(store_cold_pack_thaws_total[5m]))
sum by (cluster) (rate(store_cold_pack_thawed_points_total[5m]))
sum by (cluster) (rate(store_cold_pack_thawed_bytes_total[5m]))

# repacking previously thawed ranges: blocks/s, points/s and logical bytes/s
sum by (cluster) (rate(store_cold_pack_repacked_blocks_total[5m]))
sum by (cluster) (rate(store_cold_pack_repacked_points_total[5m]))
sum by (cluster) (rate(store_cold_pack_repacked_input_bytes_total[5m]))
sum by (cluster) (rate(store_cold_pack_repacked_output_bytes_total[5m]))

# disk, now vs a reference time
pebble_disk_bytes
pebble_level_bytes
```

Lifecycle series require the thaw/repack counter release on every compared host.
Use the usual `job="go_carbon"` and host/cluster selectors when combining jobs.
Thaw reasons are `append`, `patch`, `rewrite` and administrative `unpack`, named
after the real codec entrypoints. Slot reuse directly replaces the old range and
does not thaw it. Thaw rates include completed work whose surrounding transaction
is later discarded; packing/repacking rates count committed batches. Pack totals
exclude administrative unpacking in this release. Repack history persists across
restarts, but thaws executed by older binaries are unmarked. See
[lifecycle accounting](#observability-and-qualification).
