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

## Configuration and reload

In `[whisper]`:

```toml
store-cold-pack = "off" # off, exact, decimal, unpack
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
fraction must be between 0.001 and 1. Byte rate must be positive. The write window
must be nonnegative and the sweep interval at least one second. Zero durations
select defaults. All seven settings reload on SIGHUP; invalid settings fail the
reload. Turning off joins the current bounded operation and prevents subsequent
packing. It retains the upgraded reader fence and existing sealed data.

Only time-bucketed (v7 layout) blocks larger than one 128-point group are
eligible. Blocks a store wrote before time buckets were enabled stay in the v6
layout and are skipped until `store-time-buckets-relayout-rate` moves them into
the history bucket.

`unpack` restores ordinary 128-point groups. This is also paced, and leaves the
reader fence in place during online use. Changing exact/decimal/unpack policy
restarts the cursor. Decimal mode does not rewrite blocks already sealed in
exact mode; explicitly unpack first if changing existing cold blocks is desired.

## Resource bounds and publication

The worker seeks through catalog pages of at most 64 names. It keeps only one
page, seeks through at most 32 stored physical descriptors per step, and prepares at most one
native range per transaction. It never collects or sorts the whole catalog and never probes unallocated descriptor slots. It reads manifests for layout without loading authoritative header pages.
Native ranges above 65536 points are skipped and counted. A decoded native range holds at most 1 MiB of points. Snapshot reads are
limited to 2 MiB and mutations to 256 KiB plus the revision record. Oversized
headers/ranges are skipped rather than splitting a native writer boundary.

Snapshot reads, decompression and encoding happen outside the metric stripe.
Publication uses TryLock, rechecks ID, generation and revision, and abandons a
stale plan. A busy stripe is retried in a later sweep. Metadata validation that
already consumed 2 ms abandons publication. The only remaining locked work is
one bounded WAL batch and cache invalidation. Source groups are removed with bounded point deletes; maintenance adds no range tombstones to the shared memtable fragmentation cache. Reads use consistent snapshots and
never wait for this stripe. A late correction or append atomically thaws the
native range before following the existing exact writer and rollup path.

The byte pacer charges snapshot payload reads and committed WAL batch bytes.
The CPU pacer charges worker elapsed time, including IO waits, against a fraction
of one worker. It sleeps outside locks and snapshots; idle time does not earn a
later burst. One bounded step is the allowed burst. Reload and shutdown wake a
sleeping worker. This is a conservative worker budget, not a process-wide CPU
ceiling: asynchronous Pebble flushes, compactions and GC are additional work.
Existing compaction pacing remains independently configurable.

Maintenance pauses when Pebble's estimated compaction debt exceeds
`store-cold-pack-max-compaction-debt` (64 MiB by default), two memtables are full,
L0 reaches half the write-stop threshold (at least two sublevels), or the free
disk reserve plus 8 MiB cannot be maintained. These are admission checks, not
reservations against concurrent writers. A busy store that normally carries
several GiB of debt pauses on every step at the default; raise the threshold to
just above its usual debt, and watch `store_cold_pack_pressure_pauses_total`.
WAL, flush and compaction output require
transient disk space before old tables are reclaimed. Bounded publication cannot
place a hard deadline on filesystem syncs or a Pebble write stall that starts
between admission and commit; monitor the maximum lock metric and read latency.

The cursor is process-local. Restart begins an idempotent sweep again; completed
ranges are already sealed and are skipped. New or recreated metrics behind the
cursor are visited on a later sweep. The interval is a pause after the complete
paged sweep, not after every page.

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
go build -o cold-pack ./tool/cold-pack
./cold-pack -store /var/lib/carbon/store -source /var/lib/carbon/whisper \
  -mode unpack -downgrade
```

Omit `-source` for stores without an incremental source binding. Keep the server
stopped until the command exits successfully. The tool shares the background
worker's pacing, disk reserve and transaction limits; flags allow changing the
budgets. After a complete clean unpack sweep it flushes the WAL, then atomically
syncs the v7 marker. Any skip, conflict, failure or interruption prevents that
marker change. Retry after correcting the cause. A crash before the marker switch
leaves a readable v9 store; a crash after it leaves only v7-readable blocks.
Never edit the marker manually. An interrupted unpack can be resumed safely.

## Observability and qualification

`store_cold_pack_*` Prometheus metrics expose blocks, points, logical input/output
bytes, snapshot reads, WAL batch bytes, pages, completed sweeps, skips, conflicts,
pressure pauses, worker seconds, maximum stripe lock duration, active work and a
latched failure flag. Logical input minus output is the encoded byte saving,
not immediate filesystem reclamation. `pending_metrics` counts the discovered
page queue, not an estimated full-store backlog. The administrative status API
also includes the mode, name/descriptor cursor, counters and most recent error.
Errors are logged with `ERROR`; successful sweeps log cumulative progress.

Fast oracle tests cover exact/decimal values, corruption and allocation bounds,
physical mutation paths, atomic failed plans, crash/restart, concurrent ingestion,
corrections/rollups, revision conflicts, paging, pacing, reload and transfer fences.
Use `GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor GOMAXPROCS=4` and a per-package
`-timeout=150s`; race checks can use two processors. The optional
`TestColdPackCorpus` in `tool/cold-pack/bench_test.go` operates on private clones
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

Before a canary, validate the actual retained cold fraction and free-space
headroom. Start exact mode on one representative host with ample disk and low
budgets; compare matched read p50/p99, accepted/persisted points, correction errors,
WAL/flush/compaction rates, CPU, disk and maximum lock duration. Exercise restart,
transfer and unpack using backups before upgrading additional hosts. Turn off on
read regressions, new errors or reserve pressure; stopping maintenance does not
require downgrading the binary. Qualify decimal mode separately against the
application's numeric requirements and longer-history/churn workloads.
