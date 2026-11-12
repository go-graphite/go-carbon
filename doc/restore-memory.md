# Memory during pending restore

Opening a pending checkpoint overlaps opening the chunk store. The recovery
bundle maps its files read-only and decodes its name index on the Go heap. The
cache's restore admission bounds claimed points. That admission does not cover
Pebble's native allocations in a cgo build.

Once carbonserver opens, concurrent render requests can multiply cold SST index
buffers. The per-render worker limit applies separately to every request. Pebble
allocates compressed and decoded blocks with `calloc`; iterators retain decoded
index blocks until they close. A block larger than a cache shard is not cached,
even if the total cache budget is large. Many readers can therefore hold private
copies of the same index, without increasing the block-cache size metric or the
Go heap profile.

The chunk store admits at most 32 foreground Fetch/Metadata calls together.
Metadata prewarm keeps its existing bounded worker pool. Admission precedes
opening the foreground read view;
release follows iterator/value-handle and view closure. Waiting callers can
cancel without allocating Pebble buffers. Pebble's existing `LoadBlockSema`
additionally limits concurrent block loads to 16 across foreground and background
work. The view limit matters because limiting block loads alone does not bound
blocks already pinned by iterators waiting for their next block.

This bounds the foreground multiplier, not an individual SST block's size:
resident native memory still includes the configured cache and memtables,
bounded background workers, and the blocks held by 32 active foreground
operations. A lookup may visit multiple tables/levels. The change uses Pebble's
public options and adds no vendored patch or on-disk format change. Heavy cold
fan-out queues at the store, so latency under overload can increase.

`TestColdReadsBoundNativeIndexMemory` exercises 256 real cold lookups against a
valid Snappy SST with an oversized top-level index. It asserts admission, native
malloc growth (Linux cgo with mallinfo2; RSS/admission remain checked on musl), RSS, cancellation, and lookup results.
`TestPendingRestoreMemoryPerPoint` publishes and restores a GCPOINT3 bundle and
asserts at most 128 heap bytes / point and 192 RSS bytes / point, plus a 64 MiB
fixed allowance, for its 11-point long-name fixture. Run the larger fixture with:

```sh
GOTOOLCHAIN=go1.27.0 GO_CARBON_RESTORE_MEMORY_NAMES=2000000 \
  go test ./cache -run '^TestPendingRestoreMemoryPerPoint$' -count=1 -v
```

These fixture bounds are regression thresholds, not universal production sizing
ratios: retention, metric-name length, series count and concurrent reads matter.
Use `/proc/PID/smaps_rollup`, Go heap profiles and libc allocation statistics
together when diagnosing a discrepancy. Reducing `dump.restore-per-second`
cannot throttle native allocations made by simultaneous queries.


## Dense checkpoints

A checkpoint uses a read-only file mapping for pending reads. Opening it validates
all bytes, making the source resident. Keeping those pages beside the growing
live cache can nearly double restore RSS. On Linux, bundle open now discards
resident mapping pages after validation; GCPOINT3 file-order restore repeats the
hint after each block and on return, including cancellation. The mappings remain
valid and concurrent readers can fault the same immutable bytes back in. Files
are still retained until persistence confirms every point. The kernel may keep
reclaimable file-cache pages; MADV_DONTNEED drops the process's resident mappings,
not necessarily that file cache. Other operating systems keep their existing
residency behavior. Older checkpoint formats keep their existing traversal.

Restore also encodes each metric using reusable scratch under its cache shard
lock, then clones only the completed byte buffer into the item. The unfinished
word and encoder state are copied with it. Scratch is bounded to 64KiB per shard
(64MiB across 1024 shards), included in cache memory accounting, and dropped for
larger metrics. This avoids repeated buffer-growth garbage for every metric.
It uses the existing memory codec and changes no disk format or point ordering.

`TestPendingRestorePeakRSS` runs live ingestion and checkpoint open/attach/claim
in fresh Linux processes with GOGC=200. It checks kernel VmHWM without forcing GC,
at both 700 and 850 points/name with high-entropy finite values and long names.
The default fixtures contain 28M and 34M points. Restore must stay below both
1.25 times the matching live peak and 16B/point plus 64MiB. A 60k-name run contains
42M and 51M points and measured 11.26 and 11.95B/point after the fix. The old path
fails the same assertions. This is a dense-fixture regression ceiling, not a
universal ratio for sparse names, arbitrary timestamps or concurrent queries.
Race builds exclude physical-RSS assertions because instrumentation changes the
memory model; separate race-tested oracles cover scratch ownership, every float
bit, concurrent mapped reads, interrupted restore, and retired mappings.

```sh
GOTOOLCHAIN=go1.27.0 GO_CARBON_RESTORE_PEAK_NAMES=60000 \
  go test ./cache -run '^TestPendingRestorePeakRSS$' -count=1 -v
```

For sizing, reserve other process memory and fixed overhead first. At the fixture
ceiling, 25B points require 400GB plus 64MiB for this path. Datastore cache,
memtables, SST indexes held by admitted readers and other application components
are additional. Validate the host's actual name density and point distribution;
the sparse 11-point/name fixture above deliberately has a much higher unit cost.
