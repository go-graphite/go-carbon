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
malloc growth (Linux cgo), RSS, cancellation, and lookup results.
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
