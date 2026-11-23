# Sparse archive repair storage contract

`Store.RepairSparse` fills verifier-selected source timestamps into an existing
classic continuation image. Each selected archive identifies its ordinal and
exact step/count geometry. Point values travel as `uint64` float bits. There is
no propagation between archives.

The caller pins the expected target `TransferVersion` and supplies all selected
missing source points, ordered and unique within each archive. A finite target
value wins a timestamp tie, including signed zero and infinities. Distinct
epochs sharing one circular slot use the same `PreferArchiveTimestamp` rule as
ordinary import: readable current-retention points take precedence over future
aliases, otherwise the newest timestamp wins. Expired raw points are not simply
filtered out; ordinary import retains non-aliased raw history too.

Preparation uses the existing classic native-window index, `classicGroup`, and
`setPointBlock`. It reads at most one copy of each requested 128-slot group,
plus file headers, namespace and index metadata. Only changed groups and their
metadata are published. The target metric ID and generation remain unchanged.
The batch commits atomically after a writer-stripe check of both the original
mutation epoch and full target version/policy. Conflicts return `ErrConflict`;
callers must reverify rather than reusing an old selection. WAL sync follows
stripe release. A sync error can mean publication happened; reverify on retry.

## Compatibility and normalization

Ordinary import normalizes the complete source and target images, dropping all
NaNs, even those outside the supplied repair ranges. Sparse repair therefore
requires a revision-bound canonical-provenance certificate alongside the
existing native window index. It is optional and leaves the window-index
encoding unchanged. The certificate is established when a fresh classic image
is constructed, and maintained by known indexed writers. It certifies aligned,
unaliased slots without NaNs or hidden occupied cells on unallocated pages.
Known writers only allocate classic pages; they do not hide previously visible
cells. Encountering a NaN conservatively clears the certificate until a canonical
rebuild. Old writers leave the certificate stale when they advance the revision.

`*SparseRepairUnsupported` asks the caller to use ordinary `ImportTransfer` for
missing target names, unsupported codecs/storage, mismatched archive geometry,
compressed images, sidecars, referenced aliases, absent/stale indexes or
certificates, and noncanonical target images. Source NaNs are ignored just as
in the ordinary merged import. Invalid point ordering/grid is `ErrFormat`.
Unsupported and conflict results publish no partial archive work.

## Differential guarantee and first usage

The randomized storage integration test uses verifier-style missing timestamps,
then calls `RepairSparse` with fallback to `ImportTransfer`. It compares every
canonical circular slot in every archive against an independent ordinary import:
presence, timestamp and `math.Float64bits` must match exactly. Cases include
NaN payloads, signed zero, infinities, overlapping values, expired/future ring
aliases, sparse wrap, multiple archives and absent target names. A separate
concurrent-write test rejects an entire prepared multi-archive patch. The seek
test verifies that two adjacent missing points decode exactly one point group.
These tests and the benchmark are this slice's first consumers; no HTTP or
controller path selects the primitive yet.

Full-import equivalence requires **all raw source-only timestamps across all
archives**, not just serving-window holes. Final full-history verification must
remain in place even when a controller repairs a bounded subset first.

## Benchmark

Run with `GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor nice -n 19 go test
./internal/chunkstore -run '^$' -bench '^BenchmarkSparseRepair$'
-benchtime=20x -count=3 -benchmem` (on one line).

Whisper forbids duplicate precisions within one name, so the benchmark measures
60s/30d and 60s/14d populations separately, each with 1800s/2y and 3600s/10y.
The target contains every point except a three-day fine-archive hole and matching
coarse slots. Every timed iteration starts with that same unrepaired image.
Fixture reset and complete output validation are outside the timer. The receiver
uses an in-memory filesystem; these are per-name CPU/allocation measurements,
not a live fill speed or durable device-throughput claim.

`codec-read-B/op` counts encoded classic point payload bytes read by codec calls,
including repeated reads on both donor and target. It excludes headers, the
store's raw transfer staging walk, network and device/cache I/O. `B/op` is heap
allocation and is a separate measurement.

Median of three 40-repair samples on Apple M4 Max:

| Fine archive | Path | ns/name | Codec read B/name | Heap B/name | Allocs/name | Names/s | Ratio |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 60s/30d | ordinary | 37,522,215 | 2,259,010 | 59,092,454 | 27,472 | 26.65 | 1.00× |
| 60s/30d | sparse | 854,100 | 1,563 | 874,903 | 619 | 1,170.82 | 43.93× |
| 60s/14d | ordinary | 31,910,380 | 1,940,859 | 46,012,982 | 23,628 | 31.34 | 1.00× |
| 60s/14d | sparse | 764,855 | 1,574 | 909,271 | 597 | 1,307.44 | 41.72× |

## Next integration slice

1. Preserve archive identity and exact missing runs/bitmap in the verifier
   journal, bound to source and target versions. Keep full-history verification.
2. Export only those donor archive ranges, retaining exact float bits and
   verifying geometry/version. Bound range/frame sizes and memory.
3. Add a dedicated transfer endpoint and a storectl repair transport. Bind
   durable receipts to donor ordering, selected ranges and a new transport
   identity on resume; leave completed ordinary receipts valid.
4. Reverify changed targets, durably retry unfinished work, and fall back to
   ordinary import for every typed unsupported result. Never mark a name done
   while any selected archive is unresolved. Benchmark fallback share and the
   full pipeline before rollout.
