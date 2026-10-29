# Shared storage: CPU and memory qualification

Parity with CWhisper is **not established**. Shared storage keeps the source
CWhisper writer state, including out-of-order precedence; smaller disk use does
not imply lower CPU or memory use.

The latest completed serving window was 300 seconds, with conversion admission
paused and normal compaction active: 1,494 compactions, 69 flushes, zero new write
stalls. Both candidate and unchanged replica used Go 1.27.0. Committed rates below
come from raw one-second observations, not a dashboard maximum/consolidation.

| Measurement | Shared exact | CWhisper control |
| --- | ---: | ---: |
| Committed points/s | 1,287,945 | 1,238,681 |
| CPU cores | 22.55 | 13.46 |
| CPU microseconds/committed point | 17.51 | 10.87 |
| Allocated bytes/committed point | 1,336 | 426 |
| Mean RSS, decimal GB | 15.65 | 12.79 |
| Mean cached points, millions | 68.07 | 66.63 |
| HTTP request p99, ms | 4.54 | 2.96 |
| HTTP request p99.9, ms | 30.75 | 25.55 |

This is a matched-time pilot/control observation, with different converted
populations and cache lifecycles. It is not a randomized attribution or proof of
fleet behavior. The remaining gap is about **61% CPU per committed point** and
**22% mean RSS** in this window. A previous lock-scope change produced no measured
CPU gain after replica normalization (+0.65%); RSS changed −2.07%, request p99
+25.49%. Different before/after windows prevent a causal latency claim.

## Status on the integration line

Measured on 26 October 2026 against the reviewed integration revision,
using the same fixed-work harness:

- Ported on 24 and 25 October: decoded append-tail reuse (`9bfe82a3`), the
  per-update absent-sidecar cache (`4800067d`), namespace reuse on reopen
  (`0629f97c`), owned manifest header buffers (`f8c897bd`) and the panic-safe
  confirmation guard (`4101161b`). Each has its parity tests; only the
  evidence logs below are new here.
- Not ported: `store-read-cursor` and its per-update `SeekGE` cursor. The
  shared update iterator (`43b0f706`, `b3174ecc`) replaced it. Rarely
  rewritten keys are probed with `SeekPrefixGE`, which consults bloom
  filters, and keys most updates rewrite keep fresh lookups, which stop at
  the newest version. The same-process result below (point lookups beat the
  cursor) is consistent with that split. Routing every `Get` through the
  shared iterator with `SeekGE`, as the cursor did, opened 7–23% fewer
  tables per update but made 4–60% more block-cache lookups. Per update it
  was slower in seven of the eight `BenchmarkContinuationUpdateLevels` cells,
  by a median of up to 12% (v6/v7, 30/300 points, 256/32 MiB cache; six
  paired processes per cell, 26 October). What remains of the runtime
  control is that `store-update-point-lookups` switches on SIGHUP
  (`a8c954c5`).
- Not ported: Go block-buffer recycling and the `gtgoalloc` build tag. The
  production build uses cgo, where Pebble allocates block-cache values with C
  `calloc` and frees them with `free`. The recycler only replaced the non-cgo
  path's `make`, so it could never run there. Forcing Go allocation with
  `gtgoalloc` would move the block cache onto the Go heap, out of reach of
  the C-heap metrics and periodic `malloc_trim`. An earlier alternative branch had
  already dropped the recycler after it missed the serving bounds.

## Reader and header changes

- Partial continuation appends reuse the already decoded predecessor group.
  The reference/oracle compares encoded KV bytes, timestamp/value bits, group
  boundaries, failures and the fleet's accepted unaligned timestamps. Component
  allocation count falls from20 to18; measured bytes about9.2–9.8KB to5.5–6.0KB.
  This is not a host improvement claim.
- `whisper.store-read-cursor=false` preserves the fleet's point-get behavior.
  Set true and SIGHUP to reuse one iterator only across consecutive reads within
  each update. Mutations and commit close it; each in-flight operation retains
  its selected mode. `storage.engine.ContinuationReadCursorEnabled` reports the
  live choice. The source codec, ownership fences and WAL policy are unchanged. The isolated
  header-point benchmark goes3,349→2,084B and34→20allocations per update; this
  component result did not predict the production result below.
- A same-process point/cursor/point experiment, three300s windows, gave point
  lookups −3.15% replica-normalized CPU/committed point, −1.02% mean RSS and
  −27.10% end-to-end request p99 relative to cursor mode. The CPU difference is
  below the5% improvement floor. The cursor is retained for controlled workload
  comparisons, **not enabled by default** on an unqualified fleet.
- Missing virtual-file names are remembered only for one snapshot/indexed batch.
  Create/remove/rename update that cache. Storage errors are not cached as
  absence, and nothing is retained across updates. Four repeated missing-sidecar
  probes become one LSM lookup; the isolated map benchmark goes472→424B and
  14→8allocations. This avoids repeated header/existence work without a shared
  cross-operation header cache or weaker external-writer locking.

The update path now reads its bounded filename namespace once and reuses it
for main/OOO opens and final cleanup. This moves the namespace walk already
needed at flush; it adds no second walk. Creates, renames and removals overlay
the initial view. Unopened aliases and open-but-unlinked handles retain their
data until safe cleanup. At more than64 names or255 bytes/name it uses the
ordinary lookup path. Read errors do not publish a partial cache, and an
explicit final context check preserves cancellation even without a flush read.

In a warm1,024-metric indexed-backend reopen benchmark (16MiB cache, three
production-shaped retentions), point-name lookup versus namespace reuse gave:

| Reopen component | Point names | Namespace reuse |
| --- | ---: | ---: |
| KV gets/op | 3 | 1 |
| Namespace walks/op | 1 | 1 |
| Allocations/op | 68 | 58 |
| Allocated bytes/op, median | 17,006 | 16,641 |
| Time/op, median of5runs | 8,020ns | 6,739ns |

With4,096metrics and a1MiB cache, the same component measured35,121→24,082ns
at the median (−31.4% elapsed time); engine cache misses fell18→12per reopen.
Both cases retain the same point values
and perform no timed WAL writes; they isolate reopen/cleanup rather than full
write throughput or durability. Compaction prepares the database before timing.

This is an isolated synthetic benchmark, not a production replay or host CPU/RSS
result. The saved pilot profile attributes about972CPU-seconds/300s to the
namespace-get call site inside virtual reopen; eliminating that cost is a
target, not an achieved saving. Compressed-header decoding itself accounted
for only86CPU-seconds overall. No header is retained across operations and no
storage encoding, WAL policy, cursor default or external-writer fence changes.

Namespace qualification: full whisperio race suite106.8s; focused namespace,
alias/open-handle cleanup, bounds/error and late-cancellation races pass3runs;
final persister/carbonserver integration races32.7s/2.5s; forced-ingest
incremental/shared-only persister races174.0s. The broad normal persister run
passed1,240.5s; final targeted races cover the later cancellation/bound refinements.
Vet passes. Small source-hashed logs are in
[the namespace evidence](storage-review/namespace-prefetch/manifest.json).

Compressed reopen also reuses the manifest bytes already owned by this operation
instead of allocating a second header copy. Reuse requires at most256bytes of
extra buffer capacity; classic OOO headers, large layouts and oversized buffers
retain the copy path. There is no cross-operation header cache. Isolation tests check
that writes remain invisible to another operation until flush, that previously
opened headers remain unchanged, and that overlay writes cannot mutate their
underlying snapshot. Round trips preserve the encoded manifest bytes.

Five-run medians in the same isolated benchmark, with namespace reuse enabled:

| Header ownership component | Before | Owned-buffer reuse |
| --- | ---: | ---: |
| Warm allocations/op | 58 | 57 |
| Warm allocated bytes/op | 16,645 | 12,528 |
| Warm time/op | 6,633ns | 5,835ns |
| Pressure allocated bytes/op | 16,644 | 12,536 |
| Pressure time/op | 25,793ns | 23,180ns |

This saves about4KiB of allocation per reopen (−24.7% in this component), with
unchanged KV lookup counts. It is not a measured RSS reduction or CPU cost per
committed point. The same production parity gap above remains open.
In the saved pilot profile, the entire manifest decoder accounted for3,377MiB
of489,999MiB allocated over300s (0.69%) and8.99CPU-seconds of6,753.68 (0.13%).
Those totals include work beyond this copy; this change alone cannot close the
observed parity gap. A fully converted population still needs its own replay.
Full whisperio race tests pass64.9s, forced-ingest incremental/shared-only
persister races97.6s, ownership/bounds cases3runs1.3s, and vet passes.
[Header evidence](storage-review/header-ownership/manifest.json) includes source
hashes, both benchmark logs and the saved-profile cost ceiling.

Header scratch pooling is already present in the fork. Latest allocation deltas
attribute about14GB/300s (3%) to decoded compressed headers, versus200GB (42%) to
Pebble block-cache values and43GB (9%) to range-deletion iterators. Cumulative
frames overlap; do not add them into a budget. Original-file opening is also a
CWhisper cost. Bypassing path locks or retaining decoded headers across external
writers is not justified by these profiles.

At the final sample, heap-in-use was12.43vs9.11GB; unreleased idle heap was
2.60vs2.58GB, stacks0.025vs0.024GB. This points to extra active/fragmented heap
rather than a large extra scavenger backlog. The latest completed-GC heap
profiles retain about1.89GB in shared block-cache values,0.26GB in Pebble manual
allocations and further encoder buffers. These sampled live profiles and current
HeapAlloc measure different GC moments; do not add them or treat cumulative
allocation bytes as resident memory. Reducing decoded-header allocation alone
cannot explain or close the whole RSS/CPU gap.

A cgo allocator trial reduced fixture RSS569→268MB but increased whole replay
CPU674→827s; it was rejected. Earlier bounded Go block-buffer recycling also
failed serving CPU/RSS bounds. Neither is included here.

## Validation and next measurement

Full chunkstore and whisperio race suites passed on this branch (233s/97s),
including the forced Go-allocation path; focused runtime/HUP races passed three
runs, the full carbon race suite passed115s, the full persister suite passed
1,002s, and vet passed. These are local source qualification, not a new host canary.

Use exact CWhisper output and physical writer-state oracles, normal/OOO writes,
failed writes, compaction and disk-only reopen/crash checks. Run backend races
with `-tags gtgoalloc` as well: ordinary cgo-enabled race tests miss manual buffer
ownership. Runtime mode changes must not close an active operation's iterator.

For a future fleet-owned canary, match committed points, cache age/backlog,
read/write mix, warmup, compaction/flush activity and duration against an unchanged
replica. Keep raw sampling resolution and report CPU/committed point, live heap,
RSS, allocation/point, request p99/p99.9, failures, backlog, write visibility,
stalls and physical-device IO. A queue-rebuild timer is not persistence latency.
These changes have no new production qualification yet; this work does not
authorize another independent host build or rollout.

See [independent review findings](shared-storage-review.md) for open correctness,
durability and lifecycle issues, with archived reproducers and captured results.
