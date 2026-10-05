# Storage correctness fix plan

The plan below records the failing baseline and investigation. Implementation
now preserves classic behavior, fixes the shared compressed engine and OOO, and
updates Pebble to the already-corrected store revision. Delivery is grouped by
engine as requested: Pebble, shared cwhisper, then OOO. See
[the current suite documentation](storage-testing.md#correctness-fixes) for the
implemented behavior and validation results. The final macOS full race suite,
Linux persister race suite and all 72 benchmark correctness smoke cases pass.
The 1,256 parity/edge/random/slot-order cases pass, alongside lifecycle tests;
only the established plain-cwhisper late-write exception remains.

## Baseline

Run on 2026-10-03 at `60b1dca55c318c83abe865a4cd9e8671fc424807`,
Go 1.27.1, macOS/arm64, using the vendored engines:

```sh
go test -mod=vendor -race -run '^TestStorage' -count=1 -json ./persister
go test -mod=vendor -race -skip '^TestStorage' -count=1 -json ./persister
```

The storage suite failed in 45 seconds with no race reports. Counts below are
scenario results, not nested stage results or independent bugs. Each parity
scenario also checks writes, reopen, and compaction/reopen.

| Engine | Parity: pass / fail / skip | Fetch edges: pass / fail | Randomized: pass / fail |
| --- | --- | --- | --- |
| Classic Whisper | Reference oracle | Reference oracle | Reference oracle |
| cwhisper | 78 / 120 / 90 | 112 / 32 | Not run: late writes unsupported |
| cwhisper OOO | 114 / 174 / 0 | 112 / 32 | 0 / 18 |
| Pebble | 270 / 18 / 0 | 144 / 0 | 0 / 18 |

All recovery, concurrent metric isolation, persister round-trip, transfer, and
Pebble archive-persistence tests passed, as did the explicit cwhisper late-write
limitation test. The crash-writer helper skips outside its child process.
All baseline randomized cases stopped at their first batch; later batches were
not validated by that failing run.

Existing persister tests: 31 top-level tests passed; only
`TestStoreAlwaysCreatesSparseOutOfOrderSidecar` failed, reporting logical size
86,428 bytes versus 90,112 allocated bytes. This is an allocation assertion,
separate from the value/parity failures. No race reports appeared in this run.

## Ownership and contract

Classic Go Whisper at the pinned revision is the behavioral oracle, including
query grids, retention boundaries, duplicate precedence, future-point admission,
and circular-slot overwrite behavior. Do not change the oracle or weaken tests
to accommodate candidates. Plain cwhisper retains only its existing late-write
exception; OOO and Pebble must agree immediately after acknowledged writes, as
well as after maintenance and restart.

Implement library fixes in `go-whisper` and its separate `store` module. Then
update the corresponding go-carbon module pins and regenerate vendor. Avoid
maintaining a fix only in the vendored copy. Baseline pins differed:

- Root Whisper: `5f2e38dab385`.
- Nested store: `32ae757b63b8`.

## 1. Classic Whisper: preserve the reference

No classic-engine fix was indicated by this run. Its lifecycle tests passed; use
it as the control for every candidate change. Add minimal oracle regressions
alongside each library fix so failures are understandable without the full
matrix. Record classic's observed result even when it is surprising, especially
populated versus empty zero-length queries and future samples overwriting older
slots. Do not claim that comparison against an oracle independently proves the
oracle correct.

## 2. Shared compressed engine: fix cwhisper and OOO together

### C1 — Prevent live samples being overwritten during a large write (highest priority)

Confirmed with a separate reproducer using `ArchivePoints`, not just `Fetch`:
one 600-point batch into `1s:10m` retains only 492 physical samples in either
compressed mode. The first remaining timestamp is 108 seconds after the input
start. Classic retains all 600. Compressed integrity checks still pass, so valid
checksums are insufficient to detect this loss. The existing parity failure
persists after reopen and OOO compaction.

The same input in batches of 30 or 1 retains all 600; an initial compressed
point-size estimate of 14 also avoids this reproducer. These are diagnostic
controls, not the proposed production workaround.

Inspect `appendToBlockAndRotateWithBuffer`, `computeExtendedRetentions`, and
`UpdateManyForArchive`: block rotation can reuse a block before the end-of-call
extension check. The fix must ensure capacity before overwriting a block that
still contains logically live slots. Evaluate growth at rotation boundaries;
do not rely on a guessed batch size or larger average estimate as a guarantee.
Preserve path locking, sidecar state, and atomic file replacement during growth.

Acceptance: the same trace produces identical logical values across batch sizes,
compression ratios, partial/full block wraps, repeated ring wraps, reopen, and
OOO merge. Include incompressible values, existing files, and injected growth
failure; a failed extension must not acknowledge a successful lossy write.

### C2 — Make live rollups match classic, including XFF

Ordered writes already fail; this is not only an out-of-order problem.
Failures include missing newest coarse values and aggregates returned where
classic has a NaN because XFF was not met. The non-Mix live aggregation path in
`fetchCompressed` has no XFF check, can leave its final bucket un-emitted, and
truncates the remaining points at the first point outside the requested range.
Buffer rotation/order also needs to be covered when reducing the reproducers.

Compute each affected coarse bucket from the correct finer slots, in timestamp
order, applying classic's aggregation/XFF and propagation rules. Handle the last
bucket and interval filtering explicitly. Preserve explicitly written historical
coarse values and classic behavior when a later partial update fails XFF.

Acceptance: `ordered`, `shuffled-single-batch`, `duplicate-in-batch`, `sparse-xff`,
and `ring-wrap` pass all six aggregation methods and XFF 0/0.5/1 at every stage.
Then reclassify the OOO rollup/correction failures; many currently share this
earlier failure and do not establish separate OOO bugs.

### C3 — Match empty and populated query grids

For each compressed engine, 31 of the 32 fetch-edge failures are grid mismatches;
the remaining failure is a rollup value mismatch covered by C2.
`fetchFromArchive` expands a zero-length aligned interval only for a nonempty
classic archive. The compressed path does not implement that rule.

Apply the same rule based on whether the selected archive contains data,
including relevant buffers/sidecars. Preserve classic's empty-archive result.
Check clipping, archive selection, aligned/unaligned ranges, and sidecar-only
data. Acceptance: all 144 fetch-edge cases pass for each compressed engine.

### C4 — Match circular-slot behavior at retention/future boundaries

A minimal `1s:1m` reproduction writes `(now-59, 7)` and `(now+1, 9)`.
Classic returns NaN at `now-59` because the future point reused its circular
slot; both compressed modes return 7. This explains the first fine-archive
mismatch in `retention-boundaries`; it is distinct from Pebble's routing bug.

Define compressed logical slot validity to reproduce classic overwrite/order
semantics across encoded blocks, buffers, and sidecars. Simply rejecting future
points would change the oracle contract. Test same-batch and separate-batch
collisions at every retention, then rerun the complete boundary scenario to
expose any later mismatches.

## 3. cwhisper OOO: immediate correction visibility

Inherit C1–C4, then fix stale coarse reads before compaction. An isolated average
bucket initially reads -1 in both classic and OOO. Correcting two base points
makes classic return -9.8 immediately; OOO still returns -1 and reaches -9.8 only
after `MergeOutOfOrder`.

`mergeOutOfOrderValues` deliberately keeps existing coarse values and uses coarse
sidecar values only to fill holes. That existing policy does not satisfy the
requested classic contract. For affected coarse windows, derive the effective
result from main data, live buffers, and authoritative sidecar corrections;
reuse the compaction aggregation rules where appropriate. Do not overwrite a
complete aggregate with a partial sidecar aggregate. Preserve direct historical
coarse-write precedence and cascading rollups.

Acceptance: late holes, repeated corrections/retries, and historical coarse
corrections match classic before merge, after reopen, after merge, and after
subsequent appends, across all aggregation/XFF combinations. Merge must preserve
the already-correct visible result. Exercise both prefix-copy and full-rewrite
paths. Require all 18 seeded randomized traces to finish all 20 rounds.

## 4. Pebble: align retention routing first

The store's `extractPoints` returns `points[:i-1], points[i-1:]`; the root oracle
returns `points[:i], points[i:]`. This sends the last eligible point to an older
archive, or drops it. Fix the store split and add a minimal regression with one
eligible point followed by one expired point, plus boundaries for every archive.

A temporary compiler overlay changing only that return statement made all
**288 parity + 144 fetch-edge + 18 randomized cases pass under race detection**.
All randomized rounds consequently completed. No repository code was changed by
this experiment. It establishes a strong first fix, not blanket qualification of
all Pebble behavior.

Check whether the intended store revision already contains the correction, then
pin that tested revision together with the root module. Rerun the entire storage
suite, including WAL-only recovery, SST+WAL recovery, snapshot/import/export,
metadata and generation isolation. Retain synchronous WAL commits.

## Delivery and final gate

Group C1–C4 in the shared cwhisper commit, keep the Pebble routing/pin fix
independent, and follow with the OOO correction-visibility commit. Implement
shared compressed fixes before interpreting residual OOO failures.

For each change, first pass its minimal reproduction, then the affected engine's
complete matrix and persistence tests. After integration, require
`make test-storage` and the existing persister suite with race detection, plus
library tests and go-carbon integration checks for the changed paths. Repeat on
Linux with the target filesystem. Investigate the sparse-allocation assertion
separately using a sparse-file capability control and a fixture large enough to
distinguish allocation granularity; do not waive data comparisons to solve it.

The independent Go sparse-file control reproduces the allocation failure after
`Close` on this macOS host. The physical-allocation assertion now checks that
capability and skips when holes are materialized; no classic allocation code or
data comparison was changed. On Linux's container filesystem the control and
the original sidecar-allocation assertion both pass.

Finish only when OOO and Pebble have no parity exemptions or failures, cwhisper
passes every supported trace, and recovery/transfer tests remain green.
Run benchmark correctness gates before collecting performance comparisons.

Passing this suite covers its modeled workloads. It does not establish power-loss
durability, torn-write handling, or concurrent multiprocess replacement safety.
After semantic fixes, extend fault tests around OOO rewrite/rename/sidecar removal
and Pebble failed commit/import publication. File backends currently use unsynced
writes, while Pebble syncs its WAL; keep their durability claims distinct.

## Evidence

The local report directory is `/tmp/go-carbon-correctness-rrwp77cn/`:
`storage-tests.jsonl`, `run.json`, `persister-existing.jsonl`, `diagnose.go`,
`diagnose.txt`, and `pebble-split-experiment.jsonl`. It is temporary and is not a
committed fixture. Test databases were automatically removed.

- [Correctness suite and commands](storage-testing.md)
- [Compressed write, rotation, extension and live aggregation](../vendor/github.com/go-graphite/go-whisper/compress.go)
- [Classic routing, propagation and query grids](../vendor/github.com/go-graphite/go-whisper/whisper.go)
- [OOO read overlay and compaction aggregation](../vendor/github.com/go-graphite/go-whisper/ooo.go)
- [Historical Pebble archive routing and circular slots](https://github.com/go-graphite/go-whisper/blob/16b07882e95e65a1eb2c3c8df22712e795622bde/store/store.go)
