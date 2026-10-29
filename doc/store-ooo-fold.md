# Folding shared-store out-of-order sidecars

The shared continuation store keeps the CWhisper writer's buffers and blocks.
Historically, late values went into a sparse classic-Whisper sidecar. Reads
merged that sidecar, with base corrections replacing main-file values and
partial coarse aggregates only filling holes. A fold also recomputes affected
coarse windows from finer data, subject to the aggregation method and xFilesFactor.
The admin pass, conversion, and write fallback reuse those same merge paths.

## Why converted hosts still have sidecars

Before `83c344c8` (import-sidecar-folds, integrated with the same canonical writer),
`ContinuationCodec.Import` copied both main and sidecar verbatim. Enabling
`store-ooo-direct-writes` did not change that. The conversion improvements in
`83c344c8` only attempted an in-place fold, and only with direct writes enabled.
A refused fold or even a fold error silently kept the verbatim pair. Idle
metrics never got a next update, and maintenance of small sidecars was subject
to age, size and rate budgets. Consequently, conversion completion did not
mean the store had no sidecars.

The supplied region A and region B rates are rates of operations, not counts of pending
sidecars. region A was converted on earlier builds and therefore could import more
legacy sidecars; a larger inherited population can feed more `sidecar_unfolded`
updates. Its sparse/coarse-window workload can also explain more tail rewrites.
That is a code-supported explanation, not a measured attribution of the region
difference. The new inventory and scan establish how much remains. No production
hosts were accessed for this investigation.

`InPlaceResult` means the following in the old fold-only path:

| Result | Why a sidecar could remain |
| --- | --- |
| `in_place` | All changes fit existing blocks; removes the sidecar. Storage/deletion errors can still prevent completion. |
| `rewrite_unsupported` | Storage cannot patch blocks; compressed version is old; blocks overlap; Mix aggregation; or an unusable sidecar. |
| `rewrite_extend` | Estimated point size is too small and the file needs extension. |
| `rewrite_overflow` | Re-encoding corrections no longer fits the block/ring capacity. |
| `rewrite_tail` | A merge would append beyond an archive's encoded range, often a new sparse coarse aggregate. The rewrite must order/recompute it. |
| `rewrite_late_batch` | A colliding update reaches the newest base interval or earlier; replay is needed to order the batch. This result belongs to the batch-collision path, not the standalone fold decision. |

`DirectLateResult` describes placement during an update:

| Result | Placement |
| --- | --- |
| `direct` | Replaces base slots and recomputes their coarse windows in existing blocks. |
| `sidecar_unfolded` | An older sidecar did not fold first; writing alongside it would let older corrections beat newer values. |
| `sidecar_unsupported` | No safe in-place patch path for this format/storage/layout. |
| `sidecar_overflow` | The in-place encoding would overflow/rotate the current block. |
| `sidecar_tail` | A point or recomputed window lies beyond the encoded range and cannot be resolved through existing buffers. |
| `sidecar_coarse` | A rejected coarse-archive value is not a base correction; it must retain the merge's hole-filling precedence. |

With this change, healthy sidecars in the shared store use a full rewrite when
in-place folding is refused. A direct-write update folds an existing sidecar
first and folds any new fallback diversion before its atomic commit. The
`sidecar_*` direct counters may therefore count transient merge inputs without
leaving a stored sidecar. Original on-disk CWhisper files and stores with direct
writes disabled still need sidecars because their append-only encoder cannot
insert every late value. Corrupt or incompatible sidecars require repair: the
pass fails on that metric, retains its data, and can resume after repair.
Actual `Mix` aggregation is rejected by the shared-store policy and by
CWhisper's sidecar merger (its duplicated archive grids cannot form a classic
sidecar). It cannot be safely guessed into a different aggregation method.

## Inventory and scan

- `store_ooo_sidecars`: physical sidecars currently catalogued.
- `store_ooo_sidecar_points`: occupied physical slots, including expired records.
- `store_ooo_sidecar_oldest_timestamp_seconds`: oldest occupied sample timestamp;
  zero with no records. This is sample age, not sidecar creation time.
- `store_ooo_inventory_ready`: one when the inventory covers the complete store.

Each sidecar group updates its count and ordered minimum-timestamp index when
written. Its catalog record commits with the metric. Global counts and a heap
of per-sidecar minima are maintained on create/change/fold/delete. Scrapes are
O(1), with no store scan. Restart loads only sidecar catalog records. A legacy
store reports readiness zero until its first complete admin scan indexes old
sidecars; partial totals must not be interpreted as complete. After running an older
binary that does not maintain this inventory, run a complete scan again.

`GET /admin/store/sidecars` returns these totals. `POST` with `action=scan`
walks the metric catalog in bounded pages, including idle/staged metrics. `after`
is an exclusive cursor; `limit` is 1–10000. A complete sequential scan starting
with an empty cursor establishes readiness. `dry-run=true` reports the physical
inventory without committing indexes or changing readiness.

## Fold pass

Build `tool/store-ooo-fold`. It uses the running daemon and shares its per-metric
locks; it does not open the daemon's Pebble database separately.

```sh
store-ooo-fold -url http://127.0.0.1:8080 -dry-run
store-ooo-fold -url http://127.0.0.1:8080 -scan
store-ooo-fold -url http://127.0.0.1:8080 -checkpoint /tmp/ooo-fold.json
store-ooo-fold -url http://127.0.0.1:8080 -checkpoint /tmp/ooo-fold.json -rate 500
```

The default pass uses the sidecar catalog directly when it is ready, and
initializes legacy inventories with a scan. It runs without a pacing delay. `-batch` bounds each request and
`-rate` optionally sets folds/second. Restart the same command to resume after
the last completed page. An interrupted page is safe to retry: completed folds
have removed their catalog entries. New entries behind a saved cursor need a
new pass; a nonzero ending inventory starts another pass until none remain. A completed checkpoint restarts from the beginning next time.

`POST /admin/store/sidecars?action=fold&limit=1000&after=...&rate=0` exposes the
same operation; `dry-run=true` only counts indexed sidecars. A legacy store must
complete its initial scan first. A request cancellation discards the current
metric's uncommitted batch. History rewritten during admin folding uses the
conversion oracle's epoch horizon, preserving all physical source records.

## Conversion guarantee

Every production `ContinuationCodec.Import`, regardless of the direct-write
setting, stages and folds the source sidecar before publishing keys. Tail,
overflow, extension and older formats take a full rewrite. A failed fold fails
the conversion and keeps its original source; it never falls back to importing
a sidecar. Physical verification repeats the same fold independently of the
live retention clock. At the end of direct-write incremental conversion,
`CheckNoSidecars` requires a ready inventory and a zero count before reporting
the shared phase. Resumed legacy stores need the initial admin scan/fold first.
