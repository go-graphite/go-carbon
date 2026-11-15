# Mixed legacy and immutable-part reads

This package is an experimental reader boundary. It requires a real
`chunkstore.Options{ReadOnly: true}` legacy handle and the immutable catalogs and
part publications from `internal/partsread`. The storage owner drains readers
before closing either source. It does not open an actively written database.

```go
reader, err := mixedread.New(mixedread.Options{
    Legacy: legacy,
    LegacyCatalog: legacyIndex,
    Parts: partsAdapter,
    UnifiedCatalog: true, // partsAdapter.Catalog already indexes the visible union
    Converted: &mixedread.Cursor{Version: 1, Before: nextNameBytes},
    SuppressLegacy: isDeletedOrNewIncarnation,
})
err = reader.Fetch(ctx, name, from, until, func(s *partsread.Series) error {
    return encodeResponse(s) // finish before callback returns
})
```

With `UnifiedCatalog: true`, find queries the single published Graphite index.
The owner must seed legacy names, include new v2 names, and apply suppression
and tombstones before publishing that same immutable catalog generation.
This removes the second index lookup and per-request deduplication map. The
option defaults off; the compatibility path unions both existing indexes with
duplicate tracking bounded by name count and the pinned byte reservation.
Neither path implements a glob parser.
Render uses the same complete query interval on both backends, preserving archive
selection. An occupied v2 physical slot wins over legacy, including stored NaNs
and replacements whose newer ring timestamp invalidates the requested old slot.
Untouched slots retain the legacy render. Applying that priority across all time
also preserves late post-cutover corrections to earlier timestamps. A simple
split on the sample timestamp would silently lose those corrections.

The same schema and aggregation policy must remain in force for a shared name;
a mismatch fails closed. The caller must publish epoch changes, legacy
suppression/tombstones, catalogs and selected sources coherently. A new name
incarnation must suppress old legacy history. The portable storage catalog and
its fencing authority are not implemented here.

The converter cursor is an exclusive name prefix: names strictly below `Before`
use v2 alone; equality retains legacy. `Done` means all names are converted.
JSON encodes `Before []byte` as base64 in `before_base64`, preserving arbitrary
metric name bytes. A raw JSON string is insufficient for invalid UTF-8 names.
Persist and publish a cursor only after target verification and durability.
Drain old pins before reclaiming source data. This reader performs no deletion;
name order is not the legacy data's physical SST order. `Filter` freezes its
boundary and validates the format before publication.

Coarse archives must be authoritative sealed rollup outputs. The seal reducer
must inherit the existing physical fine/coarse state at cutover, including a
coarse bucket that straddles it; query-time reaggregation and combining partial
averages are incorrect. The differential test seeds `sealrollup.Reduce` with that
state and preserves every original update batch and admission clock. It compares
classic Whisper with legacy-only, v2-only and mixed renders across seventy
schema shapes, six aggregation methods and 1h/1d/7d/30d queries.

A mixed render reserves twice the part adapter's configured work budget before
reading, accounting for the legacy output and bounded winner workspace. It
resolves legacy metadata once and overlays occupied v2 slots directly into
the legacy values through `partsread.Handle.RenderInto`. No second dense
output, full occupancy scan, or dense merge is needed. Converted or suppressed
names skip legacy lookup and fetching entirely. `LegacyHorizon` optionally
skips legacy data reads for intervals strictly after its verified inclusive
timestamp bound, or after all legacy archives have expired. That bound must
cover every physical archive slot, including future timestamps; the log
cutover clock alone is insufficient. A missing v2 catalog entry still falls
back to legacy, preserving the legacy-only name shape. The one-slot compatibility
path preserves Whisper's distinction between populated and empty zero-width
archives. Results and their budgets live only through the callback. Treat the
borrowed series, values and occupancy mask as read-only; copy any data that must
outlive the callback. The one-slot fallback overlays into legacy's owned buffer
instead of modifying untouched slots in the pooled part workspace.

## Qualification

The mixed reader is not wired to a production backend. Passing correctness does
not establish the required latency gate. `TestLatencyComparison` is an opt-in,
private-directory diagnostic for cold block-cache and warm mixed overhead. It
uses in-memory frozen v2 parts and a real legacy continuation store, so it is a
best-case adapter comparison, not an SST or sustained-ingest qualification.
The required production comparison still needs the same data, host and queries
on actual persisted v2 sources, including concurrent ingestion. The unified-index option requires the owner to publish that index; leaving it
off retains the slower per-query union. The diagnostic enables it against the
same immutable fixture catalog used by the native baseline. Warm and Find classes take
1,000 paired samples; fresh block-cache classes take 100, without dropping OS
caches. `GO_CARBON_MIXED_COMPARE=native` selects an independent native/native
control with identical data and seeded shuffling. `GO_CARBON_MIXED_RAW=1` emits
paired nanoseconds before percentile sorting. Run the three seeds 1409, 7321,
and 9077 for both arms. The paired, repeat-stratified bootstrap uses 10,000
draws and a two-sided 95% percentile interval for each p50/p99 ratio. A class
passes only when both mixed/native upper bounds are at most the larger of 1.02
and the corresponding native/native upper bound. Individual strict rows remain
diagnostic; exit zero means the workload completed, not that this gate passed.

`GO_CARBON_MIXED_ALLOCS=1` measures query-body allocations separately; its
process-wide counters can include background bookkeeping. Keep this out of
latency samples. `GO_CARBON_MIXED_INGEST=1` continuously publishes bounded
memory parts while reads retain their immutable generation. A publication
barrier starts each writer before reads, and writer receipts report progress.
This exercises memory publication and snapshot isolation, not persisted ingest,
reader-generation turnover, or a target-rate writer soak.

## Bound immutable generations

A publisher can opt into `Options.Snapshot` to retain one part generation for
an entire reader lifetime. Publish a new Reader whenever that generation changes,
stop admitting callers to the old Reader, drain its callers, then close its
Snapshot and legacy store. `Snapshot.Close` is only valid after that drain;
it must not race with `BorrowInto`, rendering, or another Close.

The snapshot catalog must implement `partsread.LegacyCatalog`. Its single
`LookupLegacy` call returns the part metric, a `*mixedread.LegacyBinding` for
legacy-present names, and existence. A nil binding explicitly means v2-only.
Build a binding once with `legacy.ResolveFrozen(ctx, name)` followed by
`mixedread.BindLegacy(frozen, partMetric)`. The constructor verifies archive
policy without requiring the two stores' numeric IDs to match. A union name
alone never establishes legacy presence. The complete catalog must retain the
existing index's Find implementation and coherent suppression rules.

Pass the retained `publication.Snapshot()` and `UnifiedCatalog: true` to New.
The bound Fetch resolves one generation-checked catalog token for both render
paths; `FetchFrozenValues` uses the publication-time legacy metadata while preserving
native store read admission, read-view cleanup, cancellation, and close handling.
Its callback exposes only shape and owned values, so immutable metadata can
stay internal without a per-request retention clone. `Fetch` and `FetchFrozen`
continue returning owned metadata copies.

Bound requests reuse handle/result storage and a sparse workspace callback.
Their adapter bookkeeping performs no publication retain/release and introduces
no writer lock; native legacy admission and read-view synchronization remain.
The byte budget still charges every active request. Warm v2-only renders have
zero adapter allocations. A small warm mixed fixture also adds no allocations
over frozen legacy fetching (14 mixed versus 15 frozen allocations/request). The legacy
decoder and the existing index can still
allocate, and cold workspace/pool misses can allocate. These statements do not
establish a latency improvement or a persisted-part production qualification.
