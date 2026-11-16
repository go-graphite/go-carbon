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
The bound reader renders published parts for the complete query interval and
fetches legacy only through the last uncovered physical slot. It preserves the
original query start when shortening the legacy end, so archive selection stays
identical. A verified `LegacyHorizon` also excludes trailing slots beyond every
stored legacy timestamp. Disjoint physical ranges fetch legacy directly. The
unbound compatibility path uses the complete interval on both backends. An occupied v2 physical slot wins over legacy, including stored NaNs
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
resolves legacy metadata once. Bound queries scan the occupied suffix, fetch
the uncovered legacy prefix, and merge only occupied part slots into the
legacy-owned result. A shortened prefix needs one bounded output extension;
the handle charges its additional 8B per query slot. Pooled part values stay
unchanged, so workspace cleanup remains proportional to part occupancy. A suffix
covered entirely by parts therefore avoids decoding that legacy data. Interior
holes still use the enclosing legacy prefix. The
unbound compatibility path overlays into legacy values through `RenderInto`. Converted or suppressed
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
uses a real legacy continuation store. By default its v2 sources are frozen
memory parts; `GO_CARBON_MIXED_PERSISTED=1` uses actual durable file sources
through `partspublish` instead. Neither mode is an SST backend or sustained-ingest
qualification.
The required production comparison still needs the same data, host and queries
on actual persisted v2 sources, including concurrent ingestion. The unified-index option requires the owner to publish that index; leaving it
off retains the slower per-query union. The diagnostic enables it against the
same immutable fixture catalog used by the native baseline. Warm and Find classes take
10,000 paired samples per repeat; fresh block-cache classes take 1,000, without dropping OS
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
zero adapter allocations. The earlier full-range overlay fixture added no allocations above frozen
legacy fetching; the suffix path can allocate an extended result when it shortens
the legacy output. The legacy
decoder and the existing index can still
allocate, and cold workspace/pool misses can allocate. These statements do not
establish a latency improvement or a persisted-part production qualification.

The persisted diagnostic uses the publisher's `PlanSeal`, `AcquireView`, and
`Collect` APIs. Parts and their complete envelope/frontier file are fsynced before
`Plan.Commit`. Ingestion seals a replacement generation every 10ms; every class
reports completed publications. The reader retains one generation throughout each
handle lifetime. This exercises real file-backed reads during durable publication;
it does not exercise the daemon's raw/archive SST transaction, target-rate input,
or coherent Reader turnover. Quiescent allocations use separate smaller diagnostic
samples. All latency-gate repeats use the larger counts above.

Request scratch is shared across immutable Reader generations after its handle,
values and catalog pointers are cleared. The deterministic fixture measures zero
warm adapter allocations above value-only legacy reads, and one Reader allocation
when constructing a fresh generation; fresh generations need no private pool or
scratch allocation. Complete reader/reducer and durable file-source prerequisites
are included as ordinary source, so this branch builds without an overlay.

`GO_CARBON_MIXED_COVERAGE=half GO_CARBON_MIXED_CLASS=warm-1h` selects a
separate fixture whose published suffix covers 2,400 of 3,600 query slots. It
does not replace any original gate class. `GO_CARBON_MIXED_BYTES=1` enables a
separate continuation value-byte and decoded-output counter diagnostic; the
unwrapped codec remains active in all timing gates.


Bound queries use a pinned-manifest density hint to choose their legacy path.
Below 12.5% estimated part coverage they fetch legacy once and overlay directly;
queries with no physical overlap bypass part rendering. Denser queries resolve
physical occupancy first, fetch through the last uncovered slot and join into
request scratch reused across immutable Reader generations. The hint uses only
manifest extents/sample counts (or frozen record counts), allocates nothing and
performs no file IO. Conservative spans and duplicate versions can overestimate
density; they affect cost only, because physical-slot resolution remains the
source of replacement ownership, including NaNs, late corrections and wrap.

The split preserves the original query start for native archive selection.
Interior holes remain in its enclosing legacy prefix. This adapter does not yet
provide forced-archive selective decoding into one caller-owned output; that
requires the legacy decoder API. Existing continuation block decoding remains
unchanged. The request-owned join scratch is borrowed until Fetch's consumer
returns and is covered by the handle's render byte reservation.
