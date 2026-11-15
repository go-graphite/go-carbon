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
    Converted: &mixedread.Cursor{Version: 1, Before: nextNameBytes},
    SuppressLegacy: isDeletedOrNewIncarnation,
})
err = reader.Fetch(ctx, name, from, until, func(s *partsread.Series) error {
    return encodeResponse(s) // finish before callback returns
})
```

Find queries union both catalogs through their existing Graphite find indexes,
with bounded duplicate tracking. The package has no competing glob parser.
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
reading, accounting for both output workspaces. An all-occupied v2 result skips
the legacy fetch. Results and their budgets live only through the callback.

## Qualification

The mixed reader is not wired to a production backend. Passing correctness does
not establish the required latency gate. `TestLatencyComparison` is an opt-in,
private-directory diagnostic for cold block-cache and warm mixed overhead. It
uses in-memory frozen v2 parts and a real legacy continuation store, so it is a
best-case adapter comparison, not an SST or sustained-ingest qualification.
The required production comparison still needs the same data, host and queries
on actual persisted v2 sources, including concurrent ingestion. The per-query
find union also needs comparison with a unified publication-time find index.
