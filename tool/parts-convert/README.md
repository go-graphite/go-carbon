# Offline history conversion diagnostic

`parts-convert` converts an exclusively owned, stopped legacy **copy**. The
`-convert-copy` mode deletes successfully converted source batches. Never point
it at a daemon's store directory or a shared backup. Source and output must be
separate, non-nested directories; the converter retains the source's exclusive
Pebble lock while using its read-only transfer adapter.

## Frozen render oracle

Before converting a real copy, save a seeded random sample of 2,000 names and
their legacy renders. The catalogue must contain at least 2,000 names. Sampling
visits the complete copied catalogue, retains at most 2,000 metadata records,
and chooses one archive and a range of at most 4,096 steps for each name. The
oracle stores the fixed clock, archive policies, range, output shape, and every
IEEE value bit, including NaN payloads and signed zero.

```sh
parts-convert -capture-real-copy /private/offline-copy \
  -real-oracle /private/render-oracle.json
parts-convert -convert-copy /private/offline-copy \
  -dir /private/converted -copy-batch-metrics 32 -copy-batch-mib 128
```

The oracle file is created exclusively. Capture does not modify the legacy
store. Conversion can be resumed with identical source/output paths after a
process crash; preserve its name cursor, plans, receipts, and target together.
Result counters from a resumed invocation cover that invocation, not the sum
of all previous invocations.

After conversion, verify the actual completed name cursor with `mixedread` and
the immutable part reader. The adapter builds a directory for the selected
names from the converted database, so this is an offline correctness diagnostic
and does not measure production reader latency or writer isolation.

```sh
parts-convert -verify-real-copy /private/offline-copy \
  -dir /private/converted -real-oracle /private/render-oracle.json
```

Add `-real-source-render http://127.0.0.1:8080` to compare the same 2,000 names
and frozen ranges with a source daemon. This explicit option sends only render
requests using the protobuf v3 read API. Every shape or value-bit mismatch
fails; changing points are not silently discarded. A source comparison can
fail when its history has expired or changed since acquisition, so keep the
copy interval, oracle clock, and source comparison time in the evidence.

## Qualification still required

Passing a fixture or this final render sample does not establish conversion
throughput, total-store disk bounds, concurrent mixed-reader publication,
crash recovery on real data, or a same-data storage comparison. Record the
physical source size, all retained point counts, one-second disk samples
(including WAL, staging, and open deleted files), a deliberate process kill
and resume, and final settled sizes separately. The existing
`-compare-coldpack` mode creates a **synthetic** fixture; it is not a cold-pack
baseline of an acquired real copy.
