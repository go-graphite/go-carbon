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
oracle records namespace and actual archive-policy counts across the complete
copied catalogue, separately from its 2,000-query sample. Namespace means the
first name component; the policy key includes aggregation method, XFilesFactor
bits, and every archive step/count. Accounting refuses more than 4,096 distinct
namespaces or policies before any destructive conversion. Use these counts to
check actual schema coverage; frequency words in metric names alone do not prove
archive shape. The oracle stores the fixed clock, archive policies, range, output
shape, and every IEEE value bit, including NaN payloads and signed zero.

```sh
parts-convert -capture-real-copy /private/offline-copy \
  -real-oracle /private/render-oracle.json
parts-convert -convert-copy /private/offline-copy \
  -dir /private/converted -copy-batch-metrics 32 -copy-batch-mib 128
```

Capture also records each legacy Fetch duration. Verification reports legacy and converted p99 on the same saved queries. These are sequential single-pass measurements: legacy before deletion, converted afterward, with uncontrolled engine and OS cache state. Converted timing includes the exact comparison; index construction is outside the timed renders. Old oracle files without timings report legacy p99 as zero, which means unavailable.

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

When `-real-source-render` is set, verification times both arms on the same
saved queries, alternating their order. Both arms compare against the frozen
oracle; any source drift fails the run. `PairedQueries` counts successful pairs,
and `CapturedLegacyP99NS` retains the original pre-deletion diagnostic separately.
The paired legacy arm includes HTTP and protobuf decoding, while the converted
arm runs in-process. Cache state is uncontrolled, so this is an explicitly
unequal-transport diagnostic, not an engine latency qualification.

Offline copy batches stop at either `-copy-batch-metrics` or the conservative
aggregate `-copy-batch-mib` output estimate, whichever comes first. This permits
large sparse name batches without rejecting a dense range merely because the
name limit was high. A single metric that exceeds the byte cap still fails
before publication or deletion. Saved pending plans keep their exact bounds on
resume. The byte estimate does not replace sampling the full conversion disk
footprint, including shared-SST compaction and cumulative target growth.
