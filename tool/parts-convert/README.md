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

## Physical ID ranges and IO pacing

For an offline copy, `-copy-id-order` first finishes any existing active name
batch, then converts in ascending legacy numeric ID order. Each range freezes
its exact names, catalog records, revisions and bucket list. Adjacent owned IDs
share range tombstones; unconverted holes are preserved. The existing reclaimer
closes readers, commits range deletes, compacts those exact spans and reopens
the remaining source. `ID_RANGE_RECLAIM` records source logical SST bytes and
allocated bytes before and after every range, together with target allocation.
Small ranges need not release an allocation block; tombstones are never counted
as recovered space.

The independent `ranges/state.json` and reclaimed receipts describe arbitrary
verified name coverage. They never advance the lexical `names/name-cursor.json`.
This is an offline adapter; a live mixed reader would need the corresponding
immutable name-coverage/catalog/parts generation and reader pinning. Final oracle
verification accepts a completed ID journal only with matching source/target
identities and an empty remaining legacy catalog.

```sh
parts-convert -convert-copy /private/offline-copy -dir /private/converted \
  -copy-id-order -copy-batch-metrics 4096 -copy-batch-mib 8 \
  -copy-original-allocated-bytes 8267907072 -copy-io-mbps 8
```

Use the measured original source allocation for the actual copy. Keep it unchanged
across all resumes. Admission includes retained target bytes and refuses an
already over-budget store before writing identities or opening writable DBs.
The next output bound is also charged against original allocation plus 5%.
This does **not** bound shared-source SST compaction scratch, deleted-open files,
or filesystem allocation rounding; a separate complete-allocation guard and
physical rewrite reservation remain required. `DiskCapQualified` stays false.

Every destructive CLI conversion defaults to `-copy-io-mbps 8` (decimal MB/s).
One shared pacer charges data reads and writes for source, target, external SSTs,
verification, WAL and compaction. Operations use at most 64 KiB; cached reads count,
and prefetch/preallocation are disabled. Filesystem metadata, fsync and small JSON
journals are outside this data-byte budget. This is a conservative application
byte cap, not a promised device MB/s cap or a daemon health qualification. Observe
the daemon's commit rate and stop the converter if the host health gate fails.

Reclaimed name batches retain both a complete global receipt and duplicate unit
cursor JSON. The maintenance command below compares every duplicate with the
retained complete global authority before removing any duplicate. It keeps all
SSTs, global journals, active/published batches, and stage directories. This can
recover metadata allocation without rewriting archive data. Measure complete
allocation afterward; the reported potential release is not source throughput.

```sh
parts-convert -prune-reclaimed-copy /private/converted
```

## Pack an offline converted target

Small name batches may leave many tiny ingested SSTs. The packing diagnostic
streams the complete existing target in key order into new SSTs, combining
records across name batches within each exact placement-group/vshard/partition.
Every logical key (including immutable Part ID) and value stays unchanged.
`-pack-mib` is an uncompressed payload target, not a minimum file size: a boundary
tail must stay smaller when that entire boundary contains little data. Inventory
reports the number of distinct boundaries, which is a lower bound on file count.

```sh
# Read-only data inventory; the maintenance lock/recovery metadata is writable.
parts-convert -pack-copy /private/converted -copy-io-mbps 8

# Admission uses the same original allocation as conversion, never a reset base.
parts-convert -pack-copy /private/converted -pack-apply -pack-mib 32 \
  -pack-source /private/offline-copy \
  -copy-original-allocated-bytes 8267907072 -copy-io-mbps 8
```

The source remains untouched. Output is built in `packing-next` with automatic
compactions disabled, then read back completely. Canonical key/value SHA-256,
record/occupied-slot counts and metadata must agree. Actual output SST bounds
are checked against the transfer boundary. A destination-bound `PACKING.json`
journal records verification before the directory swap. Conversion and oracle
verification recover that journal under the same maintenance lock before opening
`parts`. Recovery handles both renames and retains the old DB until the verified
replacement is installed durably. This is an offline directory replacement; it
does not provide online reader pinning or shared-SST publication.

Admission first runs the identical SST encoder into a counting sink, charges
those compressed lengths rounded to 4 KiB per file and a 16 MiB DB allowance
before creating the replacement. Packing
needs both generations until verification completes. This is conservative
accounting, not a certified filesystem allocation maximum; original-conversion
source-compaction scratch, directory blocks, metadata and open-deleted files
still require qualification. `DiskCapQualified` remains false. Inspect the full
owned-root peak, including preparation, rather than calling final savings a
whole-run disk bound. Packing leaves all existing coverage and recovery receipts
unchanged and does not itself resume or delete any legacy batch.
