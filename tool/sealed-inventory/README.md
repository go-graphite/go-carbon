# Offline SST inventory and float transcode

`sealed-inventory` opens immutable SST files through the vendored Pebble sstable reader. It never opens a Pebble DB, takes a database lock, reads a MANIFEST, or writes into `-sst-dir`. Only the tool and tests import `internal/floatresearch`; the production codec and durable formats are unchanged.

```
sealed-inventory inventory -sst-dir DIR -files 20 -read-mb-s 200 -procs 4 -out inventory.json
sealed-inventory transcode -sst-dir DIR -policy current,prefix256,minfour,alp,chimp -read-mb-s 200 -procs 4 -tmp DIR2 -out transcode.json
```

`-files 0` means all files; positive limits take filenames in sorted order. Sample the files before linking them if you want a size-weighted sample. Reports must have a new filename outside the input directory. `-tmp` must already exist outside the input directory, including after resolving symlinks. SST symlinks are refused; inputs are opened with `O_RDONLY|O_NOFOLLOW`.

The tool processes files and policies serially. `-procs` caps GOMAXPROCS; serial codec measurement avoids attributing concurrent policy work to the wrong policy. Progress goes to stderr every ten seconds, with a baseline-size diagnostic per file.

The temporary filesystem must have at least **50 GiB free** initially and before each rewrite, and capacity for twice the sampled input. A write guard prevents live outputs from exceeding **2× sampled input**; a periodic disk guard stops writes below **25 GB free**. Normal operation retains only one output at a time and deletes it after measuring. `-keep` retains tables, subject to that same aggregate 2× limit: several policies may exceed it and cause a refusal. Use a single candidate policy when retaining a pair. SIGINT/SIGTERM cancels throttled reads and removes the run's temporary outputs, including retained outputs from earlier files. Completed unrelated runs are untouched.

The paired baseline rewrites every internal point key/version and its original value. Every candidate preserves those keys, point deletions, and range deletions; it changes only SET values for sealed float-byte-plane groups. Same-file newer range deletions are reported as tombstoned entry/point counts, without dropping physical bytes or applying compaction. Cross-file visibility cannot be inferred from independent SSTs.

Policies are `current` (unchanged values, current encode/decode CPU control), `chimp` (adaptive Chimp128 + optional inner Zstd), `alp` (the existing exact scalar decimal-with-exceptions experiment), `prefix256` (current/Chimp/ALP/dictionary trials on 256 points, then one complete candidate), and `minfour` (minimum complete current/Chimp/ALP/dictionary frame). All candidate policies include construction of a complete current fallback in encode CPU. Each group resets codec state; every eligible native group length is considered. A candidate must be strictly smaller than the original frame **including its two-byte experimental dispatch envelope**. Every timestamp and IEEE word, including signed zeros and NaN payloads, is checked after candidate decoding; a mismatch, codec error, or candidate panic retains the original and increments `OracleFailures`. Invalid source frames stop the run. Outputs are private experiments and **must never be ingested into a live store**.

Writer options follow `internal/chunkstore/store.go`: default Pebble 4 KiB blocks, restart interval 16, threshold 90%, Snappy outer compression, whole-key bloom when the source has one. Copy the latest regular `OPTIONS-*` into the sample directory to supply scalar per-level settings. Merger name and table format come from each SST; bloom bits/key are inferred from serialized filter length. Optional `-levels-json levels.json` takes `{"000123.sst":6,"000124.sst":2}` from an independently known snapshot. Otherwise `-level 6` applies to unknown files. The tool does not infer levels by opening a MANIFEST. Confirm every `BaselineWithin3Pct` before using paired savings as a settled-stock estimate. A larger baseline delta is explicitly reported rather than hidden. Range-key and out-of-line value-block SSTs are refused for transcoding; the latter are also refused for inventory because proportional data-block attribution would omit value bytes. Production at this revision uses in-place values and range deletions.

Pebble's Snappy is pure Go. With `CGO_ENABLED=0`, its optional Zstd path also uses vendored klauspost Go code; research inner Zstd already uses that implementation. **No cgo is required.**

## Commands for an operator on a host

Copy the Linux binary to the host using your separately authorised workflow. These are on-host commands; no production access was used to build or validate the tool. Set `store` and `bin` to the actual paths. The recipe selects N files without replacement with probability proportional to size at each draw (exponential-race sampling), pins them with hard links, and records the selection. Files removed by compaction before linking are skipped. No DB is opened.

```bash
store='<store-dir>'
bin='/tmp/sealed-inventory-linux-amd64'
sst_src="${store}/shared"
run_dir="$(mktemp -d "${store}/float-measure-XXXXXXXX")"
sample_dir="${run_dir}/sample"
work_dir="${run_dir}/rewrite"
mkdir -m 700 "${work_dir}"
sha256sum "${bin}"

python3 - "${sst_src}" "${sample_dir}" 20 8144 > "${run_dir}/sample.json" <<'PY'
import json, math, os, random, shutil, stat, sys
source, target, n, seed = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
os.mkdir(target, 0o700)
if os.stat(source).st_dev != os.stat(target).st_dev:
    raise SystemExit('sample must be on the SAME filesystem as shared; choose another run_dir')
rng = random.Random(seed)
ranked = []
source_bytes = 0
for e in os.scandir(source):
    if not e.name.endswith('.sst'):
        continue
    try:
        s = e.stat(follow_symlinks=False)
    except FileNotFoundError:
        continue
    if not stat.S_ISREG(s.st_mode) or s.st_size <= 0:
        continue
    source_bytes += s.st_size
    ranked.append((-math.log1p(-rng.random()) / s.st_size, e.name, s))
ranked.sort(key=lambda x: x[0])
selected = []
for _, name, before in ranked:
    destination = os.path.join(target, name)
    try:
        os.link(os.path.join(source, name), destination, follow_symlinks=False)
    except FileNotFoundError:
        continue
    after = os.stat(destination, follow_symlinks=False)
    if not stat.S_ISREG(after.st_mode) or (after.st_dev, after.st_ino, after.st_size) != (before.st_dev, before.st_ino, before.st_size):
        os.unlink(destination)
        continue
    selected.append({'name': name, 'bytes': after.st_size})
    if len(selected) == n:
        break
options = sorted(e.name for e in os.scandir(source)
                 if e.name.startswith('OPTIONS-') and e.is_file(follow_symlinks=False))
if options:
    shutil.copyfile(os.path.join(source, options[-1]), os.path.join(target, options[-1]), follow_symlinks=False)
json.dump({'seed': seed, 'design': 'successive PPS without replacement',
           'source_files_seen': len(ranked), 'source_bytes_seen': source_bytes,
           'selected': selected}, sys.stdout, indent=2)
if not selected:
    raise SystemExit('no files linked')
PY

"${bin}" inventory -sst-dir "${sample_dir}" -read-mb-s 200 -procs 4 \
  -out "${run_dir}/inventory.json" 2> "${run_dir}/inventory.log"
"${bin}" transcode -sst-dir "${sample_dir}" -read-mb-s 200 -procs 4 \
  -policy current,prefix256,minfour,alp,chimp -tmp "${work_dir}" \
  -out "${run_dir}/transcode.json" 2> "${run_dir}/transcode.log"

python3 - "${run_dir}/transcode.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
print('input bytes:', r['FileBytes'], 'sealed share %:', r['SealedSharePct'], 'float share %:', r['FloatSharePct'])
print('baseline OK:', all(f['BaselineWithin3Pct'] for f in r['Files']))
for name, p in r['Policies'].items():
    print(name, 'bytes', p['Bytes'], 'saved %', p['WholeFileSavingPct'],
          'float saved %', p['FloatBytesSavingPct'],
          'encode/decode ns/pt', p['EncodeNSPerPoint'], p['DecodeNSPerPoint'],
          'encode/decode delta ns/pt', p['EncodeDeltaNSPerPoint'], p['DecodeDeltaNSPerPoint'],
          'oracle failures', p['OracleFailures'])
PY

# Remove only the pinned links and copied OPTIONS; keep reports/logs/sample manifest.
python3 - "${sample_dir}" <<'PY'
import os, sys
root = sys.argv[1]
for e in os.scandir(root):
    if e.name.endswith('.sst') or e.name.startswith('OPTIONS-'):
        os.unlink(e.path)
os.rmdir(root)
PY
rmdir "${work_dir}"
printf 'Reports: %s\n' "${run_dir}"
```

Hard links pin obsolete SST space until removed; remove the sample even after a failed or interrupted command. If `shared` is a separate mount, place `run_dir` on that mount while keeping the sample/rewrite directories outside the SST input directory. The 20-file example is a starting sample, not a fleet-wide estimate. The aggregate describes selected file bytes; PPS sampling and mixed levels require inclusion weighting or an all-files scan for an unbiased cluster estimate. Compare settled L6 samples across hosts/archives before extrapolating the fleet forecast. There is no cross-file compaction or production format deployment in this experiment.

## JSON fields and interpretation

Field names use Go's exported names (PascalCase). Top-level totals aggregate `Files` by bytes/points, never by averaging percentages.

| Field | Meaning |
| --- | --- |
| `FileBytes`, `Files[].Bytes` | Exact stat size of sampled SSTs, including every footer/trailer |
| `Classes` | `sealed/<encoding>/<zstd or raw>`, `ordinary`, `descriptor`, `catalog/metadata`, `journal/other`, unknown groups |
| `Classes.*.Entries`, `Points`, `LogicalBytes` | All internal entries, encoded points, user-key + value bytes (8-byte internal sequence/kind trailer excluded) |
| `Full4096`, `Partial`, `Larger` | Sealed group counts at 4096, below 4096, and above 4096 points |
| `PhysicalBytes` | Compressed data-block length + 5-byte trailer, divided among its entries' classes in proportion to logical key/value bytes |
| `DataBytes`, `OverheadBytes` | Exact compressed data-block sum and all remaining file bytes; add to `FileBytes` |
| `IndexBytes`, `FilterBytes` | Subsets of `OverheadBytes`; remainder includes range deletions, properties, metaindex and footer |
| `SealedDataBytes`, `FloatDataBytes`, `SealedSharePct`, `FloatSharePct` | Attributed sealed/float data bytes and their shares of **whole SST bytes**, including overhead in the denominator |
| `TombstonedEntries`, `TombstonedPoints`, `RangeDelSpans`, `RangeDelKeys` | Same-file range deletion accounting; no physical bytes discarded |
| `Level`, `LevelSource`, `WriterOptions` | Explicit level map or fallback L6, actual writer configuration and its source |
| `BaselineBytes`, `BaselineDeltaPct`, `BaselineWithin3Pct` | Unchanged rewrite size; delta against original; required agreement diagnostic |
| `Policies.*.Bytes`, `SavedBytes`, `WholeFileSavingPct` | Candidate rewrite size and savings against the **paired unchanged rewrite**, not a mix of original and rewritten sizes |
| `Policies.*.FloatGroupBytes` | Exact sum of encoded eligible float SET-value frame lengths after policy selection, including unchanged fallbacks; excludes keys, outer SST compression and SST overhead |
| `FloatBytesSavingPct` | Whole-file paired byte saving / original attributed float-group physical bytes; block/index effects are included in the numerator |
| `EncodeCPU`, `DecodeCPU`, `Points` | Process CPU nanoseconds and eligible float-point denominator |
| `EncodeNSPerPoint`, `DecodeNSPerPoint` | CPU / points, including full current fallback construction for candidate encoding |
| `EncodeDeltaNSPerPoint`, `DecodeDeltaNSPerPoint` | Policy minus `current`; valid when `current` was requested (negative = less CPU) |
| `ChangedGroups`, `OracleFailures`, `Winners` | Strictly smaller groups, rejected/error/mismatching groups, dispatch choices including unchanged `sealed` |
| `Architecture`, `GoVersion`, `Procs`, `ReadMBPerSecond`, `ReadBytes`, `WallSeconds` | Measurement environment, read limiter setting (MiB/s), actual read-call bytes and elapsed time |

Attribution exactly reconciles file bytes under the stated proportional-block convention (floating-point roundoff aside). It is **not** an assertion that a class's compressed representation is independently separable inside a mixed block. The paired transcode measures the actual response of the writer to changed float frames. Entries obsolete across files remain in stock accounting. A nonzero oracle-failure count or poor baseline agreement needs investigation before using the result.

## Runtime and IO planning

Inventory reads data twice: entry-framing counts establish exact physical block membership, then Pebble supplies decoded keys/values and verifies checksums. Budget approximately **2 GB read per GB of sampled SST**, plus indexes/properties. At 200 MiB/s the read floor is about **10 seconds/GiB**, before decoding and filesystem overhead.

With P policies, transcode adds P+1 full read/write passes (one unchanged baseline plus policies). Default P=5 therefore costs approximately **8 GB reads and at most 6 GB output writes per input GB**, with one temporary output live at a time. The read floor is about **41 seconds/GiB** at 200 MiB/s. CPU can dominate: for float-byte density D stored bytes/point and measured sum of policy encode+decode costs C ns/point, codec CPU alone is approximately `C / D` CPU-seconds per decimal input GB if the entire input is float groups (scale by the measured float share). Allow minutes/GB for float-heavy files; measure one file first, then use `WallSeconds / FileBytes` for this host and archive mix. No x86 throughput number is claimed from Darwin fixtures. `getrusage(RUSAGE_SELF)` includes user+system CPU, allocations and incidental runtime/GC activity; timing excludes source decoding, SST IO/writing and the oracle comparison. Short groups and very small samples are noisy.

## Local reproduction

Use Go 1.27.0, `CGO_ENABLED=0`, `GOFLAGS='-mod=vendor -p=4'`, `GOMAXPROCS=4`, with TMPDIR/GOTMPDIR/GOCACHE in the task scratch directory. Tests use real chunkstore Create/Flush/Checkpoint fixtures, verify Pebble layout/property accounting, class counts, <3% paired baseline agreement, all emitted key/value changes, special IEEE patterns, range tombstones, path guards and SIGTERM cleanup. All test invocations have a 150-second timeout.

```
go test -timeout 150s ./tool/sealed-inventory ./internal/chunkstore -run 'TestInventoryAndTranscode|TestSafetyAndRangeDeletes|TestPolicyOracleSpecials|TestSIGTERMCleansOutputs|TestFloatResearchOracle|TestFloatForecastSelectors' -count=1
go build ./...
go vet ./internal/floatresearch ./tool/sealed-inventory ./internal/chunkstore
golangci-lint run --max-same-issues=0 ./...
GOOS=linux GOARCH=amd64 go build -trimpath -o sealed-inventory-linux-amd64 ./tool/sealed-inventory
GOOS=darwin GOARCH=arm64 go build -trimpath -o sealed-inventory-darwin-arm64 ./tool/sealed-inventory
```

## Ordinary-group reasons (schema version 2)

`inventory` adds `OrdinaryByReason` and disjoint `Ordinary` rows, both per file
and in the aggregate. Their physical/logical bytes, entries and points reconcile
with `Classes.ordinary`. Each row includes the archive ordinal, step and retention
seconds, key layout, sampled native-block position, descriptor eligibility flags,
and whole-range temperature against `-write-window` (default 24h).
`OrdinaryCutoffUTC` records the cutoff. Rows distinguish non-bucketed/pre-relayout,
count <=128, count >65536, packed-descriptor/ordinary-version, hot range,
cold eligible shape (benefit/backlog unknown), missing metadata, layout/count
mismatch and classic/sidecar groups. Descriptor flags preserve overlapping causes.

Metadata is sought across the **selected SSTs only**, using read-only readers,
never a DB or MANIFEST. Archive schemas come from continuation file manifests in
those SSTs; keys alone encode only archive ordinal, so unknown step/retention is
zero with `SchemaSource=key-layout/archive-only`. `newest-observed/open-candidate`
means greatest sampled descriptor-tail timestamp, not proof of the current writer
head. `older-than-observed-newest` is also relative to this sample. Missing tails
leave temperature/position unknown. Sampled point versions are joined by greatest
sequence; range deletions across files and unsampled metadata are unresolved.
A descriptor/layout mismatch may be obsolete physical data, not corruption.
These limitations cannot be repaired by assuming that the highest block index is
newest: native blocks are a ring.

The metadata lookup uses a 128-metric LRU with at most 8192 metadata keys / 2 MiB
per metric. Oversized metadata is labelled `metadata_budget`. Lookup/tail reads
share the byte limiter and `ReadBytes`; sparse metadata can cause substantial
extra random reads beyond the original two-pass inventory estimate. A 20-SST
sample can legitimately report mostly unknown schema/descriptor state if the
metadata lies in different, unselected files. Use an independently pinned broader
sample to resolve it; this tool does not inspect the live database.

```sh
sealed-inventory inventory -sst-dir "$sample_dir" -read-mb-s 200 -procs 4 \
  -write-window 24h -out "$run_dir/ordinary-reasons.json"
```

The v10 coverage canary is also recognized: `sealed/prefix/<encoding>` counts
immutable payloads (points counted once), and `sealed/prefix-reference` counts
writer-facing slot references (zero additional points). Transcode policies leave
these private v10 envelopes unchanged. The v9 ordinary reason model is unchanged.

`-as-of 2026-11-04T12:00:00Z` pins the reason-model clock for reproducible
fixtures or historical samples. It changes only temperature classification;
`StartedUTC`, wall time, IO accounting and the read-only scan remain actual.

### Resolve metadata outside the data sample

`inventory -sst-dir /immutable/sample -metadata-dir /immutable/all-store-ssts`
keeps byte accounting restricted to the sample, but resolves native descriptors
and manifests across all `.sst` files in the metadata directory. Without
`-metadata-dir`, all files in `-sst-dir` supply metadata, including files excluded
by `-files`. Both directories must be pinned immutable files (for example a
checkpoint or separately managed hard links). No DB, MANIFEST, WAL or LOCK is
opened, and no source file is created or modified. Missing/disappearing files
fail the run. The same `-read-mb-s` physical IO limiter applies to metadata reads.

The join seeks only each requested continuation file's descriptor `[b,c)` and
manifest `[h,i)` ranges in unsampled SSTs. It uses at most 32 metadata readers,
128 cached metric/file results, and 2 MiB/8192 metadata entries per result. Lookup
cost scales with the number of store SSTs and sampled metric/files; inspect read
MiB and elapsed time before expanding a sample. It does not sweep unsampled point
groups. Temperature and newest-observed position still use tail groups from the
data sample, so unsampled tails remain unknown. Physical versions, cross-file
range deletions, active memtables and buffered writer heads are not a live-store
view; cold-native attribution is a candidate estimate, not packing eligibility.

## Production float codec policy

`-policy current,float-codec` compares the paired current rewrite with the
production `EncodeSealedPointsFloat` encoder (64-point prefix selection). It writes native version-4 frames
without the two-byte private research envelope. CPU includes construction of
the complete current fallback. Every changed frame is decoded by the production
reader and compared timestamp/IEEE-word for timestamp/IEEE-word. The usual path,
disk, cleanup, range-tombstone and baseline-agreement guards apply. Outputs
remain offline experiments: do not ingest these standalone rewritten SSTs.

The default transcode policy list is now `current,float-codec`; the previous
`prefix256,minfour,alp,chimp` research controls remain explicitly selectable.
Inventory identifies native v4 `float-alp`, `float-dictionary`, and
`float-chimp128` groups separately.

`float-codec-min` is an additional native exhaustive control over the same
encoders and decoder, for measuring the extra stock saving versus CPU cost.
