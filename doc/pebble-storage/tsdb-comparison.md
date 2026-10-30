# Shared Pebble storage, VictoriaMetrics and GreptimeDB

Mechanisms, semantics and local measurements, 2026-10-30.

The design motivation is a production store with roughly **20 M cached metrics on 192 cores**. That context is separate from the benchmark: identical finite production-derived samples were replayed on one shared 16-core, 48 GiB arm64 machine. Dataset labels `misc`, `db` and `magg` identify the three anonymized exports.

**Current decision: bit-exact storage is no longer a requirement. A decimal value codec is under evaluation.** The measured go-carbon baseline still uses its exact codec. Precision, exceptional values and migration verification remain explicit design choices; the historical exactness explanation below describes the baseline rather than imposing a new acceptance gate.

| Evidence | Scope |
| --- | --- |
| go-carbon source and measured binary | Code baseline `d45860252fd38cb67a29fe2859d791f51840ecaf`; exact continuation v7. Repo-relative code citations describe this baseline, not every experimental follow-up. |
| VictoriaMetrics mechanisms | Source snapshot `05903c8acd9557ef018bcd0a11d648d5d75f6523`; single-node design. |
| VictoriaMetrics measured binary | v1.155.0, release revision `3fec2f7241`, `precisionBits=64`. Its exact release source was unavailable during the offline study; mechanism citations use the snapshot above. |
| GreptimeDB mechanisms | Source snapshot `8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb`, Mito. Its flat/bulk defaults are not assumed to describe every release. |
| GreptimeDB measured binary | v1.3.0, release revision `badceec12a0e98fd69e83e9b9b9b06d9358b78dd`; one ordinary table and one region. |
| Measurements | 24 completed runs: three datasets × four configurations (go-carbon, packing-on go-carbon, VM, GreptimeDB) × full-rate/50,000-points/s replay. Separate metadata and append screens are labelled below. |
| Unverified details | “not verified” means the available code/research or experiment does not establish that fact. Prometheus TSDB is a short reference column because its engine was not examined. |

## At a glance

Full-rate replays of the three datasets (details, paced runs and method in [Measured on identical data](#measured-on-identical-data)). Each cell lists **misc · db · magg**, rows grouped by kind. Read latency cells are warm native-API **p50 / p99 ms**; read CPU is server CPU per query. Response shapes differ, so reads compare serving cost, not isolated engines.

| Angle | go-carbon | VictoriaMetrics | GreptimeDB | Best |
| --- | --- | --- | --- | --- |
| **Storage** | | | | |
| Durable bytes / input point | 2.75 · 2.01 · 1.55 | **1.44 · 0.97 · 1.13** | 2.59 · 1.59 · 2.50 | VM (27–52 % below go-carbon) |
| All files / input point (incl. index, discovery) | 2.79 · 2.07 · **1.61** | **1.86** · 1.82 · 2.17 | 4.98 · **1.79** · 3.53 | VM misc, GreptimeDB db, go-carbon magg |
| **Ingest** | | | | |
| Ingest CPU, µs / input point | 0.955 · 0.873 · 0.562 | **0.603 · 0.537 · 0.512** | 1.646 · 1.675 · 1.667 | VM (9–39 % below go-carbon) |
| Persistence rate, M points/s (incl. settle) | 1.78 · 1.28 · 0.87 | **3.54 · 2.64 · 1.87** | 0.55 · 0.37 · 0.38 | VM |
| **Memory** | | | | |
| Peak / post-drain RSS, MiB | 470/470 · 401/401 · 465/165 | **420/403 · 342/333 · 374/287** | 936/583 · 746/507 · 2708/384 | VM peak; go-carbon magg post-drain |
| **Read latency (p50 / p99 ms)** | | | | |
| 1 × 24 h | **0.39/1.60 · 0.39/0.71 · 2.45/3.05** | 6.01/11.48 · 5.63/6.08 · 6.30/12.60 | 5.51/6.70 · 3.25/4.19 · 18.38/21.39 | go-carbon (2.5–15× faster p50) |
| 100 × 24 h | **10.7/23.7 · 11.0/12.3 · 257/758** | 534/632 · 532/548 · 533/554 | 71.6/76.7 · 41.8/48.9 · 1212/1663 | go-carbon (2–50× faster p50) |
| 1,000 × 1 h | **54.0/105** · 57.3/62.2 · **145**/192 | 231/291 · 236/276 · 155/**168** | 157/164 · **50.5/54.0** · 270/370 | go-carbon misc, GreptimeDB db; magg split |
| **Read CPU** | | | | |
| 100 × 24 h, ms / query | **22.5 · 26.5 · 321** | 579 · 565 · 580 | 269 · 150 · 3,841 | go-carbon |
| 1,000 × 1 h, ms / query | **133** · 192 · 256 | 284 · 288 · **194** | 248 · **177** · 776 | mixed |
| **Correctness and semantics** | | | | |
| Values changed (of sampled) | **0/400 · 0/400 · 0/332** | 16/400 · 24/400 · 0/332 (max relative error 6.5e-13) | **0/400 · 0/400 · 0/332** | go-carbon, GreptimeDB (exact) |
| Coarse retention | Native Whisper archives and rollups, written at ingest | Raw samples only | Raw samples only | Different contracts |

### Where go-carbon's bytes go

Settled SST bytes per input point split by component, from a separate replay of the same data (the split apportions compressed blocks by their uncompressed sections, so components are allocations, not exact causal savings).

| Component | misc | db | magg |
| --- | ---: | ---: | ---: |
| Base archive values | 1.271 | 0.799 | 0.732 |
| Base timestamps and presence bitmaps | 0.416 | 0.259 | 0.518 |
| Group envelope and CRC | 0.047 | 0.039 | 0.050 |
| Keys, framing and index | 0.288 | 0.267 | 0.187 |
| Continuation headers, manifests and catalog | 0.539 | 0.466 | 0.067 |
| Rollup archive values | 0.186 | 0.172 | 0.000 |
| **Total** | **2.748** | **2.003** | **1.553** |
| VictoriaMetrics durable total | 1.436 | 0.967 | 1.134 |

Headers, manifests and catalog are per-series costs: with 6–24 h of history they weigh far more per point than on stores holding months of data per series.

### Closing the storage gap: local prototypes

Neither prototype is enabled by default or merged; both keep native rollups.

| Durable bytes / input point | misc | db | magg |
| --- | ---: | ---: | ---: |
| go-carbon today | 2.75 | 2.01 | 1.55 |
| Cold history repacked into 4,096-point sealed blocks | 2.28 (−17 %) | 1.65 (−17 %) | **1.09 (−30 %)** |
| Decimal value blocks, measured separately | −13 % | −15 % | −4 % |
| VictoriaMetrics | 1.44 | 0.97 | 1.13 |

Repacking applies only to history older than the write window; the short exports here are its full-sealing ceiling. Decimal blocks double value-encoding CPU. Their combined effect is not measured yet.

## Master mechanism table

| Mechanism | go-carbon: shared Pebble / exact baseline | VictoriaMetrics: single node | GreptimeDB: Mito | Prometheus TSDB | Whisper: classic go-carbon |
| --- | --- | --- | --- | --- | --- |
| Series identity & stable IDs | Dotted metric name in catalog; durable uint64 ID, generation and revision. Archive/policy belong to metadata. Hash collisions checked against full names; an FST ordinal is not a durable ID. [`internal/chunkstore/types.go`][C-identity] [`cache/items.go`][C-items] [`carbonserver/metricstore.go`][C-index] | Metric name + tags → TSID: 24 B (metric-group uint64, job/instance uint32 each, unique MetricID uint64). Group hashes provide locality; MetricID supplies identity. [V-tsid][V-tsid] [V-index][V-index] | Region schema + encoded primary key, time index, sequence and op. Arrow batch PK dictionaries use UInt32 indices; these are local dictionary positions, not durable global series IDs. [T-format][T-format] [T-flat][T-flat] [T-bulkpart][T-bulkpart] | not verified | One metric → one .wsp file, archive offsets inside its header; no shared-store ID needed. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |
| Ingestion path & mutable structure | Sharded itemMap → compactPoints in arrival order → deadline queue → persister → indexed Pebble batch / continuation writer. Popped, unconfirmed points remain readable. [`cache/items.go`][C-items] [`cache/compact.go`][C-compact] [`cache/queue.go`][C-queue] [`internal/chunkstore/continuation_codec.go`][C-update] [`cache/cache.go`][C-cache] | AddRows in chunks ≤8,000 → partition rawRowsShards (round-robin CPU shards, ~8 MiB backing arrays) → sorted, decimal-converted compressed in-memory parts. No object/lock per series in rawRows. [V-ingest][V-ingest] [V-raw][V-raw] [V-partition][V-partition] [V-inmem][V-inmem] | Flat/sparse PK → BulkMemtable (ordinary new-region default because flat format defaults true); dense PK → TimeSeriesMemtable unless bulk selected. SeriesMap is `BTreeMap<PK, Arc<RwLock<Series>>>`, builders start at 4 rows, freeze around 512. [T-memselect][T-memselect] [T-config][T-config] [T-series][T-series] [T-bulk][T-bulk] | not verified | Same go-carbon front cache/persister; classic writer aligns points and writes fixed circular slots. CWhisper is a separate compressed file variant, also supported by this tree. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper-update] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-align] |
| Sample bytes in RAM (payload ≠ process RSS) | compactPoints: 1-byte header + optional 1–10 B zigzag delta-of-delta + 0–8 B byte-XOR payload: 1–19 B/point; regular repeats settle at 1 B. 64-bit compactPoints struct: 56 B plus backing capacity; names, items, queues, log and decoded scratch are extra. [`cache/compact.go`][C-compact] [`internal/inputlog/inputlog.go`][C-inputlog] | rawRow = 48 B on ordinary 64-bit layouts: 24 B TSID + 8 B timestamp + 8 B float + precision/alignment. Compressed in-memory parts have variable bytes/point; 48 B is only raw staging. [V-raw][V-raw] [V-inmem][V-inmem] | No verified single B/sample constant: PK dictionaries, Arrow columns, sequence/op and multi-field rows vary. TimeSeriesMemtable also pays per-series tree/key/Arc/lock/builders; bulk parts can hold encoded Parquet bytes. [T-series][T-series] [T-bulkpart][T-bulkpart] | not verified | Front-cache bytes as at left. Classic disk point = 12 B (uint32 seconds + Float64); this is not the RAM cost of a cached point or CWhisper's compressed bytes. [`cache/compact.go`][C-compact] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] |
| 960 s × 10M series × 240 points | 2.4B queued points. Supplied magg write-cache category ≈12 GiB → 5.37 B/queued point if the illustrative population were exact (includes overhead). This is a sizing model, not an identical-data benchmark. [buffer sizing](#buffer-sizing-and-memory-context) | 48 × 2.4B / 2³⁰ = 107.29 GiB raw rows, before indices/scratch. VM does not normally retain rawRows for 960 s. At 35 points/series: 15.65 GiB. [V-raw][V-raw] [buffer sizing](#buffer-sizing-and-memory-context) | Equivalent cardinality, multi-field schema and compressed memory: not verified. No inferred ratio from Parquet or Rust. | not verified | A hypothetical compact raw (ID, int64 timestamp, float) tuple is 24 B →53.64 GiB; classic disk's 12 B/slot is a different representation. |
| Flush cadence & visibility | Reference production cadence: 960 s coalescing; deadline policy has startup/restore/sparse spread. Long batches amortize per-metric read/modify/commit. Cache overlay supplies immediate visibility; this interval is not a WAL sync period. [`cache/queue.go`][C-queue] [`cache/cache.go`][C-cache] [`internal/inputlog/inputlog.go`][C-inputlog] [`internal/chunkstore/store.go`][C-store] | Pending raw rows → searchable parts every 2 s; due in-memory parts → disk default 5 s (min 2 s). Pending rawRows are not searched. Parts are already encoded; final one-part flush may copy streams directly. [V-partition][V-partition] [V-inmem][V-inmem] | Periodic auto-flush default 10 min, plus pressure. Freeze lowers active bytes but total stays charged until owners release. Flush eligible large regions until pressure falls. [T-config][T-config] [T-flush][T-flush] [T-flush-worker][T-flush-worker] [T-allocation][T-allocation] | not verified | Same cache deadlines; writes go to the selected metric file rather than a shared LSM. Fixed-size classic archives avoid LSM memtable flush/compaction. [`cache/queue.go`][C-queue] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper-update] |
| Persistent layout & seek unit | Shared Pebble SST/WAL; catalog + metric ID/generation keys. Continuation manifest, 128 B header pages, block descriptors, exact 128-slot point groups. v7 group keys start with hour bucket + ID/generation; header/descriptor keys stay metric-scoped. [`internal/chunkstore/types.go`][C-identity] [`internal/whisperio/continuation_blocks.go`][C-bucket] [`internal/whisperio/continuation_blocks.go`][C-append] [`doc/pebble-storage/data-layout.md`][C-layout-doc] | Calendar-month partitions; immutable in-memory/small/big parts. timestamps.bin, values.bin, index.bin, metaindex.bin + metadata. Series blocks ≤8,192 rows, sorted (TSID,timestamp); series span blocks. [V-partition][V-partition] [V-part][V-part] [V-header][V-header] [V-block][V-block] | Region SSTs are Parquet, ordered by PK/time with sequence/op fields; default row group 102,400 rows, query batch 8,192. Flat layout can retain tag columns; column projection and statistics prune reads. [T-format][T-format] [T-flat][T-flat] [T-writer][T-writer] [T-parquet][T-parquet] | not verified | One file per metric, header + one fixed circular area per retention. Classic 12 B slots; compressed CWhisper has variable blocks and writer buffers. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] [`vendor/github.com/go-graphite/go-whisper/compress.go`][C-compress] |
| Timestamp codec | Cache: lossless delta-of-delta + zigzag uvarint. Exact disk groups: signed timestamp deltas over present slots + 128-bit presence bitmap, optional Zstd; do not conflate the two codecs. [`cache/compact.go`][C-compact] [`internal/chunkstore/exact.go`][C-exact] | Detect constant arrays/steps, nearest-delta or nearest-delta-of-delta → zigzag/varint → optional Zstd. precisionBits=64 preserves the integer stream. Adjacent equal timestamp streams can share offsets. [V-encoding][V-encoding] [V-nearest][V-nearest] [V-int][V-int] [V-streamwriter][V-streamwriter] | Parquet timestamp/sequence: DELTA_BINARY_PACKED, dictionaries disabled. Event-time memtable partitions are half-open windows (initially one day when unspecified). [T-writer][T-writer] [T-parquet][T-parquet] [T-timeparts][T-timeparts] | not verified | Classic: literal big-endian uint32 seconds in every 12 B slot. CWhisper timestamp compression is separate from classic. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |
| Value codec | Cache: byte-granular Float64 XOR, repeated bits cost no value bytes. Disk: integer deltas only when Float64 reconstruction is bit-identical and within ±2⁵³; otherwise byte-stream-split raw float bits. Zstd only for body ≥128 B and smaller output; CRC32 covers body + interpretation. [`cache/compact.go`][C-compact] [`internal/chunkstore/exact.go`][C-exact] [`internal/chunkstore/exact.go`][C-intgate] | Float→shared decimal scale→integer array codec. Zstd tried at ≥128 intermediate bytes, kept only if output ≤90%; levels 1–5 by rows. Lower precision can drop delta bits. Integer precision=64 does not undo float→decimal conversion. [V-decimal][V-decimal] [V-encoding][V-encoding] [V-int][V-int] | Default Zstd; ordinary fields use PLAIN/default dictionaries unless overridden. Float BYTE_STREAM_SPLIT is optional, disables dictionaries, and is not automatically the default. Op column has compression disabled. [T-writer][T-writer] [T-floatopt][T-floatopt] [T-parquet][T-parquet] | not verified | Classic: literal Float64 bits, no value compression. CWhisper uses its own bitstream; do not substitute its bytes/point for classic or for exact groups. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] [`vendor/github.com/go-graphite/go-whisper/compress.go`][C-compress] |
| Exactness boundary | Bit-exact codec for arbitrary Float64: NaN payload, ±0, subnormal and infinity survive. Integer shortcut has a reconstruction gate; otherwise raw bits take the normal path. Aggregates remain calculations under the existing writer's rules. [`internal/chunkstore/exact.go`][C-exact] [`internal/chunkstore/exact.go`][C-intgate] [`internal/chunkstore/exact_test.go`][C-exact-tests] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] This describes the measured baseline; bit-exactness is no longer a requirement and a decimal codec is under evaluation. | Not an arbitrary Float64 bit oracle: decimal conversion has finite precision, normalizes zero and supports the designated stale NaN; scale calibration may further change values. precisionBits=64 describes integer encoding only. [V-decimal][V-decimal] [V-encoding][V-encoding] | GreptimeDB matched all checked finite values in the replay. Arbitrary NaN-payload, signed-zero and subnormal fidelity is not verified by that sample. [T-floatopt][T-floatopt] [T-writer][T-writer] | not verified | Classic pack/unpack uses Float64bits/frombits. Raw slot round-trip is bit-preserving; missing-slot responses and rollup arithmetic are separate semantics. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] |
| Index & discovery | Catalog is authoritative. trieIndex + nonempty file-list cache + FLC v2 selects mapped immutable FST + metadata + mutable overlay. Else trie/details or trigram route. Whole-key Pebble blooms help probes, not prefix enumeration. [`carbonserver/metricstore.go`][C-index] [`carbonserver/snapshot_fst.go`][C-fst] [`carbonserver/metricstore_mapped.go`][C-mapped] [`doc/pebble-storage/write-path.md`][C-write-doc] | Partition IndexDB on mergeset: name→TSID, ID→TSID/name, tag postings and per-day membership. Prefix-coded sorted byte items + Zstd; postings combined on merge. Graphite suffix/path lookup exists; exact go-carbon glob parity is not established. [V-index][V-index] [V-mergeset-codec][V-mergeset-codec] [V-graphite][V-graphite] | Optional inverted value→row-segment indices in Puffin blobs; external sort with memory/spill controls. Applier prunes row groups. Bloom targets have separate memory/spill accounting. Not a ready Graphite glob implementation. [T-inverted][T-inverted] [T-invertedread][T-invertedread] [T-bloom][T-bloom] | not verified | go-carbon trie/trigram/file-list discovery over metric paths; .wsp itself contains samples and policy, not a global name index. [`carbonserver/metricstore.go`][C-index] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |
| Caches & byte budgets | Separate knobs for write cache, bounded striped state slabs, optional tail slots, Pebble block cache/memtables/table readers, query caches and conversion work. No single hard process budget. Prewarm already exists; pinned readers and spare capacity still cost RSS. [`internal/chunkstore/state_cache.go`][C-state] [`internal/chunkstore/state_slab.go`][C-slab] [`internal/chunkstore/state_slots.go`][C-slots] [`internal/chunkstore/store.go`][C-store] [`carbon/metadata_prewarm.go`][C-prewarm] | memory.Allowed defaults 60% detected/cgroup RAM (absolute override). tsidCache 37%; ID cache 1/16; name cache 1/10; data index 10%; mergeset index/normal/sparse 10/25/5% of Allowed. Separate capacities are not an enforceable sum/RSS cap. [V-memory][V-memory] [V-memlinux][V-memlinux] [V-storage-caches][V-storage-caches] [V-part][V-part] [V-mergeset-part][V-mergeset-part] | Write buffer min(RAM/8,1 GiB); metadata min(RAM/8,512 MiB) split ½ compact/½ decoded. Page ranges RAM/8; vector/selector/range each min(RAM/16,512 MiB); scan 25%. Compaction configurable but unlimited by default. Extra caches remain separate. [T-config][T-config] [T-cache][T-cache] [T-cache-size][T-cache-size] [T-task][T-task] | not verified | Same application cache knobs; kernel page cache holds file data. No shared Pebble block-cache/memtable budget in classic mode. [`cache/cache.go`][C-cache] [`doc/pebble-storage/write-path.md`][C-write-doc] |
| Admission, reclamation & cache generations | stateCache already Snappy-packs keys ≥256 B when savings exceed about 12.5%; pointer-free arena records + one retained decoded last-state per stripe. Epoch/pending rules preserve snapshots. Eviction is not RSS release; count decoded and reader-pinned bytes too. [`internal/chunkstore/state_cache.go`][C-state] [`internal/chunkstore/state_slab.go`][C-slab] [`internal/chunkstore/state_slots.go`][C-slots] [`internal/chunkstore/state_cache.go`][C-state-pack] [`internal/chunkstore/state_slab.go`][C-snappy] | Working-set current/previous caches promote hits and rotate ~30 min; whole-cache transition can overlap allocations. fastcache 512 buckets, 64 KiB chunks, small cache rounded to 32 MiB; 64 MiB mmap extents can stay pooled after logical eviction. [V-wsc][V-wsc] [V-fastcache][V-fastcache] [V-fastalloc][V-fastalloc] | WriteBufferManager tracks active + total. Flush at active≥½ global or total≥global; stall at global. Global rejection threshold 2×. Optional region override: flush½/stall1×/reject2× incl stalled requests. Memory reservations follow allocation lifetimes. [T-flush][T-flush] [T-allocation][T-allocation] [T-write-worker][T-write-worker] [T-config][T-config] [T-flush-worker][T-flush-worker] | not verified | Front-cache and input-log lifetimes still matter. Closing a metric file does not immediately evict the kernel's cached file pages. Exact physical RSS effects: not verified. |
| Retention & deletion unit | Per-archive circular ownership plus per-metric conditional deletion (ID/generation/revision checked). Expiration batches ≤512 candidates. v7 buckets use block-start write clock, sealed history bucket 0: locality only, not safely TTL-droppable windows. [`internal/chunkstore/admin.go`][C-delete] [`internal/chunkstore/expiration.go`][C-expire] [`internal/whisperio/continuation_blocks.go`][C-bucket] [`internal/whisperio/continuation_blocks.go`][C-append] | Retention watcher ~1 min detaches expired calendar-month partitions; references defer physical Drop. Merges remove expired blocks/rows in partial months. IndexDB participates in partition lifecycle. [V-retention][V-retention] [V-merge][V-merge] | TWCS finds expired SSTs and groups remaining files by max event timestamp. TTL can retire a file only when its newest event satisfies expiry; version/read references defer physical purge. Late data can enter old windows. [T-twcs][T-twcs] [T-picker][T-picker] [T-sstversion][T-sstversion] [T-timeparts][T-timeparts] | not verified | Fixed ring archives overwrite slots on reuse; timestamp validates ownership. File deletion retires a metric. Query clips to now−maxRetention and chooses an archive covering requested age. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-align] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |
| Downsampling / rollups | Write-time schema archives: average/sum/last/max/min/first, XFF and buffered rollups through the same go-whisper state machine. Averages are not arbitrary repeated means; preserve source grid and completeness behavior. [`internal/whisperio/continuation_codec.go`][C-writer] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] | General age-based downsampling not found in inspected public storage. ScheduledDownsampling metrics here count dedup scheduling. Enterprise feature behavior: not verified; interval dedup is not aggregation. [V-partition][V-partition] [V-retention][V-retention] [V-dedup][V-dedup] | SQL reads and aggregation can be expressed by consumers; the replay uses ordered arrays per series. Scheduled age-based materialization and an equivalent Graphite XFF/correction policy are not verified. TWCS compaction itself is not a rollup. [T-twcs][T-twcs] | not verified | Schema-defined coarser ring archives populated on writes; XFF gates propagation. No LSM background downsampling needed for classic files. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] |
| Compaction & write amplification | Hot partial groups and writer state are rewritten before Pebble flush/L0→levels compaction. Archive-chunk API separately emits merge operands, materializing at 4 deltas; merger rejects ≥32. Aggregate compaction-output pacer + adaptive debt/stall escape paths already exist. No universal WA ratio. [`internal/whisperio/continuation_blocks.go`][C-append] [`internal/whisperio/continuation_blocks.go`][C-group] [`internal/chunkstore/access.go`][C-merge] [`internal/chunkstore/chunk.go`][C-merger] [`internal/chunkstore/compaction_pacer.go`][C-pacer] [`internal/chunkstore/compaction_controller.go`][C-controller] | Balanced size/locality candidates (default 15 parts, minimum multiplier 1.7), bounded output/free-space checks; encoded blocks can pass through if compatible. Separate worker pools; no merge-output bytes/s token bucket found. Actual WA needs matched measurement. [V-partition][V-partition] [V-merge][V-merge] [V-streamwriter][V-streamwriter] [V-filestream][V-filestream] | TWCS has active/inactive policies, PK overlap/run selection, input/output limits; ordinary candidate default ≤16 inputs. Avoids repeated L1 history rewrites beside L0. Configured memory guard can limit compaction scratch. WA not quantified. [T-twcs][T-twcs] [T-twcs-options][T-twcs-options] [T-task][T-task] | not verified | Classic slot overwrites + write-time lower archives; no LSM compaction. Compressed CWhisper may fold OOO/rewrite archives, so its amplification is not classic's. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] [`vendor/github.com/go-graphite/go-whisper/compress.go`][C-compress] |
| Durability · WAL & fsync policy | Two layers: inputlog records accepted cache points, group-commits fdatasync (default 1 s; loss includes encode/write cycle); Pebble mutations Sync at interval=0, NoSync + background sync for positive interval. Async WAL rotation avoids old-file fsync in ordinary commit critical path; one handoff maximum, sync/flush/close wait. Expiration batches deliberately NoSync. [`internal/inputlog/inputlog.go`][C-inputlog] [`internal/chunkstore/store.go`][C-store] [`internal/chunkstore/wal_sync.go`][C-wal] [`internal/chunkstore/expiration.go`][C-expire] [`internal/chunkstore/store.go`][C-wal-rotation] [`vendor/github.com/cockroachdb/pebble/options.go`][C-wal-options] | In-memory→disk scheduling default 5 s is verified. WAL presence/absence, acknowledgement guarantee, fsync cadence and crash-loss bound are not verified in this report. A flush timer alone is not a durability contract. [V-partition][V-partition] [V-inmem][V-inmem] | Measured v1.3.0 uses its native raft-engine WAL; the experiment sets 16 MiB segments, 32 MiB purge threshold, 1 s purge interval and no recycling. Equal acknowledgement/fsync/crash-loss behavior across engines is not established. [GT-release-config][GT-release-config] | not verified | Shared go-carbon inputlog can protect the front cache when configured. Classic pack/write is verified; full file fsync/ack policy is not verified here. Do not infer synchronous durability from WriteAt. [`internal/inputlog/inputlog.go`][C-inputlog] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] |
| Query path & read amplification | Find expands index; render uses membership→epoch/cache + consistent ReadView→catalog/revision on miss→cached/LSM manifest/header→descriptors/groups→cache overlay. ReadView avoids registered Snapshot's DB-mutex cost. Incremental mode can fall back to original files. L0 overlap, cold-cache/table-reader churn and persister reads increase work. [`carbonserver/fetchfromdisk.go`][C-read] [`internal/chunkstore/fetch.go`][C-fetch-view] [`internal/chunkstore/continuation_io.go`][C-io] [`carbonserver/metricstore.go`][C-index] | IndexDB postings→matching TSIDs→part metaindex/index→timestamp/value blocks→merge/dedup. Decoded part metaindices remain resident. Partition/part/block counts and cache misses determine fan-out; rawRows await conversion for search visibility. [V-index][V-index] [V-part][V-part] [V-header][V-header] [V-dedup][V-dedup] [V-partition][V-partition] | Read version + PK/time predicates + SST min/max/inverted/bloom pruning → projected Parquet pages/row groups → version-aware merge. 102,400-row default groups need not mean every query decodes all rows; sparse 35-point lookups still require measurement. [T-format][T-format] [T-writer][T-writer] [T-invertedread][T-invertedread] [T-cache][T-cache] [T-sstversion][T-sstversion] | not verified | Find metric paths; select one retention; read circular file range, validate slot timestamps, merge unpersisted cache points. Many metrics imply many file/metadata lookups. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`carbonserver/fetchfromdisk.go`][C-read] [`carbonserver/metricstore.go`][C-index] |
| Concurrency & contention candidates | Receive shard lock, per-metric/striped writer locks, state-cache stripes, Pebble commit pipeline/table cache and memtable range-tombstone fragmentation. Fetch already uses ReadView to avoid Snapshot DB-mutex contention. Indexed-batch prefix deletes prefer point deletes to reduce reader serialization. Profiles needed to rank remaining costs. [`cache/queue.go`][C-queue] [`internal/chunkstore/continuation_codec.go`][C-update] [`internal/chunkstore/fetch.go`][C-fetch-view] [`internal/chunkstore/continuation_io.go`][C-delete-prefix] [`doc/pebble-storage/read-path.md`][C-read-doc] | CPU-sharded row staging; global in-memory conversion/merge channel = CPUs; separate small/big merge channels max(4,CPUs). No per-series rawRows mutex. Index/cache and merge competition still exist; comparative lock profile not verified. [V-raw][V-raw] [V-partition][V-partition] [V-index][V-index] | Dense time-series path has Series Arc/RwLock + map; workers dispatch writes/flush; alloc tracking, sorter limits and memory-manager permits control work. Bulk path differs. A measured lock-contention ranking is not verified. [T-series][T-series] [T-write-worker][T-write-worker] [T-flush-worker][T-flush-worker] [T-memory][T-memory] [T-inverted][T-inverted] | not verified | Same go-carbon receive/writer coordination; many independent files. Actual locking cost vs the shared-store baseline is not verified. [`cache/queue.go`][C-queue] [`doc/pebble-storage/read-path.md`][C-read-doc] |

### Buffer sizing and memory context

These are cost models and supplied memory categories, not measurements of equivalent engine populations. Let **N** be catalog size and **A** simultaneously queued series; they need not be equal.

| Quantity | Calculation / supplied value | Interpretation |
| --- | --- | --- |
| 960 s × 10 M series × 240 points | 2.4 billion queued points; one sample every 4 s | Illustrative active population. A 960 s window is not a sample count. |
| Raw-row memory model | 2.4 billion × 48 B = **107.29 GiB** | VM's raw staging representation, before indices/scratch; VM normally converts it within seconds. |
| Existing compact write-cache category | Approximately **12 GiB** for the corresponding supplied workload category | 5.37 B/queued point if the illustrative population were exact; includes overhead and is not a measured codec rate. |
| Compact payload limits | Regular repeats approach 1 B/point; worst-case bound 19 B/point | 2.24–42.47 GiB for 2.4 billion points, excluding 56 B buffers, names/items, unused capacity, queues, log entries and in-flight decoding. |
| 35 points × 10 M series | 15.65 GiB at 48 B/raw row; supplied compact-cache category approximately 9 GiB | The active count is illustrative, not a measured cardinality for the short-batch workload. |
| Representative supplied RSS categories, misc / db / magg | 399 / 242 / 226 GiB | Different production populations, not the small replay fixtures. |
| Metadata category, misc / db / magg | 72 / 26 / 19 GiB | Charged records, physical arenas and RSS are different quantities. |
| Pebble block-cache category, misc / db / magg | 51 / 83 / 85 GiB | Pure-Go manual allocation uses byte slices; a heap total may already include this memory. |
| Per-series arithmetic | Saving 100 B at A=10 M gives 0.93 GiB; 1 KiB at N=10 M gives 9.54 GiB | A small handle optimization cannot explain a multi-hundred-GiB process. |

GiB uses 2³⁰ bytes. The catalog-membership filter is capped at 64 MiB and is not a plausible explanation for a very large residual heap. Do not add overlapping heap/cache categories or projected savings. Mapped files, retired reader-pinned arenas, stacks, unused allocator capacity and query scratch still contribute to resident memory. [G-manual][G-manual] [G-membership][G-membership]

## Semantics differences

| Behavior | go-carbon: exact baseline | VictoriaMetrics | GreptimeDB | Prometheus TSDB | Whisper: classic |
| --- | --- | --- | --- | --- | --- |
| Duplicate / equal timestamp | Graphite correction: last write wins for a slot (not max value). alignPoints keeps the last value in an interval; stable ordering preserves equal-time arrival order. Continuation patch/sidecar rules preserve writer behavior. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-align] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper-update] [`internal/whisperio/continuation_codec.go`][C-writer] | Dedup selects last timestamp in interval; exactly equal timestamps select maximum value, with stale-NaN handling. This describes dedup/merge behavior, not an assumed immediate rawRows rewrite. [V-dedup][V-dedup] [V-merge][V-merge] | Primary-key/time, sequence and operation take part in version-aware merging. Last-row versus LastNonNull mode defaults and equal-key winner details are not verified by this duplicate-free replay. [T-format][T-format] [T-sstversion][T-sstversion] [T-picker][T-picker] | not verified | Last value for an aligned interval on classic writes. [10 then 3] at the same timestamp yields 3; VM dedup's ordinary finite-value result is 10. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-align][V-dedup][V-dedup] |
| Out-of-order & late corrections | Arrival order retained in cache, writer sorts/alignment; compressed continuation supports virtual OOO sidecars and maintenance. DirectLateWrites can patch/fold in place; exact path depends on options. uint32 timestamp range enforced; older points follow archive/now rules. [`cache/compact.go`][C-compact] [`internal/whisperio/continuation_codec.go`][C-writer] [`internal/whisperio/continuation_blocks.go`][C-append] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper-update] | Rows sorted into parts; overlapping/extendable blocks take merge slow path. Retention filtering applies. Exact ingest late/future admission limits are not verified in the report. [V-raw][V-raw] [V-merge][V-merge] [V-retention][V-retention] | Event-time partitions accept late data into older windows; sequence/op reconcile versions. Exact configured late/future admission/rejection limits are not verified. [T-timeparts][T-timeparts] [T-twcs][T-twcs] [T-format][T-format] | not verified | Classic writes aligned slots directly within age/retention selection. Compressed CWhisper has separate OOO machinery; these are not identical write costs. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper-update] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-align] [`vendor/github.com/go-graphite/go-whisper/compress.go`][C-compress] |
| Missing value vs NaN | Presence bitmap distinguishes absent slot from stored Float64 NaN at codec layer; Graphite fetch initializes missing values to NaN, so API output may not expose that distinction. No invented zero-fill. [`internal/chunkstore/exact.go`][C-exact] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`internal/chunkstore/access.go`][C-merge] | Decimal conversion supports designated stale NaN, not arbitrary NaN payload fidelity. General missing/ordinary-NaN query semantics are not verified here. [V-decimal][V-decimal] [V-dedup][V-dedup] | Arrow/Parquet field structures are described; SQL NULL vs NaN and how each aggregation treats them are not verified. [T-format][T-format] [T-flat][T-flat] | not verified | Timestamp mismatch means empty slot; Fetch initializes gaps to NaN. Stored raw float bits still permit NaN values. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] |
| Rollup aggregation | Writer's average/sum/last/max/min/first over known grid positions; buffered compressed rollups remain visible and must survive conversion. Corrections must propagate to coarser archives atomically with state. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] [`internal/whisperio/continuation_codec.go`][C-writer] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] | Dedup chooses a representative sample; it is not sum/average or Graphite rollup propagation. General age-based downsampling implementation not verified. [V-dedup][V-dedup] [V-retention][V-retention] | Consumers can issue SQL aggregation queries; persistent window scheduling and Graphite-equivalent correction propagation/completeness remain not verified. | not verified | Write-time lower-archive aggregation using configured method. Do not replace sum by interval dedup or average by unweighted averages of averages. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] |
| xFilesFactor (XFF) | Known source slots / expected slots must meet XFF before propagation; lower-archive grid and method remain writer-owned. Example: one of four known, XFF=.5 → no rollup. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] [`internal/whisperio/continuation_codec.go`][C-writer] | Graphite XFF compatibility is not verified; dedup count is not an XFF completeness check. | Equivalent SQL completeness rule and missing-grid denominator are not verified; must be specified before claiming parity. | not verified | Same propagation test in classic archives. NaN arithmetic is separate from whether a slot timestamp counts as known. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] |
| Staleness markers | Exact codec preserves all float bit patterns; Graphite missing-slot NaN is not a separately verified Prometheus staleness state machine. No special stale-marker interpretation verified in this writer. [`internal/chunkstore/exact.go`][C-exact] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] | Designated stale NaN handled specially in decimal conversion and dedup. Full query staleness/lookback behavior is not verified in the report. [V-decimal][V-decimal] [V-dedup][V-dedup] | Protocol mapping of stale NaN, SQL NULL and deleted row: not verified. | not verified | Raw bits can retain NaN; fetch represents gaps as NaN. Special stale-marker interpretation is not verified. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] |
| Name / schema model | Dotted Graphite names → catalog ID + schema retentions/aggregation/XFF; discovery preserves glob/directory/leaf behavior. Labels are not substituted for name identity. [`internal/chunkstore/types.go`][C-identity] [`carbonserver/metricstore.go`][C-index] | Metric names + tag postings; Graphite path lookup also implemented. Equal syntax and missing-data discovery semantics with go-carbon are not verified. [V-index][V-index] [V-graphite][V-graphite] | Region/table columns, encoded PK/tag columns, timestamp and fields. Mapping dotted names to SQL tags/PK is a schema choice, not an automatic equivalent. [T-format][T-format] [T-flat][T-flat] [T-series][T-series] | not verified | Dotted paths → .wsp paths; policy per file; application index expands globs. [`carbonserver/metricstore.go`][C-index] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |
| Retention boundary & deletion visibility | Whisper Fetch clips to [now−maxRetention,now], selects resolution covering requested age; exact endpoint alignment follows writer. Metric expiration checks revision before delete. v7 hour buckets cannot be dropped solely by wall-clock age. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`internal/chunkstore/expiration.go`][C-expire] [`internal/whisperio/continuation_blocks.go`][C-bucket] | Whole-month detach plus row filtering inside partial months; reader refs defer physical removal. Exact query endpoint and retention cutoff inclusivity are not verified here. [V-retention][V-retention] [V-merge][V-merge] | Half-open event-time windows; expired SST only if newest event passes TTL; version refs defer physical purge. Exact TTL equality boundary and query cutoff semantics are not verified. [T-timeparts][T-timeparts] [T-picker][T-picker] [T-sstversion][T-sstversion] | not verified | Circular reuse validated by stored timestamp; fetch age decides archive and resolution. Not a SQL BETWEEN boundary contract. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |

## Exactness: signed zero, subnormals and NaN payloads

The baseline exact codec compares `(timestamp, Float64bits(value))`. This supported byte-for-byte value verification during conversion and replication, including cases ordinary numeric equality misses. It does not mean physical SST files are byte-identical after compaction. The current design decision permits a different numeric contract; decimal encoding is being evaluated rather than already selected.

| Pattern | Plain explanation | Why the baseline preserves it / evaluation implication |
| --- | --- | --- |
| NaN payload | NaN means “not a number”; many different 64-bit patterns encode it. Payload bits distinguish those patterns. NaN does not equal itself. | Normalizing every NaN would change stored bits and confuse an exact migration oracle. A decimal codec must specify accepted NaNs and staleness behavior, even if payload fidelity is no longer required. |
| Signed zero | +0 and −0 compare equal numerically, but differ in their sign bit and can behave differently under operations such as reciprocal. | −0 fails the baseline integer reconstruction gate and follows the raw-bit path. An approximate codec must explicitly choose whether it preserves or normalizes it. |
| Subnormal | Very small finite values near zero use a different exponent/significand convention; they are not missing samples. | Flushing them to zero changes a value. Precision/error policy should cover them separately from ordinary gauges. |
| Infinity and other finite values | Float64 represents ±infinity and values that cannot fit the exact integer shortcut. | The baseline fallback retains those bits. The replay below excludes non-finite input, so it cannot establish exceptional-value fidelity for the other systems. |
| “Zero extra representation cost” | XOR and byte-stream splitting already process all 64 bits uniformly. | No exceptional-value side table or separate record is needed in the current codec. Compression ratio can vary; this does not claim zero encoding CPU or that decimal encoding cannot be smaller. |

The integer shortcut uses signed deltas only after exact Float64 reconstruction succeeds within ±2⁵³; other values use raw byte planes, optionally Zstd-compressed. [Exact codec][C-exact] [Integer gate][C-intgate] [Value-domain tests][C-exact-tests]

VM first converts floats to a shared decimal scale. `precisionBits=64` preserves its integer stream, not arbitrary input Float64 bits. The replay confirms small finite-value changes even through a raw-sample export, so the difference is not confined to renderer formatting. GreptimeDB matched the sampled finite values; this does not establish all NaN/signed-zero/subnormal behavior. [VM decimal conversion][V-decimal] [VM integer codec][V-encoding]

Bit preservation, last-write-wins, XFF, missing data and staleness are independent contracts. Relaxing numeric exactness does not automatically resolve the other semantic differences.

## Append instead of rewrite

### Current update path

```text
compact write cache
  → metric lock / route / load committed writer state
  → indexed batch: tail descriptor + predecessor group read/decode
  → align/merge input, corrections and write-time rollups
  → encode touched groups and dirty header pages
  → atomic payload + revision/activity commit
  → confirm cache batch
```

The continuation writer extends a logical archive by rewriting small materialized groups. It checks the predecessor count/time, reuses one predecessor decode for a partial tail, writes new complete groups without reading their old payload, and persists writer state and rollups. Metadata/tail-cache hits can avoid LSM IO but still leave decoding/encoding work. [Continuation update][C-update] [Writer state machine][C-writer] [Append groups][C-append]

The archive-chunk API is a separate existing path: `chunkWriter.commit` emits `Batch.Merge` operands and periodically materializes with `Set`. The continuation path uses `Set(EncodePointBlock(...))`; the existence of the exact merger does not make those appends read-free. [Archive-chunk commit][C-merge] [Merger][C-merger] [Continuation Set][C-group]

| Supplied production stage | Worker-s/s | Share / interpretation |
| --- | ---: | --- |
| magg: total | 5.7 | Busy-worker occupancy, not CPU consumption. |
| magg: tail_read | 2.1 | 36.8%; storage lookup/decode path and waits, candidate append saving. |
| magg: commit | 1.6 | 28.1%; append records still require commits. |
| magg: decode | 0.8 | 14.0%; includes writer merge/rollup logic, not only decompression. |
| magg: encode | 0.5 | 8.8%; appended payloads still need encoding. |
| magg: route/load/lock | 0.6 | 10.5%; identity/state/coordination remain. |
| magg: unassigned/rounding | 0.1 | 1.8%; named stages sum to 5.6. |
| misc: total / commit | 14.5 / 12.0 | Commit is 82.8%; all other work is 2.5. |
| misc: updates and points | 30,000 updates/s × 33 points | 990,000 points/s; mean total/commit worker time ≈483/400 µs per update. |

Stage counters sum worker wall time, including IO and locks. Total is recorded every update; other stages are sampled 1/8 and scaled. These production observations are separate from the short local engine replay. [Stage definitions][C-timings]

| Zero-replacement-cost counterfactual | Arithmetic | Limit |
| --- | --- | --- |
| Remove all magg tail_read | 5.7 / (5.7 − 2.1) = **1.58×** | Ideal worker-capacity ceiling, not measured CPU/throughput. |
| Remove all magg tail_read + decode + encode | 5.7 / (5.7 − 2.1 − 0.8 − 0.5) = **2.48×** | Unrealistic as a design promise: append and rollups still process samples. |
| Remove every misc non-commit stage | 14.5 / 12 = **1.21×** | At most 17.2% occupancy reduction if commit is unchanged. |
| Halve misc commit | 14.5 / (14.5 − 6) = **1.71×** | Hypothetical; commit includes WAL/memtable work, waits and synchronization. |

### Gains and costs of append/time windows

| Boundary | Current continuation layout | Append / event-time-window design | Gain and cost |
| --- | --- | --- | --- |
| Writeout | Read/merge/re-encode a partial tail plus writer state; atomic revision update | Immutable independently encoded runs keyed by series/archive/time and correction order | Avoid predecessor reads solely to append; add run keys/descriptors, ordering and later merge work. |
| Merge operands | Archive-chunk deltas already defer some combination; continuation writes materialized groups | Bounded operands or runs with explicit folding limits | Shifts work to reads/compaction; never makes it disappear. |
| Rollups | Write-time CWhisper state, buffered lower archives, circular ownership and XFF | Preserve stateful materialization or verify a separate materializer | Appending only raw input changes correction/read work and cannot silently substitute interval dedup for aggregation. |
| OOO/corrections | Tail rewrite, direct patch or virtual sidecar and maintenance | Immutable correction runs with deterministic last-write-wins; fold after a bounded horizon | Cheaper foreground corrections can increase uncached-read and background work. |
| Retention | v7 hour bucket comes from block-start write clock; imported history uses bucket 0 | Event-time + archive-policy + expiry cohort with a lifecycle proof | Potentially retire one closed cohort instead of many metric ranges; current v7 buckets are not safe TTL windows. |
| Queries | Small seekable 128-slot groups, consistent ReadView and live-cache overlay | Metric→run index, time pruning and ordered merge | More recent-run fan-out, decode/allocation and cold-tail cost; gate sparse reads and concurrent p99. |
| Engine topology | One Pebble DB, shared budget and atomic cross-archive batch | Start with cohort prefixes; a DB per window needs additional lifecycle/transaction design | Separate DBs multiply WALs, minimum memtables, readers/caches and cross-window publication cost. |
| IO | Application rewrites precede LSM flush/compaction; long batches already amortize updates | Seal runs, pass through compatible encoded blocks, compact locally, expire cohorts | Measure WAL, flush, compaction, obsolete bytes and debt recovery separately. More frequent flushes can undo coalescing. |

**Measured bounded prototype:** folding every eight append calls (`fold8`) reduced foreground CPU **56–68%** and logical WAL bytes **67–72%** in isolated 30/225-point update screens. Uncached reads became more expensive because they replay pending updates and reconstruct writer/rollup state. These are prototype results, not the engine comparison below or a production rollout. The end-to-end append comparison is still running; background fold debt, activity/no-op metadata, format/transfer compatibility and concurrent read costs remain gates.

**Recommendation:** prioritize true expiry cohorts/windows; retain bounded append/fold as an experiment rather than adopting an unbounded per-metric append log. Sample expiry and catalog garbage collection are different lifecycles. A closed window may be removed only when all archive policies, late corrections and reader/writer references permit it.

## What we borrow

Statuses combine the completed replay with separate local representation/prototype screens. “Dropped” means no retained implementation gain; projections from rejected screens are not savings. Follow-up work is separate from the measured baseline above.

| Idea | Source system / mechanism | Current state | Measured result or expected gain | Risk | Status |
| --- | --- | --- | --- | --- | --- |
| Compact metadata and bounded decoded acceleration | VM serialized working set; Greptime compact/decoded metadata [V-wsc][V-wsc] [T-cache][T-cache] | Immutable pointer-free slabs, selective per-record Snappy packing, one decoded last-state per stripe, optional tail slots | **33–50% fewer charged metadata bytes**; small/inconsistent RSS change. Detailed paired results below. | Decode CPU, slab occupancy, pinned readers and epoch correctness; do not multiply charged saving into RSS. | **Measured**; qualify at realistic capacity and useful hit rate. |
| Shared name arena / stable handles | VM durable identity + cache separation; Greptime local dictionary indices [V-tsid][V-tsid] [T-bulkpart][T-bulkpart] | Existing item/log references already share some name storage | Only **≈90 B/metric** saved (86.5–93.1 B); reinsert CPU **+18–29%**, with added ownership/reclamation work still omitted. | Reinsert cost, ABA/log leases, full-name collision checks, reader lifetime | **Dropped** for CPU cost. |
| Name compression | Prefix-coded names / shared byte dictionaries [V-mergeset-codec][V-mergeset-codec] | Existing mapped discovery compresses names; additional active-name compression is being evaluated | No validated byte/CPU gain yet | Resolution and decompression cost, replacement/churn, names pinned across components | **Being evaluated**. |
| Cold compressed metadata buckets | Further serialization/compression over already packed records [T-cache][T-cache] | Screened whole 64 KiB buckets of existing records | Lookup + first name-key read **0.615 → 17.331 µs**, **28.2× slower**; extra allocation and decode bytes | Random cold hits pay whole-bucket decode and reader-safe copy cost | **Dropped**. |
| Remove duplicate discovery index | Existing FST + bounded mutable state [C-index][C-index] [C-fst][C-fst] | Reviewed profiles use the mapped route; no second bulk trie/trigram/details map identified | **No duplicate bulk index found; no saving claimed.** Mapped residency and small auxiliaries still count. | Removing semantically different overlay/query state could break discovery or increase CPU. | **No change**. |
| Shared memory-budget policy | VM Allowed; Greptime active/total accounting and memory permits [V-memory][V-memory] [T-flush][T-flush] [T-memory][T-memory] | Follow-up retains a startup policy that derives omitted settings; explicit component overrides remain authoritative | No default saving; bounds policy, not a hard RSS limit. Pressure CPU/p99 unqualified. | Double accounting, hidden pinned bytes, excess flushes/cache misses and admission deadlock | **Kept opt-in**, default off. |
| Bounded append/fold | VM immutable parts and encoded pass-through [V-partition][V-partition] [V-merge][V-merge] | Test-only `fold8` prototype, separate from ordinary continuation `Set` | Foreground CPU **−56–68%**, logical WAL **−67–72%**; costlier uncached reads | Fold backlog, read replay/allocation, correction ordering, writer-state and format compatibility | **End-to-end comparison running**; isolated screen only. |
| True sample-expiry windows | VM partition retirement; Greptime event-time/TWCS TTL [V-retention][V-retention] [T-twcs][T-twcs] | Existing v7 write-hour locality is not expiry ownership | Could replace many per-metric tombstones with safe cohort retirement; no production-wide speedup quantified | Different archive retentions, late data, snapshot/reference lifetime and catalog GC | **Recommended** for next design work. |
| Decimal values / size-aware sealed blocks | VM decimal-scaled integers and adaptive blocks; Greptime field encodings [V-decimal][V-decimal] [V-encoding][V-encoding] [T-floatopt][T-floatopt] | Exact integer/BSS + selective Zstd already exists; decimal alternative under evaluation | VM's measured byte/CPU advantage motivates the experiment; no go-carbon decimal gain claimed yet | Define error and exceptional-value policy, migration/compatibility, cold reads and mutable-tail cost | **Being evaluated**; bit-exactness no longer mandatory. |

Historical Greptime partition-tree dictionaries/PkId (u32 shard + u16 local index) informed the arena idea, but were removed on 2026-05-15 and are not the current default. Current flat/sparse mode selects bulk memtables. [H-tree][H-tree] [H-dict][H-dict] [H-data][H-data] [T-memselect][T-memselect]

## Measured on identical data

### Inputs and method

| Dataset | Raw lines | Finite replay points | Distinct finite paths | Export span | Excluded non-finite |
| --- | ---: | ---: | ---: | ---: | ---: |
| misc | 13,224,474 | 13,224,445 | 11,475 | 24.00 h | 29 |
| db | 6,350,894 | 6,350,894 | 6,886 | 24.00 h | 0 |
| magg | 4,391,985 | 4,391,985 | 2,473 | 6.00 h | 0 |

Original timestamps, gaps, names and finite values were retained. The 29 non-finite misc lines were excluded identically from all systems. The finite input contains no duplicate/conflicting `(path,timestamp)` pairs. Thus the replay checks neither exceptional floats nor duplicate/OOO policy exhaustively.

| Measurement choice | Configuration / consequence |
| --- | --- |
| Hardware/runtime | Shared 16-core, 48 GiB arm64 machine, macOS 26.7.1. go-carbon/VM use `GOMAXPROCS=8`; Greptime main pools use eight workers and two compaction workers. These are runtime budgets, not a common OS CPU quota. |
| go-carbon | Exact continuation v7; production schemas/aggregation; 256 MiB Pebble cache including memtable reservations, 32 MiB memtables, 128 MiB unflushed limit, 128 MiB metadata budget, eight persisters/two compactors. Packing off/on variants. |
| Write coalescing | 256 points or 30 s; drain after input, stop, offline flush/compact, reopen for reads. **Different from 960 s production coalescing.** Compressing 6/24 h into minutes changes points/update and metadata amortization. |
| VM | v1.155.0; Graphite TCP, 512 MiB cache allowance, historical-data retention of 100 years, `precisionBits=64`, query cache off, Graphite storage step 1 s. Force-flush and force-merge settle before reads. |
| GreptimeDB | v1.3.0; ordered 16,384-point InfluxDB HTTP batches, one Mito table keyed by metric path, millisecond time index and double values. Write buffer 256 MiB; metadata/vector 64 MiB each; page cache 128 MiB; prefilter 64 MiB; index metadata/content 32/64 MiB; selector/range/index-result caches off. Flush/compact and exact COUNT(*) precede reads. |
| Durability | go-carbon 1 s WAL sync with input log disabled; Greptime native raft-engine WAL; VM persisted parts. Final persistence barriers settle disk measurements, but equal crash-loss/acknowledgement guarantees were not established. |
| Read protocol | Canonical half-open windows and identical selected names; carbonserver pickle, VM Graphite JSON at 1 s step, Greptime SQL paired ordered arrays. Native response shapes and gap-fill differ. |
| Read timing | Warmup + 100 serial warm requests, fresh HTTP connections, warm OS/storage caches. CPU excludes clients; 0.01 s server CPU counter resolution gives ≈0.1 ms/query quantization. Shared-machine load makes small differences and empirical p99 uncertain. |
| Persistence throughput | Input-to-settle completion including waits; go-carbon includes offline flush/compaction. Paced cells include replay delay. **Observed completion rate, not sustainable maximum throughput.** |
| Ingest CPU | Server user+sys through settle plus go-carbon offline flush/compaction; excludes clients, reads and independent audits. 1 µs/input point = 1 CPU-second per million points. |
| Durable bytes | Core data/index/catalog file lengths; excludes saved VM caches and Greptime WAL. Total includes every closed-store file and go-carbon's derived discovery files outside the store. File length is not allocated-block bytes. |
| RSS | Post-drain is ingestion-process RSS immediately before reads; peak is max over ingest/read processes including large renders. Not fleet steady-state RAM. |
| Coarse window | Native 7-day query contains only the exported 6/24 h history, with different resolutions/aggregators. **Not an equal-work ranking.** |

### What this means

| Result | Meaning and scope |
| --- | --- |
| **go-carbon reads 10–50× faster on these shapes** | On misc/db **1×24 h and 100×24 h native-API p50 versus VM**, ratios are approximately 14–50×. This is not universal: 1000×1 h is around 4× there; magg gains are smaller, and Greptime is faster on the db 1000×1 h cell. Response sizes/grid/serialization differ, so these are serving comparisons, not isolated read-engine rankings. |
| **VM uses ≈37% less ingest CPU** | Full-rate misc **36.9%** and db **38.5%** below go-carbon; magg only **8.9%** below. Paced magg VM is slightly higher. Do not read 37% as an aggregate or production-capacity forecast. |
| **VM stores 27–52% fewer core bytes** | Full-rate durable B/input point: misc **2.75 → 1.44**, db **2.01 → 0.97**, magg **1.55 → 1.13**. VM does not produce go-carbon's production rollups and can change finite values; total-file accounting and retained archive0 samples differ. |
| **Greptime result depends on the dataset/read shape** | Durable B/input point **2.59 / 1.59 / 2.50** for misc/db/magg. Ingest CPU is higher here; wide db SQL reads can trade more CPU for competitive latency. One table/region is not a tuning search. |
| **Numeric differences are measured, not automatically disqualifying** | VM changes **16/400 misc values**, max relative error **6.52e−13**; db changes **24/400**, max **4.23e−13**; magg checks agree. Bit-exactness is no longer required; a decimal codec and its error/exception policy are under evaluation. |

| Dataset | go-carbon durable B/input point | VM | GreptimeDB | VM reduction vs go-carbon |
| --- | ---: | ---: | ---: | ---: |
| misc | 2.752 | 1.436 | 2.593 | 47.8% |
| db | 2.007 | 0.967 | 1.589 | 51.8% |
| magg | 1.554 | 1.134 | 2.503 | 27.0% |

### All 24 runs

`full` means unpaced input; `50000` means paced at 50,000 points/s. `pack` changes in-memory metadata packing, not the persistent sample codec. For each dataset the first table gives bytes, completion rates, ingest CPU and RSS; read cells are **p50 / p99 ms; server CPU ms/query**. Correctness samples are repeated for both rates; they are not an exhaustive archive-value proof.

¹ Persistence Mpt/s includes settle/control waits and go-carbon's offline flush/compaction; paced runs include pacing time. ² Ingest CPU includes server user+sys through settle and excludes client/read CPU. ³ The native coarse-window column uses different resolutions/aggregators and is not an equivalent-work ranking.

#### misc

**Storage, ingest and memory**

| System / rate | Durable B/input pt | Total B/input pt | Offered Mpt/s | Persistence Mpt/s¹ | CPU µs/input pt² | Peak / post-drain RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| go-carbon / full | 2.752 | 2.786 | 3.931 | 1.778 | 0.955 | 470 / 470 |
| go-carbon pack / full | 2.751 | 2.786 | 4.023 | 1.716 | 1.017 | 466 / 466 |
| VictoriaMetrics / full | 1.436 | 1.864 | 11.215 | 3.537 | 0.603 | 420 / 403 |
| GreptimeDB / full | 2.593 | 4.984 | 0.657 | 0.545 | 1.646 | 936 / 583 |
| go-carbon / 50000 | 2.746 | 2.780 | 0.050 | 0.049 | 1.373 | 469 / 426 |
| go-carbon pack / 50000 | 2.747 | 2.782 | 0.050 | 0.049 | 1.338 | 436 / 436 |
| VictoriaMetrics / 50000 | 1.434 | 1.862 | 0.050 | 0.050 | 0.787 | 460 / 160 |
| GreptimeDB / 50000 | 2.593 | 4.984 | 0.050 | 0.049 | 1.798 | 833 / 353 |

**Warm native reads**

| System / rate | 1 × 24h | 100 × 24h | 1,000 × 1h | Native coarse window³ |
| --- | ---: | ---: | ---: | ---: |
| go-carbon / full | 0.39 / 1.60; 0.50 | 10.71 / 23.66; 22.50 | 54.01 / 104.71; 133.10 | 0.44 / 0.89; 0.40 |
| go-carbon pack / full | 0.36 / 1.42; 0.40 | 10.58 / 15.05; 22.20 | 51.06 / 88.71; 131.40 | 0.50 / 1.24; 0.60 |
| VictoriaMetrics / full | 6.01 / 11.48; 6.30 | 534.41 / 632.05; 579.10 | 230.61 / 291.49; 283.60 | 4.32 / 8.77; 6.50 |
| GreptimeDB / full | 5.51 / 6.70; 7.20 | 71.61 / 76.74; 269.30 | 156.98 / 163.99; 248.10 | 5.27 / 7.45; 7.20 |
| go-carbon / 50000 | 0.32 / 0.68; 0.40 | 9.77 / 10.89; 20.80 | 49.55 / 63.98; 126.80 | 0.51 / 0.97; 0.50 |
| go-carbon pack / 50000 | 0.35 / 0.72; 0.40 | 10.13 / 31.28; 21.30 | 48.81 / 52.30; 124.60 | 0.43 / 0.78; 0.50 |
| VictoriaMetrics / 50000 | 5.54 / 6.37; 5.80 | 565.84 / 715.76; 596.80 | 254.44 / 358.34; 302.80 | 3.69 / 11.80; 6.10 |
| GreptimeDB / 50000 | 6.04 / 9.59; 7.90 | 70.65 / 79.05; 267.60 | 153.60 / 162.84; 247.00 | 4.38 / 5.79; 5.80 |

**Value correctness**

| System / rate | Correct sampled values | Missing timestamps | Numerically different | Float64 bits different | Maximum relative error |
| --- | ---: | ---: | ---: | ---: | ---: |
| go-carbon / full | 400 | 0 | 0 | 0 | 0 |
| go-carbon pack / full | 400 | 0 | 0 | 0 | 0 |
| VictoriaMetrics / full | 384 | 0 | 16 | 16 | 6.52e-13 |
| GreptimeDB / full | 400 | 0 | 0 | 0 | 0 |
| go-carbon / 50000 | 400 | 0 | 0 | 0 | 0 |
| go-carbon pack / 50000 | 400 | 0 | 0 | 0 | 0 |
| VictoriaMetrics / 50000 | 384 | 0 | 16 | 16 | 6.52e-13 |
| GreptimeDB / 50000 | 400 | 0 | 0 | 0 | 0 |

#### db

**Storage, ingest and memory**

| System / rate | Durable B/input pt | Total B/input pt | Offered Mpt/s | Persistence Mpt/s¹ | CPU µs/input pt² | Peak / post-drain RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| go-carbon / full | 2.007 | 2.067 | 4.803 | 1.275 | 0.873 | 401 / 401 |
| go-carbon pack / full | 2.000 | 2.060 | 4.761 | 1.243 | 0.917 | 397 / 397 |
| VictoriaMetrics / full | 0.967 | 1.816 | 10.384 | 2.643 | 0.537 | 342 / 333 |
| GreptimeDB / full | 1.589 | 1.794 | 0.502 | 0.373 | 1.675 | 746 / 507 |
| go-carbon / 50000 | 2.003 | 2.063 | 0.050 | 0.049 | 1.128 | 384 / 369 |
| go-carbon pack / 50000 | 1.999 | 2.059 | 0.050 | 0.049 | 1.178 | 376 / 362 |
| VictoriaMetrics / 50000 | 0.977 | 1.829 | 0.050 | 0.049 | 0.690 | 353 / 130 |
| GreptimeDB / 50000 | 1.589 | 4.500 | 0.050 | 0.048 | 1.707 | 607 / 337 |

**Warm native reads**

| System / rate | 1 × 24h | 100 × 24h | 1,000 × 1h | Native coarse window³ |
| --- | ---: | ---: | ---: | ---: |
| go-carbon / full | 0.39 / 0.71; 0.40 | 10.96 / 12.29; 26.50 | 57.33 / 62.17; 191.80 | 0.45 / 0.81; 0.50 |
| go-carbon pack / full | 0.36 / 0.76; 0.30 | 11.27 / 13.63; 27.00 | 60.61 / 70.13; 203.50 | 0.47 / 0.80; 0.50 |
| VictoriaMetrics / full | 5.63 / 6.08; 5.90 | 532.08 / 547.96; 565.40 | 235.79 / 276.47; 288.10 | 3.71 / 4.25; 5.40 |
| GreptimeDB / full | 3.25 / 4.19; 5.30 | 41.82 / 48.91; 150.10 | 50.50 / 53.97; 177.40 | 2.28 / 2.80; 4.40 |
| go-carbon / 50000 | 0.37 / 0.87; 0.40 | 11.23 / 12.83; 27.80 | 59.63 / 63.28; 200.90 | 0.49 / 0.73; 0.50 |
| go-carbon pack / 50000 | 0.37 / 0.84; 0.40 | 12.29 / 17.70; 29.80 | 67.11 / 78.51; 223.00 | 0.50 / 0.95; 0.60 |
| VictoriaMetrics / 50000 | 5.61 / 6.22; 5.80 | 528.78 / 570.52; 566.60 | 236.63 / 268.72; 288.20 | 3.35 / 3.86; 5.10 |
| GreptimeDB / 50000 | 3.43 / 8.76; 5.70 | 41.69 / 50.00; 150.40 | 52.09 / 63.24; 177.70 | 3.05 / 7.65; 5.30 |

**Value correctness**

| System / rate | Correct sampled values | Missing timestamps | Numerically different | Float64 bits different | Maximum relative error |
| --- | ---: | ---: | ---: | ---: | ---: |
| go-carbon / full | 400 | 0 | 0 | 0 | 0 |
| go-carbon pack / full | 400 | 0 | 0 | 0 | 0 |
| VictoriaMetrics / full | 376 | 0 | 24 | 24 | 4.23e-13 |
| GreptimeDB / full | 400 | 0 | 0 | 0 | 0 |
| go-carbon / 50000 | 400 | 0 | 0 | 0 | 0 |
| go-carbon pack / 50000 | 400 | 0 | 0 | 0 | 0 |
| VictoriaMetrics / 50000 | 376 | 0 | 24 | 24 | 4.23e-13 |
| GreptimeDB / 50000 | 400 | 0 | 0 | 0 | 0 |

#### magg

**Storage, ingest and memory**

| System / rate | Durable B/input pt | Total B/input pt | Offered Mpt/s | Persistence Mpt/s¹ | CPU µs/input pt² | Peak / post-drain RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| go-carbon / full | 1.554 | 1.613 | 4.406 | 0.866 | 0.562 | 465 / 165 |
| go-carbon pack / full | 1.554 | 1.613 | 3.172 | 0.842 | 0.661 | 446 / 207 |
| VictoriaMetrics / full | 1.134 | 2.174 | 7.372 | 1.870 | 0.512 | 374 / 287 |
| GreptimeDB / full | 2.503 | 3.533 | 0.601 | 0.384 | 1.667 | 2708 / 384 |
| go-carbon / 50000 | 1.552 | 1.612 | 0.050 | 0.048 | 0.677 | 459 / 161 |
| go-carbon pack / 50000 | 1.552 | 1.612 | 0.050 | 0.048 | 0.702 | 453 / 175 |
| VictoriaMetrics / 50000 | 1.150 | 2.211 | 0.050 | 0.049 | 0.710 | 443 / 141 |
| GreptimeDB / 50000 | 2.503 | 2.717 | 0.050 | 0.048 | 1.849 | 2602 / 258 |

**Warm native reads**

| System / rate | 1 × 24h | 100 × 24h | 1,000 × 1h | Native coarse window³ |
| --- | ---: | ---: | ---: | ---: |
| go-carbon / full | 2.45 / 3.05; 3.20 | 256.79 / 758.29; 321.10 | 145.34 / 192.03; 255.80 | 0.69 / 1.94; 0.70 |
| go-carbon pack / full | 3.17 / 5.13; 3.80 | 219.10 / 250.52; 275.90 | 79.05 / 89.04; 206.70 | 0.46 / 0.85; 0.40 |
| VictoriaMetrics / full | 6.30 / 12.60; 6.00 | 532.50 / 554.28; 579.80 | 155.20 / 167.90; 194.30 | 3.82 / 5.38; 5.40 |
| GreptimeDB / full | 18.38 / 21.39; 19.30 | 1211.55 / 1663.45; 3841.20 | 270.08 / 370.38; 775.80 | 3.73 / 5.42; 4.70 |
| go-carbon / 50000 | 2.23 / 3.40; 3.00 | 195.61 / 210.89; 251.50 | 79.51 / 88.73; 220.10 | 0.42 / 0.70; 0.50 |
| go-carbon pack / 50000 | 2.37 / 3.38; 3.30 | 218.68 / 250.71; 281.80 | 90.65 / 104.99; 243.40 | 0.53 / 0.86; 0.50 |
| VictoriaMetrics / 50000 | 5.73 / 7.00; 6.00 | 535.72 / 556.32; 584.10 | 161.09 / 175.45; 201.20 | 3.28 / 4.19; 5.30 |
| GreptimeDB / 50000 | 17.92 / 21.16; 18.80 | 1189.41 / 1393.15; 3630.30 | 246.09 / 265.63; 701.40 | 3.33 / 3.87; 4.10 |

**Value correctness**

| System / rate | Correct sampled values | Missing timestamps | Numerically different | Float64 bits different | Maximum relative error |
| --- | ---: | ---: | ---: | ---: | ---: |
| go-carbon / full | 332 | 0 | 0 | 0 | 0 |
| go-carbon pack / full | 332 | 0 | 0 | 0 | 0 |
| VictoriaMetrics / full | 332 | 0 | 0 | 0 | 0 |
| GreptimeDB / full | 332 | 0 | 0 | 0 | 0 |
| go-carbon / 50000 | 332 | 0 | 0 | 0 | 0 |
| go-carbon pack / 50000 | 332 | 0 | 0 | 0 | 0 |
| VictoriaMetrics / 50000 | 332 | 0 | 0 | 0 | 0 |
| GreptimeDB / 50000 | 332 | 0 | 0 | 0 | 0 |


### go-carbon archive-0 / rollup / header-catalog accounting

**The durable totals above cannot be uniquely split into physical bytes by archive.** SST block compression mixes archives and writer headers. The table below measures live encoded key/value bytes for archive groups/descriptors; shared header/catalog bytes are left separate. These logical encoded columns do **not** sum to the compressed durable B/input point. Parentheses use retained points in that archive family as denominator. Buffered rollups can reside in shared headers.

| Dataset / rate / packing | Archive0 points | Rollup points | Archive0 encoded B/input pt (B/archive0 pt) | Rollup encoded B/input pt (B/rollup pt) | Shared/header/catalog B/input pt |
| --- | ---: | ---: | ---: | ---: | ---: |
| db / full / off | 6,345,854 | 903,697 | 1.629 (1.630) | 0.318301 (2.237) | 1.991 |
| db / 50000 / off | 6,345,784 | 903,697 | 1.629 (1.630) | 0.318301 (2.237) | 1.991 |
| db / full / on | 6,345,644 | 903,697 | 1.629 (1.630) | 0.318301 (2.237) | 1.991 |
| db / 50000 / on | 6,345,644 | 903,697 | 1.629 (1.630) | 0.318301 (2.237) | 1.991 |
| magg / full / off | 4,391,985 | 2,210 | 1.737 (1.737) | 0.000473 (0.939) | 0.433 |
| magg / 50000 / off | 4,391,985 | 2,210 | 1.737 (1.737) | 0.000473 (0.939) | 0.433 |
| magg / full / on | 4,391,985 | 2,210 | 1.737 (1.737) | 0.000473 (0.939) | 0.433 |
| magg / 50000 / on | 4,391,985 | 2,210 | 1.737 (1.737) | 0.000473 (0.939) | 0.433 |
| misc / full / off | 13,214,305 | 1,087,881 | 2.191 (2.193) | 0.271954 (3.306) | 2.079 |
| misc / 50000 / off | 13,213,663 | 1,087,881 | 2.191 (2.193) | 0.271590 (3.301) | 2.079 |
| misc / full / on | 13,214,305 | 1,087,881 | 2.191 (2.193) | 0.271954 (3.306) | 2.079 |
| misc / 50000 / on | 13,212,576 | 1,087,881 | 2.191 (2.193) | 0.271590 (3.301) | 2.079 |

Magg retains every input point in archive0. Misc/db archive0 counts are slightly lower because original timestamps age beyond the shortest production retention during replay; some samples route to coarser archives. This is not proof of a lossless point-for-point primary-data disk comparison. No synthetic seven-day history or duplicated data was added.

### Metadata packing A/B

| Dataset / rate | Charged metadata MiB off→on | Charged reduction | Arena MiB off→on | Ingest CPU change | Post-drain RSS MiB off→on |
| --- | ---: | ---: | ---: | ---: | ---: |
| misc / full | 29.57→14.87 | 49.7% | 43.69→28.38 | +6.5% | 470→466 |
| misc / 50000 | 29.57→14.93 | 49.5% | 44.06→31.60 | -2.6% | 426→436 |
| db / full | 13.50→7.09 | 47.5% | 24.38→17.54 | +5.0% | 401→397 |
| db / 50000 | 13.50→7.09 | 47.5% | 24.50→18.75 | +4.4% | 369→362 |
| magg / full | 2.22→1.48 | 33.3% | 7.92→7.59 | +17.5% | 165→207 |
| magg / 50000 | 2.22→1.48 | 33.3% | 8.19→7.96 | +3.6% | 161→175 |

One paired run per dataset/rate, without eviction pressure. Charged-byte reductions are consistent; RSS and CPU require repeated capacity-matched qualification. Thousands of stripes and minimum 4 KiB chunks mean compressing scattered records may not free a chunk. Arena chunks grow up to 64 KiB. The 33–50% charged saving is not a 33–50% RSS promise. [Packing threshold][C-snappy] [State cache][C-state] [Arena][C-slab]

### RSS during paced ingestion

Median/range in the final 20 s of the 50,000-points/s replay, before reads; a short replay still does not establish production steady state.

| Dataset / system | Median RSS MiB | Range MiB | Samples |
| --- | ---: | ---: | ---: |
| db / go-carbon | 337 | 303–341 | 38 |
| db / go-carbon pack | 367 | 266–376 | 38 |
| db / GreptimeDB | 334 | 298–359 | 37 |
| db / VictoriaMetrics | 126 | 125–129 | 37 |
| magg / go-carbon | 151 | 135–154 | 38 |
| magg / go-carbon pack | 175 | 173–179 | 38 |
| magg / GreptimeDB | 229 | 185–257 | 38 |
| magg / VictoriaMetrics | 141 | 140–141 | 38 |
| misc / go-carbon | 408 | 408–408 | 38 |
| misc / go-carbon pack | 433 | 433–433 | 37 |
| misc / GreptimeDB | 351 | 297–397 | 38 |
| misc / VictoriaMetrics | 159 | 159–160 | 37 |

### Native read work explains part of the latency difference

These are full-rate, packing-off responses. Counts are non-null decoded points. go-carbon may return empty named series that SQL/VM omit; VM lookback/gap-fill on a 1 s grid can return more values. Greptime SQL also sorts the paired timestamp/value arrays. Profiling did not isolate each cost; a common serializer/grid/response contract is needed for a pure read-engine ranking.

| Dataset / system | 100×24h points / response MiB | 1000×1h series / points / response MiB |
| --- | ---: | ---: |
| misc / go-carbon | 117,551 / 1.06 | 1,000 / 56,339 / 0.74 |
| misc / VictoriaMetrics | 352,653 / 148.12 | 970 / 169,017 / 60.28 |
| misc / GreptimeDB | 117,551 / 2.26 | 970 / 56,339 / 1.25 |
| db / go-carbon | 133,568 / 1.20 | 1,000 / 58,419 / 0.91 |
| db / VictoriaMetrics | 400,704 / 148.08 | 996 / 175,257 / 62.03 |
| db / GreptimeDB | 133,568 / 2.64 | 996 / 58,419 / 1.39 |
| magg / go-carbon | 1,680,114 / 21.09 | 1,000 / 305,611 / 3.44 |
| magg / VictoriaMetrics | 1,924,077 / 145.25 | 664 / 380,569 / 40.62 |
| magg / GreptimeDB | 1,680,114 / 31.08 | 664 / 305,611 / 5.74 |

## Qualification and remaining questions

| Area | Next evidence needed |
| --- | --- |
| Decimal codec | Error tolerance, exceptional-value and staleness policy, compression/CPU/read cost, migration/rollback and precision across rollups. It need not preserve arbitrary Float64 bits under the current decision. |
| Serving parity | Repeat with a common grid/serializer and output contract; isolate gap-fill, aggregation and storage work. Add concurrent requests/writes and more than one empirical p99. |
| Sustained capacity | Quiet host, common OS CPU quota, realistic cardinality/churn and 960 s coalescing; repeat through compaction-debt recovery. These 2.5–11.5 K-series replays cannot forecast a ~20 M-series store. |
| Correctness | Larger retained-value checks, actual seven-day history, duplicate/OOO/late corrections, retention boundaries and explicit XFF/staleness behavior. All Greptime finite-input counts match, but count equality alone is insufficient. |
| Memory | Capacity-matched useful hit rates, physical charged/arena/pinned/mapped bytes and peak scratch. Opt-in budget pressure CPU and p99 remain unqualified. |
| Append | Finish end-to-end comparison, bound pending runs/fold debt, measure uncached-read CPU/allocation and preserve archive/metadata lifecycle. |
| Expiry | Prove true event-time/archive-cohort retirement; current write-hour buckets and metric/catalog GC are not substitutes. |
| External details | VM release-source match, exact WAL/acknowledgement guarantees across engines, Greptime last-row modes and SQL materialization, Prometheus TSDB mechanisms remain partially/not verified. |

## Source references

Go-carbon links below are relative to the repository and refer to the baseline named above. External code links use pinned public upstream revisions. The mechanism study, local replay and separate representation screens establish different scopes; none is a production deployment or a guaranteed fleet-wide saving.

[C-aggregate]: ../../vendor/github.com/go-graphite/go-whisper/whisper.go#L1931
[C-align]: ../../vendor/github.com/go-graphite/go-whisper/whisper.go#L1302
[C-append]: ../../internal/whisperio/continuation_blocks.go#L341
[C-bucket]: ../../internal/whisperio/continuation_blocks.go#L169
[C-cache]: ../../cache/cache.go#L103
[C-compact]: ../../cache/compact.go#L11
[C-compress]: ../../vendor/github.com/go-graphite/go-whisper/compress.go#L662
[C-controller]: ../../internal/chunkstore/compaction_controller.go#L102
[C-delete]: ../../internal/chunkstore/admin.go#L307
[C-delete-prefix]: ../../internal/chunkstore/continuation_io.go#L502
[C-exact]: ../../internal/chunkstore/exact.go#L49
[C-exact-tests]: ../../internal/chunkstore/exact_test.go#L30
[C-expire]: ../../internal/chunkstore/expiration.go#L11
[C-fetch]: ../../vendor/github.com/go-graphite/go-whisper/whisper.go#L1525
[C-fetch-view]: ../../internal/chunkstore/fetch.go#L22
[C-floatbits]: ../../vendor/github.com/go-graphite/go-whisper/whisper.go#L1981
[C-fst]: ../../carbonserver/snapshot_fst.go#L393
[C-group]: ../../internal/whisperio/continuation_blocks.go#L108
[C-identity]: ../../internal/chunkstore/types.go#L60
[C-index]: ../../carbonserver/metricstore.go#L35
[C-inputlog]: ../../internal/inputlog/inputlog.go#L1
[C-intgate]: ../../internal/chunkstore/exact.go#L108
[C-io]: ../../internal/chunkstore/continuation_io.go#L48
[C-items]: ../../cache/items.go#L10
[C-layout-doc]: ../../doc/pebble-storage/data-layout.md#L1
[C-mapped]: ../../carbonserver/metricstore_mapped.go#L124
[C-merge]: ../../internal/chunkstore/access.go#L154
[C-merger]: ../../internal/chunkstore/chunk.go#L198
[C-pacer]: ../../internal/chunkstore/compaction_pacer.go#L134
[C-prewarm]: ../../carbon/metadata_prewarm.go#L23
[C-propagate]: ../../vendor/github.com/go-graphite/go-whisper/whisper.go#L1398
[C-queue]: ../../cache/queue.go#L28
[C-read]: ../../carbonserver/fetchfromdisk.go#L1
[C-read-doc]: ../../doc/pebble-storage/read-path.md#L1
[C-slab]: ../../internal/chunkstore/state_slab.go#L28
[C-slots]: ../../internal/chunkstore/state_slots.go#L61
[C-snappy]: ../../internal/chunkstore/state_slab.go#L450
[C-state]: ../../internal/chunkstore/state_cache.go#L46
[C-state-pack]: ../../internal/chunkstore/state_cache.go#L436
[C-store]: ../../internal/chunkstore/store.go#L135
[C-timings]: ../../doc/pebble-storage/dashboard-timings.md#L18
[C-update]: ../../internal/chunkstore/continuation_codec.go#L55
[C-wal]: ../../internal/chunkstore/wal_sync.go#L9
[C-wal-options]: ../../vendor/github.com/cockroachdb/pebble/options.go#L536
[C-wal-rotation]: ../../internal/chunkstore/store.go#L399
[C-whisper]: ../../vendor/github.com/go-graphite/go-whisper/whisper.go#L29
[C-whisper-update]: ../../vendor/github.com/go-graphite/go-whisper/whisper.go#L1058
[C-write-doc]: ../../doc/pebble-storage/write-path.md#L1
[C-writer]: ../../internal/whisperio/continuation_codec.go#L620
[G-manual]: ../../vendor/github.com/cockroachdb/pebble/internal/manual/manual_nocgo.go#L14
[G-membership]: ../../internal/chunkstore/catalog_membership.go#L37
[GT-release-config]: https://github.com/GreptimeTeam/greptimedb/blob/badceec12a0e98fd69e83e9b9b9b06d9358b78dd/src/mito2/src/config.rs
[H-data]: https://github.com/GreptimeTeam/greptimedb/blob/2fdbe6c8c30d2d9a04b106f6f4d7accb330338c2/src/mito2/src/memtable/partition_tree/data.rs#L241
[H-dict]: https://github.com/GreptimeTeam/greptimedb/blob/2fdbe6c8c30d2d9a04b106f6f4d7accb330338c2/src/mito2/src/memtable/partition_tree/dict.rs#L34
[H-tree]: https://github.com/GreptimeTeam/greptimedb/blob/2fdbe6c8c30d2d9a04b106f6f4d7accb330338c2/src/mito2/src/memtable/partition_tree.rs#L74
[T-allocation]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/memtable.rs#L353
[T-bloom]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/sst/index/bloom_filter/creator.rs#L57
[T-bulk]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/memtable/bulk.rs#L126
[T-bulkpart]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/memtable/bulk/part.rs#L1257
[T-cache]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/cache.rs#L233
[T-cache-size]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/cache/cache_size.rs#L20
[T-config]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/config.rs#L72
[T-flat]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/sst/parquet/flat_format.rs#L73
[T-floatopt]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/store-api/src/mito_engine_options.rs#L126
[T-flush]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/flush.rs#L118
[T-flush-worker]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/worker/handle_flush.rs#L290
[T-format]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/sst/parquet/format.rs#L76
[T-inverted]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/sst/index/inverted_index/creator.rs#L63
[T-invertedread]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/sst/index/inverted_index/applier.rs#L116
[T-memory]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/common/memory-manager/src/manager.rs#L38
[T-memselect]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/memtable.rs#L493
[T-parquet]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/sst/parquet.rs#L100
[T-picker]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/compaction/picker.rs#L169
[T-series]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/memtable/time_series.rs#L165
[T-sstversion]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/sst/version.rs#L241
[T-task]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/compaction/task.rs#L85
[T-timeparts]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/memtable/time_partition.rs#L50
[T-twcs]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/compaction/twcs.rs#L108
[T-twcs-options]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/region/options.rs#L446
[T-write-worker]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/worker/handle_write.rs#L171
[T-writer]: https://github.com/GreptimeTeam/greptimedb/blob/8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb/src/mito2/src/sst/parquet/writer.rs#L572
[V-block]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/block.go#L22
[V-decimal]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/decimal/decimal.go#L437
[V-dedup]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/dedup.go#L30
[V-encoding]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/encoding/encoding.go#L119
[V-fastalloc]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/vendor/github.com/VictoriaMetrics/fastcache/malloc_mmap.go#L21
[V-fastcache]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/vendor/github.com/VictoriaMetrics/fastcache/fastcache.go#L123
[V-filestream]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/filestream/filestream.go#L96
[V-graphite]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/storage.go#L1440
[V-header]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/block_header.go#L19
[V-index]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/index_db.go#L412
[V-ingest]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/storage.go#L1655
[V-inmem]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/inmemory_part.go#L15
[V-int]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/encoding/int.go#L87
[V-memlinux]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/memory/memory_linux.go#L12
[V-memory]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/memory/memory.go#L29
[V-merge]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/merge.go#L42
[V-mergeset-codec]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/mergeset/encoding.go#L265
[V-mergeset-part]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/mergeset/part.go#L24
[V-nearest]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/encoding/nearest_delta.go#L15
[V-part]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/part.go#L30
[V-partition]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/partition.go#L35
[V-raw]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/raw_row.go#L12
[V-retention]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/table.go#L428
[V-storage-caches]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/storage.go#L338
[V-streamwriter]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/block_stream_writer.go#L138
[V-tsid]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/storage/tsid.go#L17
[V-wsc]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/05903c8acd9557ef018bcd0a11d648d5d75f6523/lib/workingsetcache/cache.go#L37
