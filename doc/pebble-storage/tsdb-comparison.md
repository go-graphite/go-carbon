# Shared Pebble storage, VictoriaMetrics, Prometheus/Mimir, OpenObserve and GreptimeDB

Mechanisms and current state, 2026-11-03, using supplied 2 November captures and research. The identical-data server replays below were measured on 30 October; the November research and branch results do not rerun or revise those measurements.

The design motivation is a production store with roughly **20 M cached metrics on 192 cores**. That context is separate from the benchmark: identical finite production-derived samples were replayed on one shared 16-core, 48 GiB arm64 machine. Dataset labels `D1`, `D2` and `D3` identify the three anonymized exports: D1 has many sparse series (11,475 series over 24 h), D2 fewer and denser ones (6,886 over 24 h), D3 a small, dense, aggregated set (2,473 over 6 h).

**Current decision:** exact v9 cold packing is measured; bounded decimal encoding remains optional. The October replay uses exact v7. Bit-exactness is not mandatory for the optional decimal policy, but the proposed format-free optimizations must preserve timestamp and Float64 bits, including signed zero and NaN payloads.

| Evidence | Scope |
| --- | --- |
| go-carbon source and measured binary | October replay: `d45860252fd38cb67a29fe2859d791f51840ecaf`, exact continuation v7. Current documentation base: `df9e1a4a`. November studies inspected `390daaff` plus named local branches; those branches are not one combined deployment. |
| VictoriaMetrics mechanisms | Original table: `05903c8acd9557ef018bcd0a11d648d5d75f6523`; November follow-up: `5858c66de15d18fec31ef0b6ffa580bbaad2870e`. Single-node design; citations keep their respective pins. |
| VictoriaMetrics measured binary | v1.155.0, release revision `3fec2f7241`, `precisionBits=64`. Its exact release source was unavailable during the offline study; mechanism citations use the snapshot above. |
| GreptimeDB mechanisms | Source snapshot `8ecf49dca017adf873d31f7d4e6f01ebd4cef1cb`, Mito. Its flat/bulk defaults are not assumed to describe every release. |
| GreptimeDB measured binary | v1.3.0, release revision `badceec12a0e98fd69e83e9b9b9b06d9358b78dd`; one ordinary table and one region. |
| November source studies | Prometheus `b0c3a30ab2a09b88e64d876017a0134aa2f38f40`; Mimir `1036b38cbd8c8e4bdeff829d6d99fa59bddb5928`; OpenObserve `04f6d0b8a9de66d011f7b83033d42b711565bc4c`. Source mechanisms and primitive screens, not equivalent full-server replays. |
| October measurements | 24 completed runs: three datasets × four configurations (go-carbon, packing-on go-carbon, VM, GreptimeDB) × full-rate/50,000-points/s replay. Separate metadata and append screens are labelled below. |
| Unverified details | “not verified” means the cited source or experiment does not establish that fact. Prometheus/Mimir and OpenObserve now have pinned source studies and primitive screens; neither has an equivalent full-server replay here. |

## At a glance

**Historical 30 October measurements.** Full-rate replays of the three datasets (details, paced runs and method in [Measured on identical data](#measured-on-identical-data)). Each cell lists **D1 · D2 · D3**, rows grouped by kind. Read latency cells are warm native-API **p50 / p99 ms**; read CPU is server CPU per query. Response shapes differ, so reads compare serving cost, not isolated engines.

| Angle | go-carbon | VictoriaMetrics | GreptimeDB | Best |
| --- | --- | --- | --- | --- |
| **Storage** | | | | |
| Durable bytes / input point | 2.75 · 2.01 · 1.55 | **1.44 · 0.97 · 1.13** | 2.59 · 1.59 · 2.50 | VM (27–52 % below go-carbon) |
| All files / input point (incl. index, discovery) | 2.79 · 2.07 · **1.61** | **1.86** · 1.82 · 2.17 | 4.98 · **1.79** · 3.53 | VM D1, GreptimeDB D2, go-carbon D3 |
| Durable bytes / input point, cold history packed ([#1034](https://github.com/go-graphite/go-carbon/pull/1034), fully sealed ceiling) | 1.99 · 1.41 · **1.09** | **1.44 · 0.97** · 1.13 | — | VM D1/D2, go-carbon D3 |
| **Ingest** | | | | |
| Ingest CPU, µs / input point | 0.955 · 0.873 · 0.562 | **0.603 · 0.537 · 0.512** | 1.646 · 1.675 · 1.667 | VM (9–39 % below go-carbon) |
| Persistence rate, M points/s (incl. settle) | 1.78 · 1.28 · 0.87 | **3.54 · 2.64 · 1.87** | 0.55 · 0.37 · 0.38 | VM |
| **Memory** | | | | |
| Peak / post-drain RSS, MiB | 470/470 · 401/401 · 465/165 | **420/403 · 342/333 · 374/287** | 936/583 · 746/507 · 2708/384 | VM peak; go-carbon D3 post-drain |
| **Read latency (p50 / p99 ms)** | | | | |
| 1 × 24 h | **0.39/1.60 · 0.39/0.71 · 2.45/3.05** | 6.01/11.48 · 5.63/6.08 · 6.30/12.60 | 5.51/6.70 · 3.25/4.19 · 18.38/21.39 | go-carbon (2.5–15× faster p50) |
| 100 × 24 h | **10.7/23.7 · 11.0/12.3 · 257/758** | 534/632 · 532/548 · 533/554 | 71.6/76.7 · 41.8/48.9 · 1212/1663 | go-carbon (2–50× faster p50) |
| 1,000 × 1 h | **54.0/105** · 57.3/62.2 · **145**/192 | 231/291 · 236/276 · 155/**168** | 157/164 · **50.5/54.0** · 270/370 | go-carbon D1, GreptimeDB D2; D3 split |
| **Read CPU** | | | | |
| 100 × 24 h, ms / query | **22.5 · 26.5 · 321** | 579 · 565 · 580 | 269 · 150 · 3,841 | go-carbon |
| 1,000 × 1 h, ms / query | **133** · 192 · 256 | 284 · 288 · **194** | 248 · **177** · 776 | mixed |
| **Correctness and semantics** | | | | |
| Values changed (of sampled) | **0/400 · 0/400 · 0/332** | 16/400 · 24/400 · 0/332 (max relative error 6.5e-13) | **0/400 · 0/400 · 0/332** | go-carbon, GreptimeDB (exact) |
| Coarse retention | Native Whisper archives and rollups, written at ingest | Raw samples only | Raw samples only | Different contracts |

### Where go-carbon's bytes go

Settled SST bytes per input point split by component, from a separate replay of the same data (the split apportions compressed blocks by their uncompressed sections, so components are allocations, not exact causal savings).

| Component | D1 | D2 | D3 |
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

### Closing the storage gap: October prototypes

These October prototypes keep native rollups and are off by default. [#1034](https://github.com/go-graphite/go-carbon/pull/1034) describes the packing/decimal design. The newer exact-only, fully compacted result is recorded separately in “go-carbon now”; its −25.1% must not be applied to these replay bytes a second time.

| Durable bytes / input point | D1 | D2 | D3 |
| --- | ---: | ---: | ---: |
| go-carbon October baseline | 2.75 | 2.01 | 1.55 |
| Cold history repacked into 4,096-point sealed blocks | 2.28 (−17 %) | 1.65 (−17 %) | **1.09 (−30 %)** |
| Decimal value blocks, measured separately | −13 % | −15 % | −4 % |
| Cold packing, decimal blocks on D1/D2 and exact on D3 (#1034) | 1.99 (−27.5 %) | 1.41 (−29.8 %) | **1.09 (−29.8 %)** |
| VictoriaMetrics | 1.44 | 0.97 | 1.13 |

Repacking applies only to history older than the write window; the short exports here are its full-sealing ceiling. Decimal blocks double value-encoding CPU. Combined in #1034, decimal cold packing reaches −27.5 % on D1 and −29.8 % on D2, and exact packing −29.8 % on D3, which puts D3 below VictoriaMetrics. The paced background sweep costs 38–54 CPU-s per durable GB; reader p99 moved by at most +0.18 ms during packing.

### go-carbon now

Current source capabilities and supplied 2 November measurements are separate from the October replay. Source baseline `df9e1a4a`; the source-study baseline `390daaff` precedes the merged per-component cost report. Local branch hashes identify separate experiments, including opt-in features. Evidence receipts are named below; no new production collection was performed.

| Area | go-carbon now | Evidence / boundary |
| --- | --- | --- |
| Layout and exact cold packing | v6 metric-scoped groups; v7 write-hour buckets; v9 seals eligible cold bucketed history into 4,096-point groups, preserving rollups. Fully compacted paired stores: **1,559.3→1,167.2 GB Pebble (−25.1%)**, **179.3→135.3 KB/active series (−24.5%)**. | P, `coldpack-cmp-1102-RESULT.md`; different series, matched traffic/distribution. Only already bucketed history was eligible; most supplied stores still v6. Local code: [packing](../../internal/whisperio/continuation_sealed.go). |
| Metadata packing | Existing pointer-free slabs and lossless Snappy. Supplied canary: **2,146→1,642 B/entry**; separate total-cache observation **42.9→33.3 GB**, RSS **224→203 GB**. | P, supplied handoff recorded in `resource-costs-NOTES.md`; observations/entry coverage differ, so do not derive one ratio from the other. [cache](../../internal/chunkstore/state_cache.go). |
| Cold-level Zstd | `pebble-zstd` `dc2c98ef`: optional L5/L6 Zstd, L0–L4 Snappy. **2.161→2.084 B/pt v7 (−3.57%)**, **2.124→1.898 B/pt v9 (−10.62%)**. | L, `pebble-zstd-RESULT.md`; estimated **+0.34–0.51 core** at 50–75 MB/s v9 compaction input; cache-miss point p50 **108→223 µs**. Existing-reader SST support; opt-in/restart required. |
| Mutable point cache | `cache-gorilla` `e8560ed7`: retained point capacity **5.10→1.58 B/pt**; whole-cache heap **7.70→4.26 B/pt**, at 105 pt/metric and 70% integers. Encoder state **56→80 B/series**. | L, `cache-gorilla-RESULT.md`; estimated **+0.085 core at 3M pt/s**; 30% random-mantissa mixture grows to **2.96 B/pt**. Supplied one-store canary pending. No durable-format change. |
| Cache bookkeeping | `cache-items` `1cacb2c5`: **660→528 B/metric**, staging **32→24 B/point**, map **16→9 B/slot**. | L, `cache-items-RESULT.md`; **660 MB at 5M metrics** is live-heap scaling, not an RSS promise. Inspect ancestry separately from Gorilla's payload result. |
| Shared names | `name-registry` `35e01a1a`: **−167 B/name**, **3.66→2.82 GB at 5M names**, mixed fixture. | L, `name-registry-RESULT.md`; default off; identities alone **+14 B/name**; metadata/index integration incomplete. |
| Metadata templates | `metadata-compact` `6c5c63f5`: Snappy→templates **720→565 logical B/entry**, **785→627 heap B/entry**. | L, `metadata-compact-RESULT.md`; default off. Its independent ID registry costs about **292 B/name**, so IDs alone lose without shared ownership. |
| Restart and handoff | `restart-codec` `9974e2e4`: negotiated tail **210.4→9.55 B/pt**, admission **639→480 ms**; opt-in stream dump **5.88→2.17 B/pt**, restore **709→497 ms**. | L, `restart-codec-RESULT.md`; dump encode **+40%**, open **72→272 ms**, expanded compact data retained. Old peers get legacy tails; old binaries cannot safely recover crash-leftover stream dumps. |
| Whole-worker scale | Five supplied workloads: **4.14–28.7M cached metrics**, **20–209 pt/metric**; worker RSS **108–358 GB**, live Go heap **23–148 GB**, metadata logical **12–96.5 GB**, arena **13.3–108 GB**. | P, 2 Nov ~22:50 CPU/heap profiles decoded locally with `go tool pprof -top`; contemporaneous worker metrics, not supervisor smaps. Native Pebble allocations are outside Go heap. |
| Existing limits and accelerators | Catalog ID+generation/revision; mapped FST + overlay; striped metadata/tail caches, Pebble block cache, compressed response cache and response singleflight; startup budgets and paced background work. | [index](../../carbonserver/metricstore.go), [cache keys](../../carbonserver/render.go), [memory policy](../../carbon/memory_budget.go). Estimated query-byte reservations remain a gap; no hard RSS cap claimed. |

Per-component cost provenance: `resource-costs-NOTES.md` and the merged cost inventory, with separate heap/native/mmap/IO/network/dump/handoff boundaries. Input log already measures **0.915–2.64 B/logged point**, about one sync/s; render CPU in the five quiet profiles is **0.073–1.95 cores**. Do not infer a network or whole-worker saving from a codec payload ratio.

## Master mechanism table

| Mechanism | go-carbon: shared Pebble, current capabilities (October replay remains v7) | VictoriaMetrics: single node | GreptimeDB: Mito | Prometheus TSDB | Whisper: classic go-carbon |
| --- | --- | --- | --- | --- | --- |
| Series identity & stable IDs | Dotted metric name in catalog; durable uint64 ID, generation and revision. Archive/policy belong to metadata. Hash collisions checked against full names; an FST ordinal is not a durable ID. [`internal/chunkstore/types.go`][C-identity] [`cache/items.go`][C-items] [`carbonserver/metricstore.go`][C-index] | Metric name + tags → TSID: 24 B (metric-group uint64, job/instance uint32 each, unique MetricID uint64). Group hashes provide locality; MetricID supplies identity. [V-tsid][V-tsid] [V-index][V-index] | Region schema + encoded primary key, time index, sequence and op. Arrow batch PK dictionaries use UInt32 indices; these are local dictionary positions, not durable global series IDs. [T-format][T-format] [T-flat][T-flat] [T-bulkpart][T-bulkpart] | Series labels → hash lookup → reusable SeriesRef; refs are process/Head scoped, not portable durable identities. [P-append][P-append] | One metric → one .wsp file, archive offsets inside its header; no shared-store ID needed. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |
| Ingestion path & mutable structure | Sharded itemMap → compactPoints in arrival order → deadline queue → persister → indexed Pebble batch / continuation writer. Popped, unconfirmed points remain readable. [`cache/items.go`][C-items] [`cache/compact.go`][C-compact] [`cache/queue.go`][C-queue] [`internal/chunkstore/continuation_codec.go`][C-update] [`cache/cache.go`][C-cache] | AddRows in chunks ≤8,000 → partition rawRowsShards (round-robin CPU shards, ~8 MiB backing arrays) → sorted, decimal-converted compressed in-memory parts. No object/lock per series in rawRows. [V-ingest][V-ingest] [V-raw][V-raw] [V-partition][V-partition] [V-inmem][V-inmem] | Flat/sparse PK → BulkMemtable (ordinary new-region default because flat format defaults true); dense PK → TimeSeriesMemtable unless bulk selected. SeriesMap is `BTreeMap<PK, Arc<RwLock<Series>>>`, builders start at 4 rows, freeze around 512. [T-memselect][T-memselect] [T-config][T-config] [T-series][T-series] [T-bulk][T-bulk] | Per-series Head object + mutable XOR chunk, completed chunks mmap; WAL records precede commit. [P-head][P-head] [P-wal][P-wal] | Same go-carbon front cache/persister; classic writer aligns points and writes fixed circular slots. CWhisper is a separate compressed file variant, also supported by this tree. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper-update] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-align] |
| Sample bytes in RAM (payload ≠ process RSS) | compactPoints: 1-byte header + optional 1–10 B zigzag delta-of-delta + 0–8 B byte-XOR payload: 1–19 B/point; regular repeats settle at 1 B. 64-bit compactPoints struct: 56 B plus backing capacity; names, items, queues, log and decoded scratch are extra. [`cache/compact.go`][C-compact] [`internal/inputlog/inputlog.go`][C-inputlog] | rawRow = 48 B on ordinary 64-bit layouts: 24 B TSID + 8 B timestamp + 8 B float + precision/alignment. Compressed in-memory parts have variable bytes/point; 48 B is only raw staging. [V-raw][V-raw] [V-inmem][V-inmem] | No verified single B/sample constant: PK dictionaries, Arrow columns, sequence/op and multi-field rows vary. TimeSeriesMemtable also pays per-series tree/key/Arc/lock/builders; bulk parts can hold encoded Parquet bytes. [T-series][T-series] [T-bulkpart][T-bulkpart] | Source-layout screen: stringlabels memSeries 184 B (Mimir fork 200 B), memChunk 40 B, XOR object 32 B and appender 40 B, before labels/maps/postings/payload. [P-head][P-head] | Front-cache bytes as at left. Classic disk point = 12 B (uint32 seconds + Float64); this is not the RAM cost of a cached point or CWhisper's compressed bytes. [`cache/compact.go`][C-compact] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] |
| 960 s × 10M series × 240 points | 2.4B queued points. Supplied D3 write-cache category ≈12 GiB → 5.37 B/queued point if the illustrative population were exact (includes overhead). This is a sizing model, not an identical-data benchmark. [buffer sizing](#buffer-sizing-and-memory-context) | 48 × 2.4B / 2³⁰ = 107.29 GiB raw rows, before indices/scratch. VM does not normally retain rawRows for 960 s. At 35 points/series: 15.65 GiB. [V-raw][V-raw] [buffer sizing](#buffer-sizing-and-memory-context) | Equivalent cardinality, multi-field schema and compressed memory: not verified. No inferred ratio from Parquet or Rust. | Full-Head retained B/point at this cadence/cardinality not measured; compressed payload alone is insufficient. | A hypothetical compact raw (ID, int64 timestamp, float) tuple is 24 B →53.64 GiB; classic disk's 12 B/slot is a different representation. |
| Flush cadence & visibility | Reference production cadence: 960 s coalescing; deadline policy has startup/restore/sparse spread. Long batches amortize per-metric read/modify/commit. Cache overlay supplies immediate visibility; this interval is not a WAL sync period. [`cache/queue.go`][C-queue] [`cache/cache.go`][C-cache] [`internal/inputlog/inputlog.go`][C-inputlog] [`internal/chunkstore/store.go`][C-store] | Pending raw rows → searchable parts every 2 s; due in-memory parts → disk default 5 s (min 2 s). Pending rawRows are not searched. Parts are already encoded; final one-part flush may copy streams directly. [V-partition][V-partition] [V-inmem][V-inmem] | Periodic auto-flush default 10 min, plus pressure. Freeze lowers active bytes but total stays charged until owners release. Flush eligible large regions until pressure falls. [T-config][T-config] [T-flush][T-flush] [T-flush-worker][T-flush-worker] [T-allocation][T-allocation] | Committed Head samples are queryable; chunk cut, WAL sync and persisted block creation are separate boundaries. [P-append][P-append] | Same cache deadlines; writes go to the selected metric file rather than a shared LSM. Fixed-size classic archives avoid LSM memtable flush/compaction. [`cache/queue.go`][C-queue] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper-update] |
| Persistent layout & seek unit | Shared Pebble SST/WAL; catalog + metric ID/generation keys. Continuation manifest, 128 B header pages, block descriptors, exact 128-slot point groups. v7 group keys start with write-hour bucket + ID/generation; v9 adds 4,096-point sealed cold groups and implicit regular timestamps. Header/descriptor keys stay metric-scoped. [`internal/chunkstore/sealed.go`](../../internal/chunkstore/sealed.go) [`internal/chunkstore/types.go`][C-identity] [`internal/whisperio/continuation_blocks.go`][C-bucket] [`internal/whisperio/continuation_blocks.go`][C-append] [`doc/pebble-storage/data-layout.md`][C-layout-doc] | Calendar-month partitions; immutable in-memory/small/big parts. timestamps.bin, values.bin, index.bin, metaindex.bin + metadata. Series blocks ≤8,192 rows, sorted (TSID,timestamp); series span blocks. [V-partition][V-partition] [V-part][V-part] [V-header][V-header] [V-block][V-block] | Region SSTs are Parquet, ordered by PK/time with sequence/op fields; default row group 102,400 rows, query batch 8,192. Flat layout can retain tag columns; column projection and statistics prune reads. [T-format][T-format] [T-flat][T-flat] [T-writer][T-writer] [T-parquet][T-parquet] | Time blocks separate index and chunk segments; series index stores refs with time bounds. [P-chunks][P-chunks] [P-index][P-index] | One file per metric, header + one fixed circular area per retention. Classic 12 B slots; compressed CWhisper has variable blocks and writer buffers. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] [`vendor/github.com/go-graphite/go-whisper/compress.go`][C-compress] |
| Timestamp codec | Cache: lossless delta-of-delta + zigzag uvarint. Exact disk groups: signed timestamp deltas over present slots + 128-bit presence bitmap, optional Zstd; do not conflate the two codecs. [`cache/compact.go`][C-compact] [`internal/chunkstore/exact.go`][C-exact] | Detect constant arrays/steps, nearest-delta or nearest-delta-of-delta → zigzag/varint → optional Zstd. precisionBits=64 preserves the integer stream. Adjacent equal timestamp streams can share offsets. [V-encoding][V-encoding] [V-nearest][V-nearest] [V-int][V-int] [V-streamwriter][V-streamwriter] | Parquet timestamp/sequence: DELTA_BINARY_PACKED, dictionaries disabled. Event-time memtable partitions are half-open windows (initially one day when unspecified). [T-writer][T-writer] [T-parquet][T-parquet] [T-timeparts][T-timeparts] | Timestamp delta-of-delta bitstream (native milliseconds); XOR2 adds start-time state. [P-xor][P-xor] [P-xor2][P-xor2] | Classic: literal big-endian uint32 seconds in every 12 B slot. CWhisper timestamp compression is separate from classic. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |
| Value codec | Cache: byte-granular Float64 XOR, repeated bits cost no value bytes. Disk: integer deltas only when Float64 reconstruction is bit-identical and within ±2⁵³; otherwise byte-stream-split raw float bits. Zstd only for body ≥128 B and smaller output; CRC32 covers body + interpretation. Sealed v9 also supports integer DoD and optional bounded decimal modes; proposed exact exponent-zero dispatch can reuse existing reader modes. [`cache/compact.go`][C-compact] [`internal/chunkstore/exact.go`][C-exact] [`internal/chunkstore/exact.go`][C-intgate] | Float→shared decimal scale→integer array codec. Zstd tried at ≥128 intermediate bytes, kept only if output ≤90%; levels 1–5 by rows. Lower precision can drop delta bits. Integer precision=64 does not undo float→decimal conversion. [V-decimal][V-decimal] [V-encoding][V-encoding] [V-int][V-int] | Default Zstd; ordinary fields use PLAIN/default dictionaries unless overridden. Float BYTE_STREAM_SPLIT is optional, disables dictionaries, and is not automatically the default. Op column has compression disabled. [T-writer][T-writer] [T-floatopt][T-floatopt] [T-parquet][T-parquet] | Exact Float64 XOR windows; no distinct integer counter codec. Mimir fork supports XOR/XOR2. [P-xor][P-xor] [P-xor2][P-xor2] | Classic: literal Float64 bits, no value compression. CWhisper uses its own bitstream; do not substitute its bytes/point for classic or for exact groups. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] [`vendor/github.com/go-graphite/go-whisper/compress.go`][C-compress] |
| Exactness boundary | Bit-exact codec for arbitrary Float64: NaN payload, ±0, subnormal and infinity survive. Integer shortcut has a reconstruction gate; otherwise raw bits take the normal path. Aggregates remain calculations under the existing writer's rules. [`internal/chunkstore/exact.go`][C-exact] [`internal/chunkstore/exact.go`][C-intgate] [`internal/chunkstore/exact_test.go`][C-exact-tests] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] This describes exact mode. Optional decimals use an explicit error/exception policy; exact dispatch still preserves every bit. | Not an arbitrary Float64 bit oracle: decimal conversion has finite precision, normalizes zero and supports the designated stale NaN; scale calibration may further change values. precisionBits=64 describes integer encoding only. [V-decimal][V-decimal] [V-encoding][V-encoding] | GreptimeDB matched all checked finite values in the replay. Arbitrary NaN-payload, signed-zero and subnormal fidelity is not verified by that sample. [T-floatopt][T-floatopt] [T-writer][T-writer] | Chunk-level float bit preservation; ingestion duplicate/staleness policy is a separate contract. [P-xor][P-xor] [P-ooo][P-ooo] | Classic pack/unpack uses Float64bits/frombits. Raw slot round-trip is bit-preserving; missing-slot responses and rollup arithmetic are separate semantics. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] |
| Index & discovery | Catalog is authoritative. trieIndex + nonempty file-list cache + FLC v2 selects mapped immutable FST + metadata + mutable overlay. Else trie/details or trigram route. Whole-key Pebble blooms help probes, not prefix enumeration. [`carbonserver/metricstore.go`][C-index] [`carbonserver/snapshot_fst.go`][C-fst] [`carbonserver/metricstore_mapped.go`][C-mapped] [`doc/pebble-storage/write-path.md`][C-write-doc] | Partition IndexDB on mergeset: name→TSID, ID→TSID/name, tag postings and per-day membership. Prefix-coded sorted byte items + Zstd; postings combined on merge. Graphite suffix/path lookup exists; exact go-carbon glob parity is not established. [V-index][V-index] [V-mergeset-codec][V-mergeset-codec] [V-graphite][V-graphite] | Optional inverted value→row-segment indices in Puffin blobs; external sort with memory/spill controls. Applier prunes row groups. Bloom targets have separate memory/spill accounting. Not a ready Graphite glob implementation. [T-inverted][T-inverted] [T-invertedread][T-invertedread] [T-bloom][T-bloom] | Sorted symbols/series and label-pair postings, intersected for selectors; no native Graphite glob equivalence established. [P-index][P-index] | go-carbon trie/trigram/file-list discovery over metric paths; .wsp itself contains samples and policy, not a global name index. [`carbonserver/metricstore.go`][C-index] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |
| Caches & byte budgets | Separate knobs for write cache, bounded striped state slabs, optional tail slots, Pebble block cache/memtables/table readers, query caches and conversion work. No single hard process budget. Prewarm already exists; pinned readers and spare capacity still cost RSS. [`internal/chunkstore/state_cache.go`][C-state] [`internal/chunkstore/state_slab.go`][C-slab] [`internal/chunkstore/state_slots.go`][C-slots] [`internal/chunkstore/store.go`][C-store] [`carbon/metadata_prewarm.go`][C-prewarm] | memory.Allowed defaults 60% detected/cgroup RAM (absolute override). tsidCache 37%; ID cache 1/16; name cache 1/10; data index 10%; mergeset index/normal/sparse 10/25/5% of Allowed. Separate capacities are not an enforceable sum/RSS cap. [V-memory][V-memory] [V-memlinux][V-memlinux] [V-storage-caches][V-storage-caches] [V-part][V-part] [V-mergeset-part][V-mergeset-part] | Write buffer min(RAM/8,1 GiB); metadata min(RAM/8,512 MiB) split ½ compact/½ decoded. Page ranges RAM/8; vector/selector/range each min(RAM/16,512 MiB); scan 25%. Compaction configurable but unlimited by default. Extra caches remain separate. [T-config][T-config] [T-cache][T-cache] [T-cache-size][T-cache-size] [T-task][T-task] | Mimir index-header/postings/result caches and estimated query-memory limits; see the dedicated section. [M-header][M-header] [M-limits][M-limits] | Same application cache knobs; kernel page cache holds file data. No shared Pebble block-cache/memtable budget in classic mode. [`cache/cache.go`][C-cache] [`doc/pebble-storage/write-path.md`][C-write-doc] |
| Admission, reclamation & cache generations | stateCache already Snappy-packs keys ≥256 B when savings exceed about 12.5%; pointer-free arena records + one retained decoded last-state per stripe. Epoch/pending rules preserve snapshots. Eviction is not RSS release; count decoded and reader-pinned bytes too. [`internal/chunkstore/state_cache.go`][C-state] [`internal/chunkstore/state_slab.go`][C-slab] [`internal/chunkstore/state_slots.go`][C-slots] [`internal/chunkstore/state_cache.go`][C-state-pack] [`internal/chunkstore/state_slab.go`][C-snappy] | Working-set current/previous caches promote hits and rotate ~30 min; whole-cache transition can overlap allocations. fastcache 512 buckets, 64 KiB chunks, small cache rounded to 32 MiB; 64 MiB mmap extents can stay pooled after logical eviction. [V-wsc][V-wsc] [V-fastcache][V-fastcache] [V-fastalloc][V-fastalloc] | WriteBufferManager tracks active + total. Flush at active≥½ global or total≥global; stall at global. Global rejection threshold 2×. Optional region override: flush½/stall1×/reject2× incl stalled requests. Memory reservations follow allocation lifetimes. [T-flush][T-flush] [T-allocation][T-allocation] [T-write-worker][T-write-worker] [T-config][T-config] [T-flush-worker][T-flush-worker] | Head lifecycle and index generations constrain refs; Mimir sparse headers can load symbols lazily. [P-head][P-head] [M-header][M-header] | Front-cache and input-log lifetimes still matter. Closing a metric file does not immediately evict the kernel's cached file pages. Exact physical RSS effects: not verified. |
| Retention & deletion unit | Per-archive circular ownership plus per-metric conditional deletion (ID/generation/revision checked). Expiration batches ≤512 candidates. v7 buckets use block-start write clock, sealed history bucket 0: locality only, not safely TTL-droppable windows. [`internal/chunkstore/admin.go`][C-delete] [`internal/chunkstore/expiration.go`][C-expire] [`internal/whisperio/continuation_blocks.go`][C-bucket] [`internal/whisperio/continuation_blocks.go`][C-append] | Retention watcher ~1 min detaches expired calendar-month partitions; references defer physical Drop. Merges remove expired blocks/rows in partial months. IndexDB participates in partition lifecycle. [V-retention][V-retention] [V-merge][V-merge] | TWCS finds expired SSTs and groups remaining files by max event timestamp. TTL can retire a file only when its newest event satisfies expiry; version/read references defer physical purge. Late data can enter old windows. [T-twcs][T-twcs] [T-picker][T-picker] [T-sstversion][T-sstversion] [T-timeparts][T-timeparts] | Whole time blocks expire under retention; Head/OOO and outstanding readers have independent lifetimes. [P-head][P-head] | Fixed ring archives overwrite slots on reuse; timestamp validates ownership. File deletion retires a metric. Query clips to now−maxRetention and chooses an archive covering requested age. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-align] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |
| Downsampling / rollups | Write-time schema archives: average/sum/last/max/min/first, XFF and buffered rollups through the same go-whisper state machine. Averages are not arbitrary repeated means; preserve source grid and completeness behavior. [`internal/whisperio/continuation_codec.go`][C-writer] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] | General age-based downsampling not found in inspected public storage. ScheduledDownsampling metrics here count dedup scheduling. Enterprise feature behavior: not verified; interval dedup is not aggregation. [V-partition][V-partition] [V-retention][V-retention] [V-dedup][V-dedup] | SQL reads and aggregation can be expressed by consumers; the replay uses ordered arrays per series. Scheduled age-based materialization and an equivalent Graphite XFF/correction policy are not verified. TWCS compaction itself is not a rollup. [T-twcs][T-twcs] | Rules emit separate series; Mimir validates against downsampled blocks. No native Whisper archive/XFF contract. [M-validation][M-validation] | Schema-defined coarser ring archives populated on writes; XFF gates propagation. No LSM background downsampling needed for classic files. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] |
| Compaction & write amplification | Hot partial groups and writer state are rewritten before Pebble flush/L0→levels compaction. Archive-chunk API separately emits merge operands, materializing at 4 deltas; merger rejects ≥32. Aggregate compaction-output pacer + adaptive debt/stall escape paths already exist. No universal WA ratio. [`internal/whisperio/continuation_blocks.go`][C-append] [`internal/whisperio/continuation_blocks.go`][C-group] [`internal/chunkstore/access.go`][C-merge] [`internal/chunkstore/chunk.go`][C-merger] [`internal/chunkstore/compaction_pacer.go`][C-pacer] [`internal/chunkstore/compaction_controller.go`][C-controller] | Balanced size/locality candidates (default 15 parts, minimum multiplier 1.7), bounded output/free-space checks; encoded blocks can pass through if compatible. Separate worker pools; no merge-output bytes/s token bucket found. Actual WA needs matched measurement. [V-partition][V-partition] [V-merge][V-merge] [V-streamwriter][V-streamwriter] [V-filestream][V-filestream] | TWCS has active/inactive policies, PK overlap/run selection, input/output limits; ordinary candidate default ≤16 inputs. Avoids repeated L1 history rewrites beside L0. Configured memory guard can limit compaction scratch. WA not quantified. [T-twcs][T-twcs] [T-twcs-options][T-twcs-options] [T-task][T-task] | Compatible nonoverlapping encoded chunks pass through; overlap may decode/re-encode. Mimir compactor uses 2/12/24-h ranges. [P-merge][P-merge] [M-compactor][M-compactor] | Classic slot overwrites + write-time lower archives; no LSM compaction. Compressed CWhisper may fold OOO/rewrite archives, so its amplification is not classic's. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] [`vendor/github.com/go-graphite/go-whisper/compress.go`][C-compress] |
| Durability · WAL & fsync policy | Two layers: inputlog records accepted cache points, group-commits fdatasync (default 1 s; loss includes encode/write cycle); Pebble mutations Sync at interval=0, NoSync + background sync for positive interval. Async WAL rotation avoids old-file fsync in ordinary commit critical path; one handoff maximum, sync/flush/close wait. Expiration batches deliberately NoSync. [`internal/inputlog/inputlog.go`][C-inputlog] [`internal/chunkstore/store.go`][C-store] [`internal/chunkstore/wal_sync.go`][C-wal] [`internal/chunkstore/expiration.go`][C-expire] [`internal/chunkstore/store.go`][C-wal-rotation] [`vendor/github.com/cockroachdb/pebble/options.go`][C-wal-options] | November source trace has no sample WAL before parts persist: compressed memory → fsynced parts → atomic part list, with default 5-s disk scheduling. Preflush loss remains a durability difference; equal release acknowledgement/crash-loss bounds are not established. [V-partition][V-partition] [VM2-durability][VM2-durability] | Measured v1.3.0 uses its native raft-engine WAL; the experiment sets 16 MiB segments, 32 MiB purge threshold, 1 s purge interval and no recycling. Equal acknowledgement/fsync/crash-loss behavior across engines is not established. [GT-release-config][GT-release-config] | WAL: 128-MiB segments, 32-KiB pages, CRC fragments, optional Snappy/Zstd; checkpoint and compressed Head snapshots accelerate replay. [P-wal][P-wal] [P-replay][P-replay] | Shared go-carbon inputlog can protect the front cache when configured. Classic pack/write is verified; full file fsync/ack policy is not verified here. Do not infer synchronous durability from WriteAt. [`internal/inputlog/inputlog.go`][C-inputlog] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] |
| Query path & read amplification | Find expands index; render uses membership→epoch/cache + consistent ReadView→catalog/revision on miss→cached/LSM manifest/header→descriptors/groups→cache overlay. ReadView avoids registered Snapshot's DB-mutex cost. Incremental mode can fall back to original files. L0 overlap, cold-cache/table-reader churn and persister reads increase work. [`carbonserver/fetchfromdisk.go`][C-read] [`internal/chunkstore/fetch.go`][C-fetch-view] [`internal/chunkstore/continuation_io.go`][C-io] [`carbonserver/metricstore.go`][C-index] | IndexDB postings→matching TSIDs→part metaindex/index→timestamp/value blocks→merge/dedup. Decoded part metaindices remain resident. Partition/part/block counts and cache misses determine fan-out; rawRows await conversion for search visibility. [V-index][V-index] [V-part][V-part] [V-header][V-header] [V-dedup][V-dedup] [V-partition][V-partition] | Read version + PK/time predicates + SST min/max/inverted/bloom pruning → projected Parquet pages/row groups → version-aware merge. 102,400-row default groups need not mean every query decodes all rows; sparse 35-point lookups still require measurement. [T-format][T-format] [T-writer][T-writer] [T-invertedread][T-invertedread] [T-cache][T-cache] [T-sstversion][T-sstversion] | Select chunk refs by time bounds; Mimir coalesces bounded chunk reads. Current go-carbon Fetch still filters after reading a full native range. [P-index][P-index] [M-chunks][M-chunks] | Find metric paths; select one retention; read circular file range, validate slot timestamps, merge unpersisted cache points. Many metrics imply many file/metadata lookups. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`carbonserver/fetchfromdisk.go`][C-read] [`carbonserver/metricstore.go`][C-index] |
| Concurrency & contention candidates | Receive shard lock, per-metric/striped writer locks, state-cache stripes, Pebble commit pipeline/table cache and memtable range-tombstone fragmentation. Fetch already uses ReadView to avoid Snapshot DB-mutex contention. Indexed-batch prefix deletes prefer point deletes to reduce reader serialization. Profiles needed to rank remaining costs. [`cache/queue.go`][C-queue] [`internal/chunkstore/continuation_codec.go`][C-update] [`internal/chunkstore/fetch.go`][C-fetch-view] [`internal/chunkstore/continuation_io.go`][C-delete-prefix] [`doc/pebble-storage/read-path.md`][C-read-doc] | CPU-sharded row staging; global in-memory conversion/merge channel = CPUs; separate small/big merge channels max(4,CPUs). No per-series rawRows mutex. Index/cache and merge competition still exist; comparative lock profile not verified. [V-raw][V-raw] [V-partition][V-partition] [V-index][V-index] | Dense time-series path has Series Arc/RwLock + map; workers dispatch writes/flush; alloc tracking, sorter limits and memory-manager permits control work. Bulk path differs. A measured lock-contention ranking is not verified. [T-series][T-series] [T-write-worker][T-write-worker] [T-flush-worker][T-flush-worker] [T-memory][T-memory] [T-inverted][T-inverted] | Head series locking, sharded replay; Mimir bounded streaming/query sharding. No equivalent-server concurrency benchmark here. [P-replay][P-replay] [M-shard][M-shard] | Same go-carbon receive/writer coordination; many independent files. Actual locking cost vs the shared-store baseline is not verified. [`cache/queue.go`][C-queue] [`doc/pebble-storage/read-path.md`][C-read-doc] |


### Buffer sizing and memory context

These are cost models and supplied memory categories, not measurements of equivalent engine populations. Let **N** be catalog size and **A** simultaneously queued series; they need not be equal.

| Quantity | Calculation / supplied value | Interpretation |
| --- | --- | --- |
| 960 s × 10 M series × 240 points | 2.4 billion queued points; one sample every 4 s | Illustrative active population. A 960 s window is not a sample count. |
| Raw-row memory model | 2.4 billion × 48 B = **107.29 GiB** | VM's raw staging representation, before indices/scratch; VM normally converts it within seconds. |
| Existing compact write-cache category | Approximately **12 GiB** for the corresponding supplied workload category | 5.37 B/queued point if the illustrative population were exact; includes overhead and is not a measured codec rate. |
| Compact payload limits | Regular repeats approach 1 B/point; worst-case bound 19 B/point | 2.24–42.47 GiB for 2.4 billion points, excluding 56 B buffers, names/items, unused capacity, queues, log entries and in-flight decoding. |
| 35 points × 10 M series | 15.65 GiB at 48 B/raw row; supplied compact-cache category approximately 9 GiB | The active count is illustrative, not a measured cardinality for the short-batch workload. |
| Representative supplied RSS categories, D1 / D2 / D3 | 399 / 242 / 226 GiB | Different production populations, not the small replay fixtures. |
| Metadata category, D1 / D2 / D3 | 72 / 26 / 19 GiB | Charged records, physical arenas and RSS are different quantities. |
| Pebble block-cache category, D1 / D2 / D3 | 51 / 83 / 85 GiB | Pure-Go manual allocation uses byte slices; a heap total may already include this memory. |
| Per-series arithmetic | Saving 100 B at A=10 M gives 0.93 GiB; 1 KiB at N=10 M gives 9.54 GiB | A small handle optimization cannot explain a multi-hundred-GiB process. |

GiB uses 2³⁰ bytes. The catalog-membership filter is capped at 64 MiB and is not a plausible explanation for a very large residual heap. Do not add overlapping heap/cache categories or projected savings. Mapped files, retired reader-pinned arenas, stacks, unused allocator capacity and query scratch still contribute to resident memory. [G-manual][G-manual] [G-membership][G-membership]

## Semantics differences

| Behavior | go-carbon: exact baseline | VictoriaMetrics | GreptimeDB | Prometheus TSDB | Whisper: classic |
| --- | --- | --- | --- | --- | --- |
| Duplicate / equal timestamp | Graphite correction: last write wins for a slot (not max value). alignPoints keeps the last value in an interval; stable ordering preserves equal-time arrival order. Continuation patch/sidecar rules preserve writer behavior. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-align] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper-update] [`internal/whisperio/continuation_codec.go`][C-writer] | Dedup selects last timestamp in interval; exactly equal timestamps select maximum value, with stale-NaN handling. This describes dedup/merge behavior, not an assumed immediate rawRows rewrite. [V-dedup][V-dedup] [V-merge][V-merge] | Primary-key/time, sequence and operation take part in version-aware merging. Last-row versus LastNonNull mode defaults and equal-key winner details are not verified by this duplicate-free replay. [T-format][T-format] [T-sstversion][T-sstversion] [T-picker][T-picker] | Conflicting same-timestamp writes are rejected/dropped; OOO handling is separate. This is not Graphite last-arrival correction behavior. [P-append][P-append] [P-ooo][P-ooo] | Last value for an aligned interval on classic writes. [10 then 3] at the same timestamp yields 3; VM dedup's ordinary finite-value result is 10. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-align][V-dedup][V-dedup] |
| Out-of-order & late corrections | Arrival order retained in cache, writer sorts/alignment; compressed continuation supports virtual OOO sidecars and maintenance. DirectLateWrites can patch/fold in place; exact path depends on options. uint32 timestamp range enforced; older points follow archive/now rules. [`cache/compact.go`][C-compact] [`internal/whisperio/continuation_codec.go`][C-writer] [`internal/whisperio/continuation_blocks.go`][C-append] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper-update] | Rows sorted into parts; overlapping/extendable blocks take merge slow path. Retention filtering applies. Exact ingest late/future admission limits are not verified in the report. [V-raw][V-raw] [V-merge][V-merge] [V-retention][V-retention] | Event-time partitions accept late data into older windows; sequence/op reconcile versions. Exact configured late/future admission/rejection limits are not verified. [T-timeparts][T-timeparts] [T-twcs][T-twcs] [T-format][T-format] | Separate OOO Head/chunks and admission checks; allowed window is configurable. Native Whisper correction/rollup parity is not established. [P-ooo][P-ooo] [P-append][P-append] | Classic writes aligned slots directly within age/retention selection. Compressed CWhisper has separate OOO machinery; these are not identical write costs. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper-update] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-align] [`vendor/github.com/go-graphite/go-whisper/compress.go`][C-compress] |
| Missing value vs NaN | Presence bitmap distinguishes absent slot from stored Float64 NaN at codec layer; Graphite fetch initializes missing values to NaN, so API output may not expose that distinction. No invented zero-fill. [`internal/chunkstore/exact.go`][C-exact] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`internal/chunkstore/access.go`][C-merge] | Decimal conversion supports designated stale NaN, not arbitrary NaN payload fidelity. General missing/ordinary-NaN query semantics are not verified here. [V-decimal][V-decimal] [V-dedup][V-dedup] | Arrow/Parquet field structures are described; SQL NULL vs NaN and how each aggregation treats them are not verified. [T-format][T-format] [T-flat][T-flat] | XOR chunks preserve stored NaN bits; codec fidelity is separate from query missing/stale interpretation. No equal-API replay was run. [P-xor][P-xor] | Timestamp mismatch means empty slot; Fetch initializes gaps to NaN. Stored raw float bits still permit NaN values. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] |
| Rollup aggregation | Writer's average/sum/last/max/min/first over known grid positions; buffered compressed rollups remain visible and must survive conversion. Corrections must propagate to coarser archives atomically with state. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] [`internal/whisperio/continuation_codec.go`][C-writer] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] | Dedup chooses a representative; Enterprise age-downsampling retains the last sample. Public stream averages use sum/count, without a Whisper XFF grid or atomic archive correction propagation. [V-dedup][V-dedup] [V-downsample][V-downsample] [VM2-average][VM2-average] | Consumers can issue SQL aggregation queries; persistent window scheduling and Graphite-equivalent correction propagation/completeness remain not verified. | Rules emit separate series; Mimir validates against downsampled blocks. No native Whisper multi-archive propagation contract. [M-validation][M-validation] | Write-time lower-archive aggregation using configured method. Do not replace sum by interval dedup or average by unweighted averages of averages. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] |
| xFilesFactor (XFF) | Known source slots / expected slots must meet XFF before propagation; lower-archive grid and method remain writer-owned. Example: one of four known, XFF=.5 → no rollup. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] [`internal/whisperio/continuation_codec.go`][C-writer] | Graphite XFF compatibility is not verified; dedup count is not an XFF completeness check. | Equivalent SQL completeness rule and missing-grid denominator are not verified; must be specified before claiming parity. | No native Whisper archive/XFF contract identified; equivalent rule completeness/missing-grid behavior would need explicit design. [M-validation][M-validation] | Same propagation test in classic archives. NaN arithmetic is separate from whether a slot timestamp counts as known. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-propagate] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-aggregate] |
| Staleness markers | Exact codec preserves all float bit patterns; Graphite missing-slot NaN is not a separately verified Prometheus staleness state machine. No special stale-marker interpretation verified in this writer. [`internal/chunkstore/exact.go`][C-exact] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] | Designated stale NaN handled specially in decimal conversion and dedup. Full query staleness/lookback behavior is not verified in the report. [V-decimal][V-decimal] [V-dedup][V-dedup] | Protocol mapping of stale NaN, SQL NULL and deleted row: not verified. | not verified | Raw bits can retain NaN; fetch represents gaps as NaN. Special stale-marker interpretation is not verified. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-floatbits] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] |
| Name / schema model | Dotted Graphite names → catalog ID + schema retentions/aggregation/XFF; discovery preserves glob/directory/leaf behavior. Labels are not substituted for name identity. [`internal/chunkstore/types.go`][C-identity] [`carbonserver/metricstore.go`][C-index] | Metric names + tag postings; Graphite path lookup also implemented. Equal syntax and missing-data discovery semantics with go-carbon are not verified. [V-index][V-index] [V-graphite][V-graphite] | Region/table columns, encoded PK/tag columns, timestamp and fields. Mapping dotted names to SQL tags/PK is a schema choice, not an automatic equivalent. [T-format][T-format] [T-flat][T-flat] [T-series][T-series] | Label-set identity and label-pair postings; reusable refs are scoped to the live Head. Native Graphite path/glob parity is not established. [P-append][P-append] [P-index][P-index] | Dotted paths → .wsp paths; policy per file; application index expands globs. [`carbonserver/metricstore.go`][C-index] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |
| Retention boundary & deletion visibility | Whisper Fetch clips to [now−maxRetention,now], selects resolution covering requested age; exact endpoint alignment follows writer. Metric expiration checks revision before delete. v7 hour buckets cannot be dropped solely by wall-clock age. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`internal/chunkstore/expiration.go`][C-expire] [`internal/whisperio/continuation_blocks.go`][C-bucket] | Whole-month detach plus row filtering inside partial months; reader refs defer physical removal. Exact query endpoint and retention cutoff inclusivity are not verified here. [V-retention][V-retention] [V-merge][V-merge] | Half-open event-time windows; expired SST only if newest event passes TTL; version refs defer physical purge. Exact TTL equality boundary and query cutoff semantics are not verified. [T-timeparts][T-timeparts] [T-picker][T-picker] [T-sstversion][T-sstversion] | Time-block retention with separate live Head/OOO lifetime; exact equality/query-cutoff parity with Whisper not established. [P-head][P-head] [P-ooo][P-ooo] | Circular reuse validated by stored timestamp; fetch age decides archive and resolution. Not a SQL BETWEEN boundary contract. [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-fetch] [`vendor/github.com/go-graphite/go-whisper/whisper.go`][C-whisper] |


## Exactness: signed zero, subnormals and NaN payloads

The baseline exact codec compares `(timestamp, Float64bits(value))`. This supported byte-for-byte value verification during conversion and replication, including cases ordinary numeric equality misses. It does not mean physical SST files are byte-identical after compaction. The optional decimal policy permits a different numeric contract; its precision/exception checks remain separate from exact packing.

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
| D3: total | 5.7 | Busy-worker occupancy, not CPU consumption. |
| D3: tail_read | 2.1 | 36.8%; storage lookup/decode path and waits, candidate append saving. |
| D3: commit | 1.6 | 28.1%; append records still require commits. |
| D3: decode | 0.8 | 14.0%; includes writer merge/rollup logic, not only decompression. |
| D3: encode | 0.5 | 8.8%; appended payloads still need encoding. |
| D3: route/load/lock | 0.6 | 10.5%; identity/state/coordination remain. |
| D3: unassigned/rounding | 0.1 | 1.8%; named stages sum to 5.6. |
| D1: total / commit | 14.5 / 12.0 | Commit is 82.8%; all other work is 2.5. |
| D1: updates and points | 30,000 updates/s × 33 points | 990,000 points/s; mean total/commit worker time ≈483/400 µs per update. |

Stage counters sum worker wall time, including IO and locks. Total is recorded every update; other stages are sampled 1/8 and scaled. These production observations are separate from the short local engine replay. [Stage definitions][C-timings]

| Zero-replacement-cost counterfactual | Arithmetic | Limit |
| --- | --- | --- |
| Remove all D3 tail_read | 5.7 / (5.7 − 2.1) = **1.58×** | Ideal worker-capacity ceiling, not measured CPU/throughput. |
| Remove all D3 tail_read + decode + encode | 5.7 / (5.7 − 2.1 − 0.8 − 0.5) = **2.48×** | Unrealistic as a design promise: append and rollups still process samples. |
| Remove every D1 non-commit stage | 14.5 / 12 = **1.21×** | At most 17.2% occupancy reduction if commit is unchanged. |
| Halve D1 commit | 14.5 / (14.5 − 6) = **1.71×** | Hypothetical; commit includes WAL/memtable work, waits and synchronization. |

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

**Measured bounded prototype:** folding every eight append calls (`fold8`) reduced foreground CPU **56–68%** and logical WAL bytes **67–72%** in isolated 30/225-point update screens. Uncached reads became more expensive because they replay pending updates and reconstruct writer/rollup state. These are prototype results, not the engine comparison below or a production rollout. End-to-end adoption is not established by this screen; background fold debt, activity/no-op metadata, format/transfer compatibility and concurrent read costs remain gates.

**Recommendation:** prioritize true expiry cohorts/windows; retain bounded append/fold as an experiment rather than adopting an unbounded per-metric append log. Sample expiry and catalog garbage collection are different lifecycles. A closed window may be removed only when all archive policies, late corrections and reader/writer references permit it.

## VictoriaMetrics

The November source review pins **`5858c66de15d18fec31ef0b6ffa580bbaad2870e`**, independently of the October release replay. VM resolves metric identity once and carries TSIDs through sharded raw staging, tests constant/linear integer arrays before generic encoding, and reuses historical query intervals. Those narrow ideas transfer without replacing Pebble or re-crediting existing packing, metadata and cache work. [Identity][VM2-identity], [integer dispatch][VM2-codec], [query reuse][VM2-query].

| Area | Verified mechanism and incremental consequence |
| --- | --- |
| Ingest / identity / indexes | CPU-sharded **48-B raw rows** become compressed parts; numeric TSIDs and prefix-coded mergeset avoid repeated identity work. go-carbon already has durable IDs/FST and compressed cache points; copying raw staging has **0 established saving**. Reuse its existing hash at quota accounting. [storage][VM2-identity] [index codec][V-mergeset-codec] |
| Values / timestamps | Constant/affine arrays precede varints/Zstd. go-carbon can use existing exponent-zero modes for exact integers, then select one delta candidate. Dense sealed timestamps already have **zero per-point payload**; shared timestamp streams add no dense-case gain. [dispatch][VM2-codec] [classifier][VM2-classifier] |
| Compression | Part/index coding (first pass −5; later −5…3) differs from integer-stream levels 1…5. Independent 4,096-point SST screen with outer Zstd: stronger inner coding saves **11.31% gauge /3.54% rounded /2.77% random-float bytes**, but counter bytes grow **1.84%**; gauge compression CPU **5.31→11.47 ns/pt**. A uniform stronger inner codec is rejected; cold-family selection needs design/flags. [levels][VM2-levels] |
| Compaction / expiry / rollups | Compatible compressed blocks pass through; event-month partitions retire after references drain. Pebble already compacts opaque values. Write-hour buckets are not expiry domains. Enterprise age-downsampling keeps the **last** sample; sum/count averages do not reproduce Graphite XFF. **0 equivalent-contract new rollup/compaction saving.** [merge][V-merge] [retention][V-retention] [downsampling][V-downsample] |
| Precision / dedup | VM shared scale maps `[0.001,0.002,1e18]` to `[0,0,1e18]` in the probe and normalizes −0. Equal-time dedup chooses max, while Graphite correction is last arrival (`10 then 3` must stay **3**). Keep bounded decimal fallback and existing correction semantics. [decimal][V-decimal] [dedup][VM2-dedup] |
| Query / WAL / limits / network | Historical interval reuse needs a mutable tail covering coalescing+lateness and invalidation for older corrections. Query-byte reservations bound counted arrays, not RSS. VM's traced sample path has no sample WAL before part persistence; dropping either go-carbon log changes durability. Numeric transfer, SST checkpoints and stream handoff already exist: **0 new network saving**. [cache][VM2-query] [limits][VM2-limits] [parts][V-inmem] |

Source/measurement receipts: `tsdb-vm-RESULT.md`, `tsdb-vm-final-oracle.log`, `tsdb-vm-levels-independent.log`, `tsdb-vm-vm-probe.log`. Integer screens preserve exact bits; source decimal conversion does not. Quota hashing attributed to the accounting stack reaches **0.655–0.703 core** in two supplied captures; this is a removal ceiling, not a measured whole-worker win. Codec pool retention is only **~0.58 MB/context**: a conditional 96→8 idle contexts saves **~51 MB**, far below the existing metadata opportunity.

## Prometheus TSDB and Grafana Mimir

Pins: Prometheus **`b0c3a30ab2a09b88e64d876017a0134aa2f38f40`**, Mimir **`1036b38cbd8c8e4bdeff829d6d99fa59bddb5928`**. Mimir's vendored Grafana Prometheus fork is **`v1.8.2-0.20261029124111-b9c2d71e13bd`**; it is not the standalone Prometheus revision. The inspected Mimir build uses stringlabels; optional dedupelabels are not its default. [vendor pin][M-modules] [build][M-build].

| Area | Verified mechanism and incremental consequence |
| --- | --- |
| Head / identity / indexes | Reusable series refs skip repeated label lookup. Source-layout mirrors: **184 B Prom /200 B Mimir memSeries**, active memChunk **40 B**, XOR object **32 B**, appender **40 B**, initial payload capacity **128 B**, before labels/maps/postings. This is not full retained bytes/series. Go-carbon's **56-B base /80-B Gorilla** state is already compact. [append][P-append] [Head][P-head] |
| Counter / decimal / float codec | XOR uses timestamp DoD and Float64 bit windows, with no separate integer codec. At 120 points: counter **2.333 base /1.092 Gorilla /1.700 XOR B/pt**; decimal gauge **7.500 /2.975 /6.725**. Stock XOR is not a new cache win. XOR2 joint repeat control has a **0.125 B/repeated-point** theoretical increment, but at 4,096 constant points this pinned XOR2 uses **6,985 B vs 1,040 B XOR** because start-time deltas begin at sample 127. [XOR][P-xor] [XOR2][P-xor2] |
| Blocks / reads / compaction | Time-bounded chunk refs prune reads; Mimir streams bounded/coalesced chunk ranges. Go-carbon prunes native blocks but still decodes every group before filtering. The new design is range-aware reads **inside** packed ranges. A 4,096→256-point decimal screen cuts decode **47.24→4.53 µs**, allocation **99,900→6,211 B**, but corpus size grows **5.400→5.807 B/pt (+7.5%)**. Opaque pass-through compaction already exists. [index][P-index] [reader][M-chunks] [merge][P-merge] |
| Caches / metadata / limits | Deterministic cache identities motivate sorting render time-range groups. Mimir's historical extents and byte limiter motivate tiles and preallocation reservations. Sparse/lazy index headers are a design alternative to loading cold writer metadata: a stride-32 model reduces 1M offset entries **8.0→0.25 MB**, excluding overlay/name cost. Mapped pages still count in RSS. [cache keys][M-cachekey] [extents][M-extents] [headers][M-header] [limiter][M-limits] |
| WAL / restart / compression | **128-MiB segments**, **32-KiB pages**, CRC-fragment WAL; optional Snappy/Zstd, checkpoints, sharded replay and compressed Head snapshots. Go-carbon already has compact logs/dumps, clean fences and restart pacing. Cold-level Zstd and negotiated tails remain the separately measured work above. **0 additional restart/network saving established.** [WAL][P-wal] [replay][P-replay] |
| Rollups / OOO / dedup | Mimir rules create separate series; block validation rejects downsampled blocks. OOO storage, HA replica selection and redundant-block elimination are different from Graphite last-arrival corrections, native archives and XFF. Keep current semantics and durability replicas. [OOO][P-ooo] [validation][M-validation] |

Receipts: `tsdb-prom-mimir-{RESULT.md,codecs.txt,sealed.txt,index.txt,renderkey.txt,series-layout.txt}`. Current-code screen produced **2 /5 different keys** for identical **2 /5-range** requests over 10,000 calls. Removing those variants fixes identity; it does not predict fleet cache savings. Query-byte and historical-tile benefits in the combined table remain explicitly modelled, with no equivalent server replay here.

## OpenObserve

Pin **`04f6d0b8a9de66d011f7b83033d42b711565bc4c`**. Metrics follow Arrow ingest batches → hash/time-sorted Parquet (or configured Vortex) → closed-hour `.midx` sidecars with a direct numeric reader. MIDX stores samples **beside the parent file**; its codec payload is not total OpenObserve disk size. Borrow decoder ownership, time directories and exact exception coding within Pebble's existing authoritative sample store. [writer][OO-writer-finish] [query entry][OO-block-path].

| Area | Verified mechanism and incremental consequence |
| --- | --- |
| Ingest / identity | Org/stream→schema→hour Arrow batches avoid per-series mutable trees; hash/time/value alone are **24 raw B/pt**, before labels. Block-local 1/2/4-B dictionaries and bounded interning are not durable global IDs. Existing compact points, registry and metadata branches already cover the useful sharing; **0 saving from replacing them with Arrow**. [memtable][OO-memtable] [dictionary][OO-compact] [interning][OO-interner] |
| Metadata / time pruning | ≤8,192-row blocks, five separately Zstd-1 varint directory columns, derived neighboring offsets/counts, **40 parsed B/block**, **32-B versioned trailer**. New go-carbon target: first/last group fences and a range API. Sixteen 4,096-point groups need only **128 raw B** of uint32 bounds per 65,536-point range; distribution/consistency costs remain unknown. [directory][OO-directory] [pruning][OO-time-prune] [trailer][OO-trailer] |
| Counters / gauges / decimals / timestamps | Constant, sparse-packed integer delta, exact ALP+raw-bit exceptions or Gorilla; timestamps constant-step or GCD/RLE/DoD. Current integer/Zstd wins the linear counter **42 vs 47 B** and sparse gauge **43 vs 129 B** screens. Exact decimal exceptions are useful: 4,096-point blocks with 1%/5% exceptions **2,497/5,139→1,190/1,615 B**. This needs a new version, not a format-free patch. [selection][OO-samples] [ALP][OO-alp] [packing][OO-pack] |
| Decoder / caches | Reusable reader state avoids allocating intermediate points per block; returned borrowed data must be consumed before reuse. Existing response singleflight does not coalesce distinct response keys' cold metadata loads. Shared immutable metadata loads must respect caller epochs/read views/cancellation; optional cache fills can be scan-resistant. [reader][OO-reader] [samples][OO-samples-decode] [flights][OO-singleflight] [admission][OO-label-cache] |
| Compression / compaction / corrections | Snappy Arrow WAL, Zstd Parquet, native MIDX codecs; no Pebble L0–L6 equivalent. Closed hours merge late fragments before rewriting the base. A v9-specific correction-fragment design could defer thawing **32 ordinary groups /13,404 value B** for a measured 4,096-point decimal range; a proposed **32-B value fragment** excludes keys/rollups/folding. **No new generic compaction saving**. [WAL][OO-wal] [Parquet][OO-parquet] [late files][OO-late] |
| Rollups / WAL / network / limits | SQL date bins and first/last ordering do not imply Graphite XFF; automatic downsampling is enterprise-gated and equal-time duplicates can remain. Packed segment files/replay already map to existing shared SSTs/recovery work. Query reservations and scan admission are gaps, not a reason to replace existing logs or indexes. **0 additional network/restart gain measured.** [downsampling][OO-downsample] [gate][OO-downsample-gate] [duplicates][OO-duplicates] [reservations][OO-query-pool] |

Receipts: `tsdb-openobserve-{RESULT.md,methods.txt,codec-summary.json,decode-summary.json}`. Extracted codec modules passed **28 tests**; synthetic 128/4,096-point comparisons preserve timestamp/value bits and price a hypothetical 16-B go-carbon envelope. Original reusable-decode screen: **70.6–99.1 KB→16–45 B/block**, **3→1 allocations**, elapsed **−20–27%**. Integration is assigned to the `decode-reuse` session; this document does not claim a completed implementation.

## What we borrow

The numerical screens are supplied research measurements, not implementations by the documentation session. VM full-encoder CPU uses fixed-work process user+system time in A/B/B/A order; Prometheus/Mimir and OpenObserve short decode timings are elapsed. Shared M4 Max, Go 1.27.0, vendored dependencies, `-p 4`; synthetic family proportions do not forecast production entropy or CPU. The supplied five CPU/heap snapshots were rechecked locally with `go tool pprof -top`; retained receipts are `tsdb-parity-doc-snapshot-{1..5}-{cpu,heap}.txt` and `tsdb-parity-doc-snapshots.json`. Supervisor smaps/IO do not establish worker RSS; quoted worker sizes follow contemporaneous worker metrics and the supplied cost inventory.

One ranked list combines the three November studies with the older proposals. Implementation is delegated to eight sibling sessions: **sealed-encode, sealed-alp, decode-reuse, range-reads, query-budget, render-cache, ingest-identity, md-admission**. Their `results/<name>-RESULT.md` paths below identify pending handoffs, not completed results. This change is documentation only. Ranks favor small, high-confidence implementation candidates first, then measured memory/disk opportunities, then conditional designs. **P** = supplied production evidence analysed locally; **L** = local synthetic measurement; **E** = estimate/scenario. Effort is estimated engineer-days for implementation and local qualification. Rows use different resources/populations and are not additive. “Done” describes the stated implementation/canary scope, not fleet adoption.

| Rank / technique | Systems | go-carbon target | Gain + unit | Effort | Risk | Confidence | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1. Carry resolved identity through quota accounting | VM, Prometheus/Mimir | Pass cache/stream xxh3 hash through admission into counter stripes; retain exact full-name namespace resolution and reload rules | L primitive, 192-B name: **14.48→0.388 ns/op** hash→scalar reuse (elapsed). P attributed removal ceiling **0.655–0.703 core** in two captures, not server savings | 1–2 d | Low–medium; stripe/reload parity | High source; medium net gain | **in progress: session ingest-identity, results/ingest-identity-RESULT.md** |
| 2. Canonical render-cache time-range groups | Mimir | Sort independent groups in `getRenderCacheKeyAndSize`; preserve ordered targets, duplicate targets, expressions, format and store epoch | L current-code screen: **2/5 distinct keys** for identical **2/5-range** requests over 10,000 calls; expected canonical result **1 key**; workload return unmeasured | 1–2 d | Low–medium; response identity | High defect; medium return | **in progress: session render-cache, results/render-cache-RESULT.md** |
| 3. Exact constant/linear dispatch before generic encoding | VM; OO constant dispatch | Reuse existing exponent-zero constant/linear decoder modes after bit-verified integer validation; no new format or flag | L supplied prototype, 4,096 pt: constant **40→19 B**, **7.90→1.10 CPU ns/pt**; linear **42→22 B**, **8.77→1.17 CPU ns/pt**; **84.6–94.1 kB→56 allocated B/block** | 1–2 d | Low–medium; signed zero, bounds, fallback | High fixture; family share unknown | **in progress: session sealed-encode, results/sealed-encode-RESULT.md** |
| 4. Select one integer delta candidate | VM, OO | Classify during exact preflight; compress one stream using existing flags | L supplied prototype: variable counter **10.83→7.58 CPU ns/pt**, **105→65.6 kB allocated/block**, **+2 B/block**; gauge **19.18→10.09 ns/pt**, size unchanged. Reset screen **4,144→4,176 B** | 1–3 d | Medium; raw length need not predict compressed size | High fixture correctness; medium size transfer | **in progress: session sealed-encode, results/sealed-encode-RESULT.md**; size-regression gates required |
| 5. Reuse sealed decode buffers | OO | Borrow bounded decoder/point scratch within packed read/probe; consume before reuse; retain owned-output API | L supplied prototype, 4,096 pt: **70.6–99.1 kB→16–45 allocated B/block**, **3→1 alloc**, elapsed **−20–27%**; integrated CPU/p99 unmeasured | 2–4 d | Medium; lifetime/corrupt-input recovery | High prototype; server return unmeasured | **in progress: session decode-reuse, results/decode-reuse-RESULT.md** |
| 6. Lossless metadata packing | VM, GreptimeDB | Existing pointer-free state slabs + Snappy; bounded decoded acceleration | P canary **2,146→1,642 B/entry**; separate total-cache **42.9→33.3 GB**, RSS **224→203 GB** | 0 new | Medium; coverage/hit CPU | High supplied canary; no universal RSS ratio | **done** in source, supplied one-store canary; broader scope unverified |
| 7. Compact mutable point payload | VM, Prometheus/Mimir, OO | `cache-gorilla` `e8560ed7` | L **5.10→1.58 retained B/pt**; state **+24 B/series**; model **+0.085 core at 3M pt/s** | Existing branch | Medium; random floats, snapshot tails | High fixture; canary pending | **local branch**; supplied one-store canary pending |
| 8. Compact cache bookkeeping / staged rows | VM, OO | `cache-items` `1cacb2c5` | L **660→528 B/metric**, **32→24 B/staged pt**; scale **−660 MB/5M metrics** | Existing branch | Low–medium; lifecycle/map occupancy | High fixture; host RSS unmeasured | **local branch**; branch tip is outside the documentation base ancestry |
| 9. Share names / template repeated metadata | VM, GreptimeDB, OO | `name-registry` `35e01a1a`, `metadata-compact` `6c5c63f5` | L **−167 B/name** mixed fixture; Snappy→template **720→565 logical /785→627 heap B/entry**; gains overlap | Existing branches + integration | Medium–high; identity/generation ownership | High local bytes; ID sharing incomplete | **local branch**; default off, IDs alone lose with independent registry |
| 10. Exact cold packing and time-bucket locality | VM, GreptimeDB, OO | Existing v7/v9 continuation layout | P fully compacted **1,559.3→1,167.2 GB (−25.1%)**, **−24.5%/active series** | 0 new code | Medium–high; one-way layout, rewrite debt | High supplied comparison; population matched, not identical keys | **done** in source and supplied canary; only bucketed cold history eligible; most captured stores v6 |
| 11. Cold-level outer Zstd | VM/OO lifecycle policies | `pebble-zstd` `dc2c98ef`, optional L5/L6 | L **−3.57% v7 /−10.62% v9 SST**; v9 cache-miss point p50 **108→223 µs** | Existing branch | Medium; read-miss CPU/GC | High fixture; production transfer untested | **local branch**, opt-in/restart; design/measurement only here |
| 12. Aggregate query-byte reservations | VM #7, Prom/Mimir #4, OO #4 | Reserve estimated expanded arrays/scratch before fetch; release on cancel/error; retain request caps | E **8×10k×5,760×16 B =7.37 GB** demand; **2 GiB** shared cap bounds this category to **≤2.15 GB**; **0 observed RSS saving** | 3–6 d | Medium; underestimation/admission latency | High arithmetic; low peak incidence evidence | **in progress: session query-budget, results/query-budget-RESULT.md**; needs explicit policy/configuration |
| 13. Range-aware reads within packed groups | OO #2, Prom/Mimir #3 | Typed range API + group first/last directory, circular/off-grid boundary checks; preserve full-block writer API | E **16→1 decoded group (−93.75%)** for narrow window in 65,536 pt, **128 raw B** directory. L 256-pt subchunks decode **47.24→4.53 µs** but **+7.5% encoded B/pt** | 8–15 d | High; consistency, boundaries, wide reads | High local tradeoff; workload selectivity unknown | **in progress: session range-reads, results/range-reads-RESULT.md**; directory/version and vendor API design |
| 14. Exact decimal exceptions | OO | New sealed candidate retaining existing integer/BSS/Zstd candidates | L 1%/5% exceptional 4,096-pt decimal blocks **2,497/5,139→1,190/1,615 B**; **−52%/−69%** exact bytes, no SST ratio | 6–10 d | Medium–high; version/encode CPU | High size screen; family share unknown | **in progress: session sealed-alp, results/sealed-alp-RESULT.md**; durable-format design |
| 15. Historical time tiles + mutable tail | VM #4, Prom/Mimir #5, OO #8 | Archive/policy/content identity, old-correction invalidation; mutable horizon ≥coalescing+lateness | E 24-h/60-s refresh, 960–1,800-s tail: **97.85–98.82% fewer history slots fetched**; 1-h tail gives **95.8%**. Serialization/copy/NIC remain | 8–15 d | High; late writes, restore, XFF | High mechanism; low net-return evidence | **in progress: session render-cache, results/render-cache-RESULT.md**; mutable horizon must include current coalescing |
| 16. Batch observational counters | VM | Unlimited namespaces only; preserve existing finite-quota stripes/folding bound | L batches 8/64 mean **87.5%/98.4% fewer counter calls**; E host **0.073–0.265 core** assumes 50% eligible/75% cheaper; not additive to rank 1 | 2–4 d | Medium; resets/report lag | High call count; low host estimate | **proposed**, requires measured eligible fraction |
| 17. Scan-resistant optional metadata admission | OO | Cap query-triggered fills; retain mandatory writer identity/state | E 1M one-use names avoid **0.93–2.54 GB logical churn**; fixed budget need not reduce heap | 3–5 d | Medium; useful cold-query hit rate | High mechanism; scan rate unknown | **in progress: session md-admission, results/md-admission-RESULT.md**; optional fills only |
| 18. Metadata singleflight across response keys | OO, Mimir | Share immutable same-identity/epoch reads; separate caller views/cancellation | E 16 coincident misses→1 load (**−93.75% redundant loads**); incidence unmeasured; full-response flight already present | 3–5 d | High; snapshots/cancellation/writer waits | High mechanism; low measured payoff | **proposed**, needs ownership proof; related session md-admission, results/md-admission-RESULT.md |
| 19. Stronger inner compression by value family | VM | Cold sealed gauge/float candidate with fast hot path and outer Zstd retained | L outer-Zstd SST **−11.31% gauge /−3.54% rounded /−2.77% float**, counter **+1.84%**; gauge CPU **5.31→11.47 ns/pt** | 2–3 d | Medium; extra CPU | High fixture; low whole-store estimate | **in progress: session sealed-encode, results/sealed-encode-RESULT.md**; uniform default rejected by supplied screen |
| 20. Sparse/lazy cold metadata | Prometheus/Mimir | Independently checked read-only records + sparse seeks + mutable overlay | L/E 1M offsets **8.0→0.25 MB** at stride32; E 25% cold logical data **3.0–24.1 GB** before overlay/index; RSS may retain mapped pages | 10–20 d | High; checkpoints/lifetimes | Low cold-fraction evidence | **proposed**, design only; current FST already mmap |
| 21. Late correction fragments over sealed base | OO; earlier VM append idea | Bound ordered corrections/folding with atomic archive+rollup state | E one 4,096-pt decimal base thaws **32 groups /13,404 value B** vs proposed **32 value B fragment**; excludes keys/rollups/folding | 10–20 d | High; ordering/debt/new format | High thaw behavior; low end-to-end return | **proposed**, design only; deduplicates prior generic append/fold recommendation |
| 22. True sample-expiry cohorts | VM, GreptimeDB | Event-time + archive-policy expiry ownership, readers/reference fence | E replace many per-metric tombstones with cohort retirement; **0 quantified new bytes/CPU** | 15–30 d | High; late data/cross-archive lifecycle | High gap; return unmeasured | **proposed**, design only; write-hour buckets cannot be TTL-dropped |
| 23. Bounded append/fold | VM, GreptimeDB, OO | Preserve correction order, writer state, read bounds and fold debt | L old fold8 screen foreground CPU **−56–68%**, logical WAL **−67–72%**; uncached reads cost more | 10–20 d | High; format and backlog | High isolated screen; low end-to-end evidence | **proposed**, isolated prototype only; end-to-end behavior unqualified |
| 24. Negotiated restart/tail codec | VM, Prometheus/Mimir, OO | `restart-codec` `9974e2e4`; legacy peers retain fallback | L tail **210.4→9.55 B/pt**, admission **639→480 ms**; stream dump **5.88→2.17 B/pt**, encode **+40%** | Existing branch | High; old-reader crash rollback | High local result; production restart unproved | **local branch**, experimental opt-in dumps; no new flags/formats here |
| 25. XOR2 joint repeat bit | Prometheus/Mimir | Incremental Gorilla control only; preserve current integer/decimal choices | E **0.125 B/repeated pt**, **32–174 MB × repeat fraction** at captured occupancy | 2–4 d | Medium; bitstream/state | Low net gain | **proposed**, format/design only; direct stock XOR2 replacement **rejected** |
| 26. Compressed expansion IDs | Mimir | Bounded generation-pinned expansion cache + ordinal→name lookup | L 1M IDs **8.0→1.94 MB** random gaps or **0.0469 MB** dense; excludes names/reverse index | 7–12 d | Medium–high; index epochs | High primitive; zero current saving | **proposed**, shared-catalog mode currently bypasses this cache |
| 27. Cost-aware query series sharding | Mimir | Budgeted independent series execution; expression planning remains frontend-owned | E four balanced workers give **≤4× parallelizable-stage speedup**, may increase total CPU/NIC | 5–10 d | High; skew/budget/fanout | Low workload return evidence | **proposed**, design only |
| 28. Cap idle codec contexts | VM | Bound surplus retained contexts without blocking active calls | L **~0.58 MB/context**; E 96→8 idle contexts **~51 MB** | 1–2 d | Medium; churn under bursts | High per-context bytes; idle count unknown | **proposed**, low priority versus GB-scale metadata |
| 29. Shared startup budget | VM, GreptimeDB, OO | Existing startup component policy and pacing | **0 measured default saving**; explicit settings remain authoritative; not an RSS cap | 0 new | Medium; pinned/native memory | High mechanism | **done**, existing opt-in policy; no new flag here |
| 30. Earlier name arena / whole compressed metadata buckets | VM, historical GreptimeDB | Rejected representations, distinct from shared registry | L arena **~90 B/name** but reinsertion **+18–29% CPU**; bucket lookup **28.2× slower** | 0 | High CPU regression | High tested negative result | **rejected**; superseded ownership study is rank 9 |
| 31. Replace index / raw staging / WAL / correction semantics wholesale | All | Keep durable IDs, FST, compact cache, existing logs, LWW and native archives | **0 established equivalent-contract saving**; VM max-value dedup and WAL-less preflush change guarantees; OO MIDX duplicates parent numeric data | 0 | High semantic/durability cost | High mismatch evidence | **rejected**; no duplicate bulk index found, no network saving re-credited |



Historical name-arena and whole-bucket compression screens remain rejected at their tested boundaries: the former saved about 90 B/metric but increased reinsertion CPU 18–29%; the latter increased cold lookup + first name-key read 0.615→17.331 µs (28.2×). The newer shared-name registry has different ownership and a separate mixed-population result. Historical Greptime partition-tree dictionaries/PkId informed that work but were removed on 2026-05-15; current flat/sparse mode selects bulk memtables. [H-tree][H-tree] [H-dict][H-dict] [T-memselect][T-memselect]

## Measured on identical data

The following server replays and metadata A/B are the retained **30 October** experiments. New codec/primitive screens and canaries above have separate populations and do not revise these rows.

### Inputs and method

| Dataset | Raw lines | Finite replay points | Distinct finite paths | Export span | Excluded non-finite |
| --- | ---: | ---: | ---: | ---: | ---: |
| D1 | 13,224,474 | 13,224,445 | 11,475 | 24.00 h | 29 |
| D2 | 6,350,894 | 6,350,894 | 6,886 | 24.00 h | 0 |
| D3 | 4,391,985 | 4,391,985 | 2,473 | 6.00 h | 0 |

Original timestamps, gaps, names and finite values were retained. The 29 non-finite D1 lines were excluded identically from all systems. The finite input contains no duplicate/conflicting `(path,timestamp)` pairs. Thus the replay checks neither exceptional floats nor duplicate/OOO policy exhaustively.

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
| **go-carbon reads 10–50× faster on these shapes** | On D1/D2 **1×24 h and 100×24 h native-API p50 versus VM**, ratios are approximately 14–50×. This is not universal: 1000×1 h is around 4× there; D3 gains are smaller, and Greptime is faster on the D2 1000×1 h cell. Response sizes/grid/serialization differ, so these are serving comparisons, not isolated read-engine rankings. |
| **VM uses ≈37% less ingest CPU** | Full-rate D1 **36.9%** and D2 **38.5%** below go-carbon; D3 only **8.9%** below. Paced D3 VM is slightly higher. Do not read 37% as an aggregate or production-capacity forecast. |
| **VM stores 27–52% fewer core bytes** | Full-rate durable B/input point: D1 **2.75 → 1.44**, D2 **2.01 → 0.97**, D3 **1.55 → 1.13**. VM does not produce go-carbon's production rollups and can change finite values; total-file accounting and retained archive0 samples differ. |
| **Greptime result depends on the dataset/read shape** | Durable B/input point **2.59 / 1.59 / 2.50** for D1/D2/D3. Ingest CPU is higher here; wide D2 SQL reads can trade more CPU for competitive latency. One table/region is not a tuning search. |
| **Numeric differences are measured, not automatically disqualifying** | VM changes **16/400 D1 values**, max relative error **6.52e−13**; D2 changes **24/400**, max **4.23e−13**; D3 checks agree. Bit-exactness is no longer required; a decimal codec and its error/exception policy are under evaluation. |

| Dataset | go-carbon durable B/input point | VM | GreptimeDB | VM reduction vs go-carbon |
| --- | ---: | ---: | ---: | ---: |
| D1 | 2.752 | 1.436 | 2.593 | 47.8% |
| D2 | 2.007 | 0.967 | 1.589 | 51.8% |
| D3 | 1.554 | 1.134 | 2.503 | 27.0% |

### All 24 runs

`full` means unpaced input; `50000` means paced at 50,000 points/s. `pack` changes in-memory metadata packing, not the persistent sample codec. For each dataset the first table gives bytes, completion rates, ingest CPU and RSS; read cells are **p50 / p99 ms; server CPU ms/query**. Correctness samples are repeated for both rates; they are not an exhaustive archive-value proof.

¹ Persistence Mpt/s includes settle/control waits and go-carbon's offline flush/compaction; paced runs include pacing time. ² Ingest CPU includes server user+sys through settle and excludes client/read CPU. ³ The native coarse-window column uses different resolutions/aggregators and is not an equivalent-work ranking.

#### D1

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

#### D2

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

#### D3

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
| D2 / full / off | 6,345,854 | 903,697 | 1.629 (1.630) | 0.318301 (2.237) | 1.991 |
| D2 / 50000 / off | 6,345,784 | 903,697 | 1.629 (1.630) | 0.318301 (2.237) | 1.991 |
| D2 / full / on | 6,345,644 | 903,697 | 1.629 (1.630) | 0.318301 (2.237) | 1.991 |
| D2 / 50000 / on | 6,345,644 | 903,697 | 1.629 (1.630) | 0.318301 (2.237) | 1.991 |
| D3 / full / off | 4,391,985 | 2,210 | 1.737 (1.737) | 0.000473 (0.939) | 0.433 |
| D3 / 50000 / off | 4,391,985 | 2,210 | 1.737 (1.737) | 0.000473 (0.939) | 0.433 |
| D3 / full / on | 4,391,985 | 2,210 | 1.737 (1.737) | 0.000473 (0.939) | 0.433 |
| D3 / 50000 / on | 4,391,985 | 2,210 | 1.737 (1.737) | 0.000473 (0.939) | 0.433 |
| D1 / full / off | 13,214,305 | 1,087,881 | 2.191 (2.193) | 0.271954 (3.306) | 2.079 |
| D1 / 50000 / off | 13,213,663 | 1,087,881 | 2.191 (2.193) | 0.271590 (3.301) | 2.079 |
| D1 / full / on | 13,214,305 | 1,087,881 | 2.191 (2.193) | 0.271954 (3.306) | 2.079 |
| D1 / 50000 / on | 13,212,576 | 1,087,881 | 2.191 (2.193) | 0.271590 (3.301) | 2.079 |

D3 retains every input point in archive0. D1/D2 archive0 counts are slightly lower because original timestamps age beyond the shortest production retention during replay; some samples route to coarser archives. This is not proof of a lossless point-for-point primary-data disk comparison. No synthetic seven-day history or duplicated data was added.

### Metadata packing A/B

| Dataset / rate | Charged metadata MiB off→on | Charged reduction | Arena MiB off→on | Ingest CPU change | Post-drain RSS MiB off→on |
| --- | ---: | ---: | ---: | ---: | ---: |
| D1 / full | 29.57→14.87 | 49.7% | 43.69→28.38 | +6.5% | 470→466 |
| D1 / 50000 | 29.57→14.93 | 49.5% | 44.06→31.60 | -2.6% | 426→436 |
| D2 / full | 13.50→7.09 | 47.5% | 24.38→17.54 | +5.0% | 401→397 |
| D2 / 50000 | 13.50→7.09 | 47.5% | 24.50→18.75 | +4.4% | 369→362 |
| D3 / full | 2.22→1.48 | 33.3% | 7.92→7.59 | +17.5% | 165→207 |
| D3 / 50000 | 2.22→1.48 | 33.3% | 8.19→7.96 | +3.6% | 161→175 |

One paired run per dataset/rate, without eviction pressure. Charged-byte reductions are consistent; RSS and CPU require repeated capacity-matched qualification. Thousands of stripes and minimum 4 KiB chunks mean compressing scattered records may not free a chunk. Arena chunks grow up to 64 KiB. The 33–50% charged saving is not a 33–50% RSS promise. [Packing threshold][C-snappy] [State cache][C-state] [Arena][C-slab]

### RSS during paced ingestion

Median/range in the final 20 s of the 50,000-points/s replay, before reads; a short replay still does not establish production steady state.

| Dataset / system | Median RSS MiB | Range MiB | Samples |
| --- | ---: | ---: | ---: |
| D2 / go-carbon | 337 | 303–341 | 38 |
| D2 / go-carbon pack | 367 | 266–376 | 38 |
| D2 / GreptimeDB | 334 | 298–359 | 37 |
| D2 / VictoriaMetrics | 126 | 125–129 | 37 |
| D3 / go-carbon | 151 | 135–154 | 38 |
| D3 / go-carbon pack | 175 | 173–179 | 38 |
| D3 / GreptimeDB | 229 | 185–257 | 38 |
| D3 / VictoriaMetrics | 141 | 140–141 | 38 |
| D1 / go-carbon | 408 | 408–408 | 38 |
| D1 / go-carbon pack | 433 | 433–433 | 37 |
| D1 / GreptimeDB | 351 | 297–397 | 38 |
| D1 / VictoriaMetrics | 159 | 159–160 | 37 |

### Native read work explains part of the latency difference

These are full-rate, packing-off responses. Counts are non-null decoded points. go-carbon may return empty named series that SQL/VM omit; VM lookback/gap-fill on a 1 s grid can return more values. Greptime SQL also sorts the paired timestamp/value arrays. Profiling did not isolate each cost; a common serializer/grid/response contract is needed for a pure read-engine ranking.

| Dataset / system | 100×24h points / response MiB | 1000×1h series / points / response MiB |
| --- | ---: | ---: |
| D1 / go-carbon | 117,551 / 1.06 | 1,000 / 56,339 / 0.74 |
| D1 / VictoriaMetrics | 352,653 / 148.12 | 970 / 169,017 / 60.28 |
| D1 / GreptimeDB | 117,551 / 2.26 | 970 / 56,339 / 1.25 |
| D2 / go-carbon | 133,568 / 1.20 | 1,000 / 58,419 / 0.91 |
| D2 / VictoriaMetrics | 400,704 / 148.08 | 996 / 175,257 / 62.03 |
| D2 / GreptimeDB | 133,568 / 2.64 | 996 / 58,419 / 1.39 |
| D3 / go-carbon | 1,680,114 / 21.09 | 1,000 / 305,611 / 3.44 |
| D3 / VictoriaMetrics | 1,924,077 / 145.25 | 664 / 380,569 / 40.62 |
| D3 / GreptimeDB | 1,680,114 / 31.08 | 664 / 305,611 / 5.74 |

## Qualification and remaining questions

| Area | Next evidence needed |
| --- | --- |
| Decimal codec | #1034 keeps a decimal block only within 5.1e-12 relative error and leaves NaN, Inf, −0 and subnormals exact. Still needed: staleness policy, precision across rollups and production read/CPU cost. It need not preserve arbitrary Float64 bits under the current decision. |
| Serving parity | Repeat with a common grid/serializer and output contract; isolate gap-fill, aggregation and storage work. Add concurrent requests/writes and more than one empirical p99. |
| Sustained capacity | Quiet host, common OS CPU quota, realistic cardinality/churn and 960 s coalescing; repeat through compaction-debt recovery. These 2.5–11.5 K-series replays cannot forecast a ~20 M-series store. |
| Correctness | Larger retained-value checks, actual seven-day history, duplicate/OOO/late corrections, retention boundaries and explicit XFF/staleness behavior. All Greptime finite-input counts match, but count equality alone is insufficient. |
| Memory | Capacity-matched useful hit rates, physical charged/arena/pinned/mapped bytes and peak scratch. Opt-in budget pressure CPU and p99 remain unqualified. |
| Append | Qualify end-to-end behavior, bound pending runs/fold debt, measure uncached-read CPU/allocation and preserve archive/metadata lifecycle. |
| Expiry | Prove true event-time/archive-cohort retirement; current write-hour buckets and metric/catalog GC are not substitutes. |
| External details | VM release-source match, exact WAL/acknowledgement guarantees across engines, Greptime last-row modes and SQL materialization, full-server Prometheus/Mimir/OpenObserve serving parity remain unverified. |

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

[VM2-identity]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/lib/storage/storage.go#L1927
[VM2-codec]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/lib/encoding/encoding.go#L120
[VM2-classifier]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/lib/encoding/encoding.go#L354
[VM2-query]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/app/vmselect/promql/rollup_result_cache.go#L283
[VM2-levels]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/lib/encoding/encoding.go#L394
[VM2-stats]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/lib/storage/merge.go#L53
[VM2-limits]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/app/vmselect/promql/eval.go#L1864
[VM2-pool]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/lib/storage/inmemory_part.go#L103
[OO-alp]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/metrics_index/src/block/codec/alp.rs#L35
[OO-block-path]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/promql_service/src/search/grpc/storage.rs#L77
[OO-blockmeta]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/metrics_index/src/block/mod.rs#L65
[OO-bloom-prune]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/search/src/bloom_pruner.rs#L16
[OO-breakers]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/ingester/src/writer.rs#L65
[OO-compact]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/metrics_index/src/block/compact.rs#L126
[OO-config]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/config/src/config.rs#L2180
[OO-directory]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/metrics_index/src/block/directory.rs#L117
[OO-downsample]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/search/src/datafusion/merge/downsampling.rs#L226
[OO-downsample-gate]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/compaction/src/lib.rs#L198
[OO-duplicates]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/promql/src/series_stream/blocks.rs#L2292
[OO-index-cache]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/metrics_index/src/block_cache.rs#L27
[OO-ingest-pack]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/ingester/src/pack.rs#L16
[OO-interner]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/promql/src/series_loader/label_interner.rs#L27
[OO-label-cache]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/promql/src/series_loader/label_cache.rs#L119
[OO-late]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/compaction/src/merge/metrics.rs#L42
[OO-memtable]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/ingester/src/memtable.rs#L39
[OO-pack]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/metrics_index/src/block/codec/pack.rs#L94
[OO-parquet]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/config/src/utils/parquet.rs#L87
[OO-partition]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/ingester/src/partition.rs#L308
[OO-peak]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/search/src/datafusion/peak_memory_pool.rs#L28
[OO-plan]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/compaction/src/merge/plan.rs#L97
[OO-prefetch]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/promql/src/series_stream/blocks.rs#L214
[OO-preprocess]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/ingester/src/writer.rs#L441
[OO-query-pool]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/search/src/datafusion/exec.rs#L149
[OO-range-cache]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/promql_service/src/search/cache/mod.rs#L112
[OO-reader]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/metrics_index/src/block/reader.rs#L29
[OO-restart]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/ingester/src/lib.rs#L113
[OO-samples]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/metrics_index/src/block/samples.rs#L60
[OO-samples-decode]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/metrics_index/src/block/samples.rs#L126
[OO-singleflight]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/promql/src/series_stream/blocks/loads.rs#L28
[OO-time-prune]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/promql/src/series_stream/blocks.rs#L905
[OO-trailer]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/config/src/meta/promql/index.rs#L25
[OO-value-pick]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/metrics_index/src/block/samples.rs#L76
[OO-wal]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/wal/src/writer.rs#L115
[OO-writer-finish]: https://github.com/openobserve/openobserve/blob/04f6d0b8a9de66d011f7b83033d42b711565bc4c/src/metrics_index/src/block/writer.rs#L152
[P-append]: https://github.com/prometheus/prometheus/blob/b0c3a30ab2a09b88e64d876017a0134aa2f38f40/tsdb/head_append.go#L444
[P-head]: https://github.com/prometheus/prometheus/blob/b0c3a30ab2a09b88e64d876017a0134aa2f38f40/tsdb/head.go#L3074
[P-wal]: https://github.com/prometheus/prometheus/blob/b0c3a30ab2a09b88e64d876017a0134aa2f38f40/tsdb/wlog/wlog.go#L41
[P-chunks]: https://github.com/prometheus/prometheus/blob/b0c3a30ab2a09b88e64d876017a0134aa2f38f40/tsdb/chunks/chunks.go#L480
[P-index]: https://github.com/prometheus/prometheus/blob/b0c3a30ab2a09b88e64d876017a0134aa2f38f40/tsdb/index/index.go#L495
[P-xor]: https://github.com/prometheus/prometheus/blob/b0c3a30ab2a09b88e64d876017a0134aa2f38f40/tsdb/chunkenc/xor.go#L161
[P-xor2]: https://github.com/prometheus/prometheus/blob/b0c3a30ab2a09b88e64d876017a0134aa2f38f40/tsdb/chunkenc/xor2.go#L343
[P-ooo]: https://github.com/prometheus/prometheus/blob/b0c3a30ab2a09b88e64d876017a0134aa2f38f40/tsdb/ooo_head.go#L23
[P-merge]: https://github.com/prometheus/prometheus/blob/b0c3a30ab2a09b88e64d876017a0134aa2f38f40/storage/merge.go#L808
[P-replay]: https://github.com/prometheus/prometheus/blob/b0c3a30ab2a09b88e64d876017a0134aa2f38f40/tsdb/head_wal.go#L98
[M-header]: https://github.com/grafana/mimir/blob/1036b38cbd8c8e4bdeff829d6d99fa59bddb5928/pkg/storage/indexheader/header.go#L93
[M-limits]: https://github.com/grafana/mimir/blob/1036b38cbd8c8e4bdeff829d6d99fa59bddb5928/pkg/util/limiter/memory_consumption.go#L370
[M-validation]: https://github.com/grafana/mimir/blob/1036b38cbd8c8e4bdeff829d6d99fa59bddb5928/pkg/storage/tsdb/block/blockvalidation/meta.go#L61
[M-compactor]: https://github.com/grafana/mimir/blob/1036b38cbd8c8e4bdeff829d6d99fa59bddb5928/pkg/compactor/compactor.go#L167
[M-chunks]: https://github.com/grafana/mimir/blob/1036b38cbd8c8e4bdeff829d6d99fa59bddb5928/pkg/storegateway/bucket_chunk_reader.go#L74
[M-shard]: https://github.com/grafana/mimir/blob/1036b38cbd8c8e4bdeff829d6d99fa59bddb5928/pkg/frontend/querymiddleware/querysharding.go#L354
[M-modules]: https://github.com/grafana/mimir/blob/1036b38cbd8c8e4bdeff829d6d99fa59bddb5928/vendor/modules.txt#L2172
[M-build]: https://github.com/grafana/mimir/blob/1036b38cbd8c8e4bdeff829d6d99fa59bddb5928/Makefile#L259
[M-cachekey]: https://github.com/grafana/mimir/blob/1036b38cbd8c8e4bdeff829d6d99fa59bddb5928/pkg/storage/tsdb/indexcache/cache.go#L196
[M-extents]: https://github.com/grafana/mimir/blob/1036b38cbd8c8e4bdeff829d6d99fa59bddb5928/pkg/frontend/querymiddleware/results_cache.go#L460
[VM2-dedup]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/lib/storage/dedup.go#L30

[V-downsample]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/docs/victoriametrics/README.md#L1715

[VM2-durability]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/lib/storage/partition.go#L1479
[VM2-average]: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5858c66de15d18fec31ef0b6ffa580bbaad2870e/lib/streamaggr/avg.go#L3
