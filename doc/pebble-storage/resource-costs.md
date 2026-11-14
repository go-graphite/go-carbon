# Pebble store: per-component resource costs

Historical measurements from five production stores on 2 November 2026, plus
explicitly labelled local experiments and estimates. D1–D5 identify anonymous
workloads with 4.14–28.7M cached metrics; these are not sizing promises.


| Resource | Biggest consumers per host in the snapshot; overlapping memory rows are not additive | Biggest remaining cut / trade-off | Kind · source |
| --- | --- | --- | --- |
| Memory | Metadata arenas 13.3–108 GB; block cache 33.9–91.3 GB; allocated heap above last-GC live 10.8–84.7 GB; cache-owned names up to 21.7 GB; point buffers 0.81–3.18 GB | Metadata pack: D2 cache 42.9 → 33.3 GB, RSS 224 → 203 GB; templates model another 4.23–5.45 GB beyond packing | P · S, N; E · MC |
| CPU | Receive/cache 1.41–5.14 cores; persister 0.64–2.40; render 0.073–1.95; compaction 0–1.11; input log 0.045–0.38 | Further batch/commit grouping unmeasured; prior D5 batching cut host CPU 25–41%; local sampler 153 ms → 84.6 µs CPU/sample | P · S, MW; L · AS |
| Write I/O | Table writes 19.0–150 MB/s; WAL 4.60–40.8 MB/s; input log 0.90–9.49 MB/s; compaction up to 126 MB/s inside table writes | Batch/coalesce metric updates; historical D3 compaction 258 vs flush 27 MB/s | P · S, MM |
| Read I/O | Table reads 8.96–152 MB/s; compaction 0–146 MB/s inside table reads; device reads 0.0041–1.86 MB/s; render/persister share unmeasured | Keep useful block-cache coverage; cold Zstd doubles local point-read p50 | P · S; L · PZ |
| Network | Ingress 1.91–20.3 MB/s; egress 0.55–15.5 MB/s; loopback 0.17–0.25 MB/s; render share unmeasured | Stream codec already removes repeated names; negotiated render Zstd models 0.420 → 0.211 MB/response | P · S; L · CI |
| Disk space | L6 448–1,441 GB; L5 up to 420 GB; L4 up to 135 GB; metadata checkpoint 10.5–93.5 GB; input log 8.59–12.9 GB | Exact cold-pack: fully compacted D1 1,559.3 → 1,167.2 GB; cold-level Zstd adds a separate local 10.6% packed-v9 SST cut | P · S, CC; L · PZ |
| Restart | Metadata checkpoints 10.5–93.5 GB; cache+tail dumps 1.39–4.62 GB; cache restore 18.6–71.5 s; prewarm 2.92–152 s | Parallel metadata load; compact → stream dump 5.88 → 2.17 B/pt, with higher encode/open cost and unsafe old-binary crash rollback | P · S; L · MD, RC |
| Threads | Worker OS tasks 199–264; Go-created threads 200–265; goroutines 1,004–1,643; 96 persister workers; 16 compaction workers | No measured steady thread reduction; more maintenance workers increase CPU and scratch budgets | P · S, S/W; E · CP |

P = prod-measured; L = local-bench; E = estimate; U = unmeasured (recipes below); source IDs resolve in the final table; snapshot columns always mean D1 / D2 / D3 / D4 / D5.

GB/MB are decimal, B/pt means bytes per point, and cores means CPU-seconds/wall-second; CPU core cost at 1M pt/s = ns/pt ÷ 1,000.

## Per place

Direct costs at each boundary; “—” means no direct charge, not free downstream storage; U-D/L/I/T/H/C/P identify measurement recipes below; five-value cells follow the host order above.

| Place | Memory: GB/host; per unit | CPU | Write I/O | Read I/O | Disk space | Network | Kind · source |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Write cache: points + bookkeeping + encoder state | Append buffers 0.809–3.18 GB; items/initial capacity 0.823–6.56 GB; encoder 56 B/item inside item; local point **5.10 → 1.58 B/pt**, whole cache **7.70 → 4.26 B/pt**, bookkeeping **660 → 528 B/metric** | Insert 1.28–4.74 cores; local Gorilla lifecycle **+0.085 core at 3M pt/s** | — RAM; persister/Pebble and log are separate places | — RAM; pop decodes 16 B/pt output | —; dumps/logs below | —; receivers below | P · S/H, S/C; L · CG, IT; E · code |
| Input log: segments | Encoder/frame sites 0.130–0.593 GB; decoder/segment peak U-L | Shared log phase 0.0447–0.376 core; local summed collect/encode/compress wall ~56 ns/pt | 0.900–9.49 MB/s; 0.915–2.64 B/logged pt | Replay 0 in snapshots; per-replay physical bytes U-L | 8.59–12.9 GB/host; self-contained segments repeat definitions after reset | — local log; copied separately when replacing a host | P · S/H, S/C; L · IT; U-L |
| Input log: dictionaries/identities | Lookup sites 0.322–1.88 GB; names referenced per segment; B/live definition U-L | Included in log phase above; isolated lookup U-L | Definitions once/segment, included above; no separate IOPS | Definitions read at replay; included in log reads | Included in segments; map/string headers are RAM only | — | P · S/H; E · code; U-L |
| Input log: staging | 0.302–1.32 GB; **32 → 24 B/staged pt** local; buffers have capacity slack | U-L CPU; local fused staged ingest wall 98.1 → 83.9 ns/pt | — RAM; drain charged to segments | — RAM | — | — | P · S/H; L · IT; U-L isolated CPU |
| Metadata cache: raw / packed / templates | Arena 13.3–108 GB/host, **1.11–1.12× logical**; writer header pages dominate record size, not arena waste; header-shrink ranking in HS | Charged inside persister/read work; local hit wall 148/542/490 ns raw/packed/templates; isolated CPU U-P | — RAM installs; checkpoint below | RAM hits; fallback store lookups U-P | — cache itself; GCSTATE below | — | P · S; L · MC, HS; U-P |
| Restart prewarm | Installs 0.00597–0.364 GB since start into existing metadata budget; transient peak U-P | Historical ~56 µs CPU/metric; current stage cores U-P; 2.92–152 s accumulated wall | — except ordinary store activity | 0.00630–3.34 GB logical startup reads; physical bytes U-P | — extra file | — local store | P · S, MR; U-P |
| Carbonserver trigram / trie | Alternative trigram ~190 B/name + postings; trie labels/nodes U-I; current mutable/index support heap 0.00749–0.352 GB | Current index label 0–0.562 core, including refresh; alternative-mode CPU U-I | No direct per-query writes; saved generations below | Lookup RAM / mapped index; backing-store reads on render | — for heap trie/trigram; saved files below | Find results/query bytes U-T | P · S/H, S/C; E · CI/I, code; U-I,T |
| Carbonserver `.fst` files + index snapshots | Resident FST **1.04 / 0.470 / 0.800 / 0.221 / 0.157 GB**; `.meta`/overlay/retired-generation peak U-I | Build/checksum/open/query CPU U-I; current index phase above is shared | Writes `.fst` + packed `.meta` + manifest; replacement may coexist with current generation; bytes/IOPS U-I | mmap/page faults + validation scans; physical bytes U-I | Full file lengths U-I; RSS is only resident pages | — local accelerator | P · S/W; E · IX, code; U-I |
| File-list cache (FLC) | Gzip buffers + decoded path/entry; peak and B/metric U-I | Gzip encode/decode CPU U-I; saved-index fast startup can bypass full decode | gzip file; v2 raw path bytes + **41 B/entry** before gzip; compressed bytes/IOPS U-I | File/gzip stream for fallback/bootstrap; bytes/IOPS U-I | gzip size U-I; v1 newline names, v2 names + four statistics | — | E · code, IX; U-I |
| Stream wire codec: raw / Zstd | Decoder flat 0.0885–1.40 GB; 40 B/bound ID-slot budget + identities; no extra bound-name copy; peak U-T | Receive+insert 1.41–5.14 cores; local fused raw/Zstd ingest 92.5/104 ns/pt; isolated codec U-T | — except downstream log/store | — except downstream render/store | — | Raw 3.01 B/pt local; Zstd 2.78 B/output pt in a different local fixture; attributed host bytes U-T | P · S/H, S/C; L · NR, SC; E · code; U-T |
| HTTP render responses | ~9 B/response pt + per-series names/range; raw decoded scratch 16 B/pt; per-host peak U-T | Render 0.073–1.95 cores; marginal D2 replay ~53 ms CPU/render; local gzip/Zstd compression wall 5.8/3.1 ms/response | — response; optional cache RAM | Backend + cache; physical bytes/render U-T | — response | Local protobuf/gzip/Zstd **1.672 / 0.420 / 0.211 MB/response**; current NIC share U-T | P · S/C, DR; L · CI; E · CI/I; U-T |
| gRPC / CarbonLink | gRPC float/absence arrays ~9 B/pt; CarbonLink decoded 16 B/pt; per-host peak U-T | Disabled in S; enabled-call CPU U-T | — response | gRPC fetch can read store; CarbonLink reads write cache; attributed bytes U-T | — response | CarbonLink **15 B/pt + envelope**; gRPC names/range once/series, gzip registered; bytes by negotiated encoding U-T | P · S config; E · CI/I, code; U-T |

| Place | Memory: GB/host; per unit | CPU (wall timings labeled separately) | Write I/O | Read I/O | Disk space | Network | Kind · source |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Dump: legacy | Decoded batch 16 B/pt + writer scratch/sink buffers; host peak U-D | U-D CPU; variable-length value/time delta encoding | Serialized file + checksum/fsync; bytes/IOPS U-D | Ordered restore / checksum scans; bytes/IOPS U-D | Name/count once/batch + signed varint value/timestamp deltas; B/pt U-D | — local file | E · code; U-D |
| Dump: compact | Mapped source + index; 50M-pt local open heap growth 6.37 MB, not total peak; decoded restore 16 B/pt | U-D CPU; local encode/restore wall 4.86/14.2 ns/pt | 5.88 B/pt local, plus checkpoint index/fsync | Mapped validation/restore; physical bytes U-D | 0.294 GB/50M pt fixture; name/count/length per batch + compact bytes | — local file | L · RC; E · code; U-D |
| Dump: blocks | Compact bytes + sorted/front-coded/Zstd name buffers; ≤7 prefix steps/name lookup; host peak U-D | U-D sort/name codec; compact point bytes copied unchanged | 32 B/block header + compressed names + compact records; bytes/IOPS U-D | Validates/decompresses names, mapped point bytes; physical reads U-D | Exact B/pt U-D; provisional earlier host model 4.1–4.8 GB versus 5.30 GB compact | — local file | E · CI, code/recovery; U-D |
| Dump: opt-in stream (historical experiment, local) | Compressed mmap + **0.294 GB expanded compact payload/50M pt**, plus indexes; local open heap growth ~1.01 GB, not retained payload alone | U-D CPU; encode/restore wall 6.80/9.94 ns/pt; encode +40%, restore −30% | **2.17 B/pt**, plus manifest/fsync; ≤1.6 target missed | Open expands source (272 ms/50M pt); physical bytes U-D | 0.109 GB/50M pt fixture; old-binary crash rollback unsafe | — local file | L · RC; U-D |
| Pending-point recovery index (GCPOINT) | Mapped hash/record tables; GCPOINT3 16 B/slot at ~75% load + 4 B/ID + name table; host bytes U-D | Hash/checksum/validation/name claims U-D | Separate index + manifest; bytes/IOPS U-D | mmap plus source validation; bytes/IOPS U-D | V2 record extents; V3 reconstructs extents from Blocks; host file lengths U-D | — local file | E · code; U-D |
| Metadata cache checkpoint (GCSTATE) | Load rebuilds same metadata cache, not point history; section buffers ~4.19 MB, whole peak U-D | U-D CPU; local 28.3/88.7 GB load wall **13.0/33.1 s**, independent-window sums | Live records + **8 B/entry**, **60 B/file**; no slab/hash capacity; IOPS U-D | Snapshot last-load bytes **32.9 / 41.6 / 93.5 / 10.5 / 11.8 GB**; physical bytes U-D | GCSTATE1/2 baseline; local MC GCSTATE3 expands IDs/templates to portable name-bearing records | — local file | P · S; L · MD, MC; E · code; U-D |
| Handoff tail (file replay) | Cold dictionaries + admission arrays; local admission allocation 680 → 950 B/pt is churn, not retained heap; host peak U-H | U-H CPU; local admission wall 639 → 480 ms; production **14.6 s not reproduced** | Legacy → negotiated stream **210 → 9.6 B/pt**; ≤2 target missed | Tail-file read + cache admission; physical I/O U-H | 0.389 → 0.0177 GB/1.85M pt fixture; old peers receive legacy | — local tail file, distinct from socket handoff | L · RC; P · CI/I earlier load; U-H |
| Handoff transfer (sockets + decoder state) | Passed FDs + buffered raw input + serialized per-connection names/predictors; ≤17 B/predictor plus name/framing before optional Zstd; peak U-H | Snapshot/restore/framing CPU U-H | — control stream; tail/dump are separate files | — control stream | — directly | Unix socket FD/control + raw length-delimited `Leftover`; no NIC payload; actual bytes U-H | E · code; U-H |
| Copy tool: whole-store checkpoint | Reused 1.05 MB buffer/stream/role, ≤64 active streams =67.1 MB active scratch/role; total peak U-C | Local sparse/dense **1.33/1.12 CPU-s/stored GB**; checksum/syscall costs remain | Target stored bytes + log/manifest/fsync; local payload 612 MB/s is not device bandwidth; physical U-C | Source immutable SST ranges; target verification/resume rereads; physical U-C | Native checkpoint/hardlinked SSTs + retained log; target full copy; source pinned SST growth U-C | Stored files + logs/protocol; local loopback 612 MB/s, physical NIC U-T | L · WC; E · code; U-C,T |
| Copy tool: metric-range transfer tables | Local representative process RSS 1.5–2.2 GB; batch/export/target staging peak per host U-C | Local **16.08 CPU-s/raw GB**, **224 MB/s** baseline median; includes framing/verification/publication | Reserved-ID SST + independent read-back + ingest + receipt; physical bytes/amplification U-C | Export SST/history + overlap merge + verification/proofs; physical U-C | Incoming SST staging + target LSM; non-overlap packed values copied, overlap decoded; peak U-C | Name/config per metric + history payload/checksum; raw-history bytes differ from whole-store stored bytes | L · CM; E · code; U-C,T |

D3 selects Blocks; the other four snapshot configs select compact; legacy is supported, and stream is the separate local opt-in experiment; snapshot dump+tail totals do not isolate each file. [P · S; L · RC]

| Unmeasured code | How to measure the missing place-specific costs | Kind · source |
| --- | --- | --- |
| U-D: dumps, GCPOINT, GCSTATE | Same corpus per format; count points/batches/indexed names; record file lengths, stage CPU, peak heap/RSS, filesystem/device deltas and encode/open/restore wall separately | U · RC, MD, code |
| U-L: input log | Count live definitions, predictor/map/slice capacities and staged rows; profile collect/encode/compress/replay and measure segment/device deltas across full drain/rotation cycles | U · IT, code |
| U-I: index/FLC | `stat` `.fst`/`.meta`/manifest/gzip files, count indexed names, retain all per-VMA size/RSS (not only large mappings); profile build/open/refresh/query separately | U · S/W, IX, code |
| U-T: wire/RPC/render | Count socket payload bytes and negotiated encoding per protocol; profile matched request/frame/point workloads, peak response/dictionary memory and attributed backend I/O | U · NR, SC, CI, code |
| U-H: handoff | Record per-connection raw/compressed `Leftover`, names/predictors/FD counts, Unix-socket bytes, tail bytes and stage CPU/peak memory; separate transfer from file admission | U · RC, code |
| U-C: copy | Local source/target clones: peak RSS/staging bytes, useful raw history versus stored/wire/SST/device bytes, read-back/compaction I/O, process CPU and log retention; keep whole-store/range paths separate | U · WC, CM, code |
| U-P: metadata/prewarm | Profile hits/installs and prewarm separately from checkpoint load; record logical key reads, misses, installed bytes, CPU, peak heap and device deltas at the same entry coverage | U · MR, code |

## Snapshot boundary and occupancy

| Per host, 2026-11-02 | D1 | D2 | D3 | D4 | D5 | Kind · source |
| --- | --- | --- | --- | --- | --- | --- |
| Accepted M pt/s | 2.86 | 2.88 | 1.07 | 3.61 | 0.867 | P · S |
| Cached metrics, M | 8.1 | 11.5 | 28.7 | 4.86 | 4.14 | P · S |
| Cached points, M | 839 | 1,390 | 581 | 1,020 | 255 | P · S |
| Cached pt/metric | 104 | 122 | 20.2 | 209 | 61.6 | P · S |
| Metadata entries, M | 36.8 | 20.1 | 38 | 12.3 | 13.9 | P · S |
| GOGC / metadata budget GB | 150 / 34.4 | 150 / 68.7 | 200 / 103 | 200 / 17.2 | 200 / 42.9 | P · S |

One host per class, a historical production release; CPU windows start 22:47–22:50 CET, metrics span ~30 s, heap follows CPU; no class is missing. [S]

`stat*`, `io*`, `status.txt`, `threads.txt`, `smaps_rollup.txt` are supervisor captures; added `worker_proc.txt` supplies worker status/smaps/I/O/tasks/mappings; keep its later sample separate from earlier metrics/heap. [P · S, S/W]

## Memory: heap, native cache and RSS

| GB/host | D1 | D2 | D3 | D4 | D5 | Kind · source |
| --- | --- | --- | --- | --- | --- | --- |
| RSS, earlier worker metrics | 266 | 227 | 358 | 170 | 108 | P · S |
| RSS, later worker smaps | 265 | 229 | 378 | 174 | 117 | P · S/W |
| ↳ Anonymous RSS: Go + native C | 264 | 229 | 377 | 174 | 117 | P · S/W |
| ↳ File-backed RSS, all files | 1.08 | 0.519 | 0.861 | 0.26 | 0.197 | P · S/W |
| ↳ Resident `.fst`, subset of file RSS | 1.04 | 0.47 | 0.8 | 0.221 | 0.157 | P · S/W |
| ↳ `[heap]` brk, subset of anonymous RSS | 1.64 | 1.08 | 0.918 | 1.66 | 0.753 | P · S/W |
| PSS, later worker smaps | 265 | 229 | 378 | 174 | 117 | P · S/W |
| Last-GC live heap, runtime | 55.1 | 61.6 | 148 | 24.7 | 23.1 | P · S |
| Sampled retained heap, pprof | 54.9 | 61.5 | 147 | 24.6 | 23.3 | P · S |
| HeapAlloc, including uncollected allocations | 123 | 110 | 233 | 65.6 | 33.9 | P · S |
| HeapInuse, allocated Go spans | 134 | 118 | 249 | 69.5 | 39.1 | P · S |
| HeapSys / released to OS | 169 / 14.2 | 181 / 16.8 | 345 / 47.4 | 79.7 / 0.00454 | 74.6 / 9.98 | P · S |
| Go Sys − released, resident-budget proxy | 158 | 167 | 303 | 81.4 | 66.8 | E · S |
| HeapAlloc − last-GC live | 68.3 | 48.5 | 84.7 | 41 | 10.8 | P · S |
| Pebble block cache, native C allocation | 91.3 | 49.9 | 45.1 | 75.4 | 33.9 | P · S |
| Pebble memtables, native C allocation | 4.29 | 2.15 | 4.29 | 4.29 | 2.15 | P · S |
| Metadata logical / allocated arena | 34.3 / 38 | 43.1 / 48.1 | 96.5 / 108 | 12 / 13.3 | 13.1 / 14.6 | P · S |
| GOMEMLIMIT, Go runtime budget | 535 | 535 | 535 | 535 | 535 | P · S |
| Write-cache reported owned estimate | 7.94 | 9.37 | 25.2 | 6.84 | 3.2 | P · S |
| Earlier RSS − earlier HeapAlloc (not native-only) | 143 | 117 | 125 | 104 | 74 | P · S |

Heap profiles describe last-GC sampled objects, not current HeapAlloc or resident pages; metadata arenas are already in Go heap, while native block-cache/memtable bytes are outside it. [S, MI]

GOGC 150 targets roughly 2.5× live Go heap; GOGC 200 roughly 3×, subject to roots, pacing and GOMEMLIMIT; neither limits C memory. [E · MI, code]

Worker smaps resolves anonymous/file RSS and resident `.fst` pages (mapping-group MiB rounded down, converted to GB; small groups omitted); `[heap]` is the native brk mapping, not Go heap; anonymous mappings still mix Go spans, native mmap arenas and C stacks. [P · S/W]

Do not subtract earlier heap/native-cache gauges from later smaps: samples are not aligned; malloc-arena statistics and separate Go/C residency remain unmeasured. [U · S, S/W]

## Memory: components and per-unit costs

| Retained GB/host; allocation-site attribution | D1 | D2 | D3 | D4 | D5 | Kind · source |
| --- | --- | --- | --- | --- | --- | --- |
| Metadata chunks / slots | 35.9 / 1.77 | 47.1 / 0.889 | 106 / 1.92 | 12.6 / 0.543 | 13.9 / 0.679 | P · S/H, N |
| Point append buffers, flat sites | 2.7 | 3.18 | 1.21 | 3.06 | 0.809 | P · S/H, N |
| Items + initial point capacities | 1.78 | 2.4 | 6.56 | 1.56 | 0.823 | P · S/H, N |
| Item-map arrays | 0.527 | 0.523 | 1.06 | 0.274 | 0.455 | P · S/H, N |
| Input-log point staging | 1.32 | 0.932 | 0.858 | 0.632 | 0.302 | P · S/H, N |
| Queued/in-flight log-generation maps | 0.702 | 0.645 | 1.27 | 0.201 | 0.322 | P · S/H, N |
| Sparse writeout tracking | 0.283 | 0.279 | 0.55 | 0.132 | 0.232 | P · S/H, N |
| Owned cache/stream name allocation sites | 1.84 | 2.19 | 21.7 | 1.25 | 1.72 | P · S/H, N |
| Stream identities + weak-table arrays | 0.769 | 0.15 | 0.181 | 0.397 | 0.555 | P · S/H, N |
| Receiver decoder flat allocations | 1.4 | 0.162 | 0.0885 | 0.968 | 0.847 | P · S/H, N |
| Input-log dictionary / frame sites | 0.845 / 0.319 | 0.535 / 0.255 | 1.88 / 0.593 | 0.322 / 0.13 | 0.537 / 0.214 | P · S/H, N |
| Index/namespace/query support, heap only | 0.352 | 0.00749 | 0.0275 | 0.241 | 0.0484 | P · S/H, N |

Flat allocation buckets above do not overlap; N retains source-line/normalization details; initial point capacity shares the item allocation line, and the reported owned estimate excludes the log encoder and recovery mappings. [S/H, N]

| Component | B/metric (or stated denominator) | B/pt where meaningful | Origin / constraint | Kind · source |
| --- | --- | --- | --- | --- |
| Metadata, logical per covered metric | 933 / 2,145 / 2,536 / 973 / 944 | Not point-proportional | Writer headers/buffers, names, keys, manifests and absent-prefix facts; `store-metadata-cache-size` bounds logical occupancy | P · S, HS |
| Metadata, arena per covered metric | 1,030 / 2,390 / 2,850 / 1,080 / 1,050 | Not point-proportional | Only **1.11–1.12× logical**: slot/index capacity, dead records and chunk tails; target writer record size, not a presumed 2–4× arena amplification | P · S, HS |
| Native block cache / memtables | Not metric-proportional | Not point-proportional | Block cache cap 96.6 GB; memtable 2.15 GB each, 17.2 GB maximum unflushed; occupancy above | P · S |
| Point append allocations, per cached metric | 333 / 278 / 42.2 / 631 / 195 | 3.21 / 2.28 / 2.09 / 3.02 / 3.17 | Compact delta/XOR buffers; excludes initial capacities in item site | P · S/H; E · normalization |
| Items + initial capacities | 220 / 210 / 228 / 321 / 199 | 2.12 / 1.72 / 11.3 / 1.54 / 3.23 | Baseline item 184 B, 192 B allocated, before point capacity | P · S/H; L · IT |
| Item-map arrays | 65.1 / 45.7 / 36.8 / 56.3 / 110 | 0.628 / 0.375 / 1.82 / 0.269 / 1.78 | Baseline 16 B/slot; half-full growth threshold leaves additional capacity | P · S/H; E · normalization |
| Log staging | 163 / 81.4 / 29.9 / 130 / 73 | 1.58 / 0.669 / 1.48 / 0.622 / 1.18 per cached pt; 32 B/staged pt | Transient raw points + ref + cached epoch/ID; depends on drain cadence, not total cached points | P · S/H; L · IT |
| Generation maps + sparse tracking | 122 / 80.7 / 63.4 / 68.4 / 134 | 1.17 / 0.662 / 3.13 / 0.327 / 2.17 per cached pt | Baseline shard-log first-generation map and sparse timestamp table | P · S/H; E · normalization |
| New bookkeeping benchmark | 528 (names/encoded payload excluded) | 5.28 at 100 cached pt/metric | Includes ten undrained staged pt/metric; not a universal constant | L · IT |
| Gorilla retained point storage | 24 extra encoder-state B/series, 56 → 80 | 5.10 → 1.58; whole heap 7.70 → 4.26 | Mean 105 pt/metric, 70% integer fixture; metadata/state remain | L · CG |
| Receiver decoded points / render arrays | Per connection/request, not catalog | 16 decoded; ~9 float+absence response | Temporary arrays may coexist with compact cache points | E · CI/I, code |
| Stream decoder predictors | 40 B/bound wire-ID slot, plus capacity/frame scratch | Not point-proportional | Bound IDs retain identities; full-name arena released with name resolver | E · CI/I, code |
| Cache-owned name sites, per cached metric | 228 / 191 / 755 / 257 / 415 | Not point-proportional | Allocator-attributed names; active identities/in-flight generations alter denominator | P · S/H; E · normalization |
| Metadata pack canary, per covered metric | ~2,140 → 1,660 B/entry | Not point-proportional | 42.9 → 33.3 GB / 20.08M entries; different D2 worker from snapshot | P · N; E · division |
| Input-log dictionary / segment identities | Unmeasured per defined name | Not point-proportional | Heap GB above; collect live dictionary population/capacity for B/name | P · S/H; E · gap |
| Metadata name payload, local fixture | ~176 B/entry including framing | Not point-proportional | One path in each sealed record, including packed records unless IDs enabled | L · MC |
| Legacy trigram file list, alternative mode | ~190 B/name at 174 B names | Not point-proportional | String header + allocated name, before postings; current trigram=false | E · CI/I; P · S |
| Current discovery index / file-list cache | Unmeasured B/indexed metric | Not point-proportional | Mmap FST + numeric columns, mutable trie overlay; `.fst` resident GB measured above; file sizes/indexed-count denominator remain U-I | P · S/H, S/W; U-I |

Normalized five-value cells use D1 / D2 / D3 / D4 / D5; metadata divides by covered entries, cache components by cached metrics/points, with naturally asynchronous heap samples. [E · S, N]

| Name copy | Current ownership | Cost boundary | Kind · source |
| --- | --- | --- | --- |
| Cache + stream | `Points.Metric` and embedded log ref alias one item name; live stream identity may share it; weak-slot replacement can retain duplicates | Owned name sites above; do not count all identity allocations as strings | P · S/H; E · CI/I |
| Decoder | Resolver-bound wire IDs hold references/predictors; unbound mode owns names per connection | Bound mode has no additional full-name copy per ID | E · CI/I, code |
| Input log | StreamRef aliases item; segment dictionary has string headers/references; definitions write the name once per segment | Dictionary allocations above are maps/scratch, not another full string per point | P · S/H; E · CI/I |
| Store catalog / metadata | Catalog `m/name` persists path; continuation/group keys use numeric ID+generation; metadata slabs independently retain path | Local catalog ~194–206 B/metric logical; transient LSM versions repeat it | L · CI/I |
| Index / file list | FST factors path prefixes; mutable trie has labels; legacy trigram has independent strings/postings | Resident `.fst` pages measured in S/W; full mapping/file lengths and per-name attribution unmeasured; do not apply 174 B/name savings to an already factored FST | P · S/W; E · CI/I |
| Dump / tail | Compact dump names each metric batch; legacy binary tail repeats name for each point | ~174 B/batch fixture name versus 210 B/pt cold-tail total | L · CI/I, RC |
| Shared registry candidate | Exact shared leases across decoder/cache/log; metadata/index integration later | Identities alone +14 B/name; full mixed fixture −167 B/name; not a free global name table | L · NR |

## Why bookkeeping still costs 528 B/metric

| Part in the actual 5M-metric / 500M-point fixture | B/metric after | Accounting | Kind · source |
| --- | --- | --- | --- |
| Item, including inline generation tracking | 208 | Points/name header 40 + compact state 56 + log ref 24 + hash/generation/cold fields/padding 88 | L · IT; E · code, N |
| Queued first-log generation + tracked flag | 9, already inside 208 | No separate queued pointer-map allocation; in-flight map empty before benchmark pop | E · code, N |
| Item-map pointer slots | 26.84 | 16,777,216 × 8 / 5M; actual load 29.8%, threshold 50% | L · IT; E · N |
| Item-map control bytes | 3.36 | One byte/slot; map total 30.20, not 9 B/metric | E · code, N |
| Staging refs + raw points, used rows | 240 | 10 staged pt/metric × (8 B ref + 16 B point) | E · IT benchmark, N |
| Staging slice capacity slack | 49.77 | Actual capacities: refs 95.51 + points 194.26 = 289.77; used rows above already charged | E · Go capacity replay, N |
| Shards, per-generation count maps, empty in-flight maps, fixed scaffolding | ~0.23 combined | Residual to rounded 528.2; individual subparts unmeasured, not charged as zero | E · N |
| **Total, excluding names and encoded point buffers** | **528.2** | 208 + 30.20 + 240 + 49.77 + 0.23 | L · IT; E · decomposition |

| Further cut | Estimated saving B/metric | Confidence / dependency | Kind · source |
| --- | --- | --- | --- |
| Compact staged points, keeping 8 B ref | ~109–144 at ten staged pt/metric | Medium-low: replace raw 16 B/pt with 1.58–5.10 B/pt; compression of interleaved/short staging not measured | E · CG, IT, N |
| Staging buffers sized to observed batch | Up to 49.8 | Medium upper bound: remove capacity slack, trading growth/copy/CPU; requires real cycle distribution | E · N |
| Item-map 75% maximum load | ~15.1 at this fixture, 30.2 → 15.1 | Medium capacity arithmetic: half-sized power-of-two tables, 59.6% actual load; probe/collision latency unmeasured | E · code, N |
| Move cold item fields into sparse side storage | ~24–48 gross | Low: firstArrival 24, throttle interface 16, index generation 8 B; side-table cost and allocator class may erase gain | E · code, N |
| ID-keyed item + registry | ~16–32 gross item headers, plus duplicate name savings | Low: name header 16 and duplicated StreamRef header 16; registry resolution/ref/generation cost must be subtracted | E · code, NR, N |
| Reduce staged rows by faster log drain | 24 × fewer staged pt/metric, before slack | High arithmetic, unmeasured operational effect; retain sync/replay/loss semantics | E · IT, N |
| Queued-generation / shard maps | At most ~0.23 outside item in this fixture | High upper bound; inline 9 B already included; production in-flight map is workload-dependent | E · N |

Capacity replay uses the exact benchmark names/hash distribution and Go 1.27 slice growth, without allocating the full cache; residual includes benchmark rounding, so its subparts are not false precision. [N]

## CPU by phase: 30 s production profiles

| Cores/host | D1 | D2 | D3 | D4 | D5 | Kind · source |
| --- | --- | --- | --- | --- | --- | --- |
| All sampled process CPU | 11.6 | 8.61 | 7.28 | 8.88 | 2.79 | P · S/C, N |
| Receive/decode INCLUDING insertion | 5.08 | 3.17 | 2.36 | 5.14 | 1.41 | P · S/C, N |
| ↳ Cache insert stack subset; do not add | 4.43 | 2.63 | 1.85 | 4.74 | 1.28 | P · S/C, N |
| Persister / Pebble update path | 2.05 | 1.76 | 2.22 | 2.4 | 0.641 | P · S/C, N |
| Writeout queue | 0.0877 | 0.123 | 0 | 0 | 0 | P · S/C, N |
| Pebble compaction label | 0 | 1.11 | 0.027 | 0 | 0.139 | P · S/C, N |
| Pebble flush / WAL-sync labels | 0.213 | 0.236 | 0.23 | 0.194 | 0.0127 | P · S/C, N |
| Carbonserver render, includes glob expansion | 1.95 | 0.794 | 0.514 | 0.0797 | 0.073 | P · S/C, N |
| Find/glob stack subset; overlaps render | 0.106 | 0.002 | 0.005 | 0.0183 | 0.00133 | P · S/C, N |
| Index build/refresh label | 0.0737 | 0.0797 | 0.562 | 0.001 | 0 | P · S/C, N |
| Input-log label | 0.172 | 0.17 | 0.0967 | 0.376 | 0.0447 | P · S/C, N |
| GC stack subset, sampled window only | 0.0237 | 0 | 0 | 0 | 0 | P · S/C, N |
| Automatic WriteoutState stack subset | 0.00033 | 0.00067 | 0.00067 | 0.00033 | 0 | P · S/C, N |

| CPU ns/accepted pt; includes fixed workload | D1 | D2 | D3 | D4 | D5 | Kind · source |
| --- | --- | --- | --- | --- | --- | --- |
| All sampled process CPU | 4,060 | 2,990 | 6,800 | 2,460 | 3,220 | P · S/C; E · normalization |
| Receive/decode INCLUDING insertion | 1,780 | 1,100 | 2,200 | 1,420 | 1,620 | P · S/C; E · normalization |
| ↳ Cache insert stack subset; do not add | 1,550 | 914 | 1,720 | 1,310 | 1,480 | P · S/C; E · normalization |
| Persister / Pebble update path | 718 | 610 | 2,070 | 664 | 740 | P · S/C; E · normalization |
| Writeout queue | 30.6 | 42.8 | 0 | 0 | 0 | P · S/C; E · normalization |
| Pebble compaction label | 0 | 387 | 25.2 | 0 | 160 | P · S/C; E · normalization |
| Pebble flush / WAL-sync labels | 74.5 | 82.1 | 214 | 53.9 | 14.6 | P · S/C; E · normalization |
| Carbonserver render, includes glob expansion | 682 | 276 | 479 | 22.1 | 84.2 | P · S/C; E · normalization |
| Find/glob stack subset; overlaps render | 37.1 | 0.695 | 4.67 | 5.08 | 1.54 | P · S/C; E · normalization |
| Index build/refresh label | 25.7 | 27.7 | 525 | 0.277 | 0 | P · S/C; E · normalization |
| Input-log label | 60.1 | 59.2 | 90.2 | 104 | 51.5 | P · S/C; E · normalization |
| GC stack subset, sampled window only | 8.27 | 0 | 0 | 0 | 0 | P · S/C; E · normalization |
| Automatic WriteoutState stack subset | 0.117 | 0.232 | 0.622 | 0.0924 | 0 | P · S/C; E · normalization |

Labels are inclusive work groups; stack subsets overlap them; Pebble labels are independent, and unlabeled/runtime work remains in total; persister stage-seconds are wall time, not cores. [S/C]

Most windows sample no GC cycle; this does not imply zero long-run GC cost; alloc_space totals 37.4 / 26.5 / 25.0 / 20.9 / 7.43 TB are cumulative allocation churn, not retained memory or a 30 s rate. [P · S/H]

| Other phase / unit | Cost | Scope / boundary | Kind · source |
| --- | --- | --- | --- |
| Gorilla full cache lifecycle | +0.085 core at 3M pt/s (~28 ns/pt extra) | Includes ingest, pop/decode, confirm; do not add isolated append/decode costs | L · CG |
| Name registry raw / Zstd ingest | 92.5 → 85.3 / 104 → 103 ns/pt | Median local CPU, including GC; no measured increase, no server speedup claim | L · NR |
| Auto sampler idle, 4M metrics | 153 ms → 84.6 µs CPU/sample; ~0.0307 → 0.0000169 core at 5 s cadence | Loaded wall 230 ms → 398 µs; mutation overhead remains | L · AS; E · core conversion |
| Cold-pack / relayout in these snapshots | 0 sampled cores | Both inactive; no per-point maintenance-cost measurement in these windows | P · S/C |
| Exact cold-pack sweep | 0.760 CPU-s / 6.52M packed pt ≈116 ns/pt; 25.2 CPU-s/original GB | Small synthetic population; scan/codec and concurrent work inside sweep, final compaction excluded | L · CY; E · division |
| Cold-pack parallel sweep | 1.18 → 2.25 process cores at 1 → 8 workers | Local 33.6M-point fixture; shared CPU ceiling workers × duty fraction; not host throughput | L · CP |
| Relayout v6 → v7 | 0.772 / 1.56 / 1.69 / 3.00 cores at 1/2/4/8 workers; 8.20 / 7.83 / 10.6 / 12.6 CPU-ns/moved B | 791 MB real-codec fixture; deferred compaction excluded; no measured ns/pt | L · RL |
| Restart cold metadata + tail reads | Historical D1 17 → 77, D2 13 → 56, D3 9 → 74 cores | 2026-10-29 spikes, ~4–12 min; not the steady snapshot nor current-release restart proof | P · MR |
| Restart prewarm | ~56,000 ns/metric versus ~324,000 cold-path ns/metric | Historical windowed comparison; snapshot checkpoint-loader CPU unmeasured while idle | P · MR |
| Metadata templates versus Snappy | ~+0.020 core, +10.6 MB/s allocations | Proxy at 14k hits/s + 12k installs/s, avoids double-counting update hit | E · MC |

## Write I/O: rates and amplification

| Per host, ~30 s window | D1 | D2 | D3 | D4 | D5 | Kind · source |
| --- | --- | --- | --- | --- | --- | --- |
| Committed updates/s | 10,700 | 10,800 | 16,800 | 6,780 | 5,100 | P · S |
| Committed M pt/s | 2.84 | 2.55 | 1.16 | 3.62 | 0.785 | P · S |
| Committed pt/update | 267 | 235 | 69 | 534 | 154 | P · S; E · ratios |
| Pebble WAL MB/s | 11.6 | 22.9 | 40.8 | 8.49 | 4.6 | P · S; E · ratios |
| Pebble table MB/s | 19 | 150 | 23.3 | 19.2 | 31.3 | P · S; E · ratios |
| Completed flush MB/s | 0 | 24.9 | 0 | 0 | 0 | P · S; E · ratios |
| Compaction write MB/s | 0 | 126 | 3.2 | 0 | 30.2 | P · S; E · ratios |
| Input-log MB/s | 5.23 | 2.63 | 2.1 | 9.49 | 0.9 | P · S; E · ratios |
| Device write MB/s | 17.4 | 176 | 71.2 | 37.9 | 37.9 | P · S; E · ratios |
| Device write IOPS | 154 | 1,460 | 593 | 395 | 322 | P · S |
| Device write await ms | 0.338 | 1.02 | 1.57 | 0.283 | 0.54 | P · S |
| Pebble filesystem write calls/s, not IOPS | 5,920 | 38,700 | 9,350 | 5,400 | 8,080 | P · S |
| Device written B/committed pt | 6.1 | 69 | 61.2 | 10.5 | 48.2 | P · S; E · ratios |
| Device written B/update | 1,630 | 16,200 | 4,230 | 5,590 | 7,420 | P · S; E · ratios |

Device rows use the data NVMe, or the md data array for D4; never sum md + members, partitions + whole disks, or NVMe namespace aliases. [S/D]

Filesystem writes count bytes passed to the kernel; device writes can lag buffered SST/WAL/input-log writes and include other same-device activity. [E · S/D, code]

Completed flush/compaction counters jump at job completion; several windows have 0 completed flush bytes despite active table writes, so short-window write amplification is undefined, not zero. [S]

| Amplification / control | Cost or formula | What sets it / trade-off | Kind · source |
| --- | --- | --- | --- |
| Snapshot D2 completed compaction / flush | 126 / 24.9 = 5.07×; total SST/flush ~6.07×; compaction share ~83.5% | Completion-accounted 30 s ratio; not steady-state WAL-to-device amplification | P · S; E · ratio |
| Historical D3 flush / compaction | 27 / 258 MB/s, 9.56× compaction/flush, 90.5% of flush+compaction | ~30k updates/s × ~33 pt; ~285 B/pt or ~9.5 kB/update SST traffic at ~1M pt/s | P · MM; E · ratios |
| Historical write-byte mix | WAL 19%, flush 12%, compaction 69%; flush ~1.0 kB/update + 9.7 B/pt | Earlier 2026-10-25 phase; not additive to current snapshot counters | P · MS |
| `max-updates-per-second` | All snapshot classes −1 (auto); ceilings 22k / 23k / 26.5k / 12.6k / 12k updates/s | Caps writes, not points; backlog can bypass ordinary limits; restart has its own admission | P · S; E · code |
| `writeout-min-points` / `writeout-max-delay` | 960 pt / 960 s; D3 960 pt / 1,800 s | Sparse update floor ≈cached metrics / delay; longer delay raises cached points/log/restart burden | P · S, MM |
| Coalescing A/B, D5 | 480 → 960: ~3× pt/write, CPU −25–41%, flush −50%, compaction −30–73%, IOPS −30–71%, heap +~4 GB | 2026-10-25 matched-load result; tail-block rewrite/catalog cost is per update | P · MW |
| Device ceiling / trim | Untrimmed compaction ~580 → 930–1,040 MB/s after trim; device writes ~750 → 1,200 MB/s | Measured ceiling under compaction, not a device rating; daily trim shipped; SSD internal GC matters | P · MT |
| Device IOPS rating | Unmeasured | Small-write latency/read p99 under mixed load needed; 30 s sampled IOPS is demand, not a limit | E · S, DS |

## Read I/O

| Per host | D1 | D2 | D3 | D4 | D5 | Kind · source |
| --- | --- | --- | --- | --- | --- | --- |
| Pebble table reads MB/s | 19.4 | 152 | 8.96 | 13.9 | 40.7 | P · S |
| Compaction reads MB/s, subset of table | 0 | 146 | 3.46 | 0 | 39.7 | P · S |
| Physical device reads MB/s | 1.86 | 0.00545 | 0.23 | 0.00409 | 0.00409 | P · S |
| Physical device read IOPS | 152 | 1.13 | 21.6 | 0.998 | 0.999 | P · S |
| Block-cache hit % | 99.3 | 52.4 | 93.2 | 97.1 | 34.1 | P · S; E · hit ratio |

Block-cache hit rate is per lookup across all users; compaction can bypass it; filesystem reads can hit OS cache, so neither misses nor filesystem bytes equal physical read I/O. [S]

| Read unit | Cost | Boundary | Kind · source |
| --- | --- | --- | --- |
| Production D2 render replay | +0.65 core / +12.3 successful renders/s ≈53 ms CPU/render; ~1.70 MB returned/render | Whole-process marginal estimate; 76 GB/44,687 requests; extra working set and ingest not replayed | P · DR; E · division |
| Current render physical bytes/request | Unmeasured | Need request-attributed read bytes with compaction/persister separated; aggregate device bytes/render is not attribution | E · S |
| Packed v9 cold point Snappy → Zstd | p50 108 → 223 µs; CPU 194 → 306 µs/query | Local OS-cache-warm, Pebble-cache-cold, single reader; not production device latency | L · PZ |
| Packed v9 warm point Snappy → Zstd | p50 23.5 → 21.7 µs | Local all-hit working set; p99 noisy | L · PZ |
| region 3 longer-window device read demand | D1 p99 ~34–38 MB/s; other sampled classes often ~0 | 2026-11-02 10:00–16:00, 2 s samples; complements short quiet snapshot | P · DS |

## Network

| Per host; whole NIC, not socket-attributed | D1 | D2 | D3 | D4 | D5 | Kind · source |
| --- | --- | --- | --- | --- | --- | --- |
| Ingress MB/s | 8.42 | 20.3 | 15.6 | 8.32 | 1.91 | P · S/N |
| Egress MB/s | 10.7 | 15.5 | 0.549 | 0.878 | 0.763 | P · S/N |
| Ingress B/accepted pt | 2.94 | 7.04 | 14.6 | 2.31 | 2.21 | E · S/N normalization |
| Input-log B/logged pt | 1.83 | 0.915 | 2.03 | 2.64 | 1.04 | P · S; E · ratio |

Use bond totals once; slave links and loopback would double count traffic; NIC B/pt includes responses/control/framing and does not identify raw versus Zstd stream payload. [S/N]

| Wire / transfer format | B/pt or transfer cost | Boundary | Kind · source |
| --- | --- | --- | --- |
| Plaintext production reference | ~240 B/pt | Long names dominate; distribution-dependent, not a codec constant | P · MN |
| Local stream lifecycle, plaintext | 271 B/output pt | 813 B/input with three replicated outputs; generated names and values | L · SC; E · division |
| Local stream codec, raw | 3.01 B/pt | Name-registry ingest fixture, unusually regular data; includes amortized definitions | L · NR |
| Local stream codec, Zstd | 2.78 B/output pt | 8.33 B/input across three outputs, 42M-input full-cycle fixture | L · SC; E · division |
| Cold handoff tail, legacy → negotiated stream | 210 → 9.6 B/pt | 1.85M names/points, cold dictionary; transfer is local handoff-file I/O, not steady NIC | L · RC |
| Store-history transfer | 2.1 GB/s historical copy; ~64 min total backfill | Offline source note; copied SST/network demand separate from normal ingress | P · MB |
| Render compression | 1.672 MB protobuf → 0.420 MB gzip → 0.211 MB Zstd | Local response fixture; actual egress shares/negotiation unmeasured | L · CI |

## Disk space: formats, levels and supporting files

| GB/host, v6-default snapshots | D1 | D2 | D3 | D4 | D5 | Kind · source |
| --- | --- | --- | --- | --- | --- | --- |
| Pebble total | 2,070 | 1,080 | 1,610 | 509 | 979 | P · S |
| L0 | 0.661 | 0.715 | 0.776 | 0.546 | 0.128 | P · S |
| L1 | 3.71 | 0.00426 | 0.0607 | 0 | 0.0524 | P · S |
| L2 | 11.2 | 0.435 | 0.471 | 0.0605 | 0.357 | P · S |
| L3 | 39.6 | 2.97 | 3.51 | 0.525 | 2.39 | P · S |
| L4 | 135 | 17.7 | 24.9 | 4.5 | 15.5 | P · S |
| L5 | 420 | 95.6 | 150 | 37.4 | 113 | P · S |
| L6 | 1,440 | 940 | 1,410 | 448 | 829 | P · S |
| WAL live | 2.04 | 0.797 | 2.5 | 2.14 | 1.62 | P · S |
| Input log on disk | 12.9 | 8.59 | 12.9 | 12.9 | 8.59 | P · S |
| Metadata checkpoint loaded bytes | 32.9 | 41.6 | 93.5 | 10.5 | 11.8 | P · S |

Pebble total includes non-level files: level bytes need not sum to it; loaded checkpoint bytes describe the last restart, not a current independent directory-size measurement. [S]

| Format / object | B/pt | GB/host or per-metric cost | Scope / qualification | Kind · source |
| --- | --- | --- | --- | --- |
| v6 native continuation blocks | Unmeasured production durable-point denominator | Snapshot totals above; uncompacted D1 comparison 1,731.8 GB | Encoded point groups and descriptors; no universal B/pt without retained-point count | P · S, CC |
| v7 time buckets, no packing | 2.16 local settled SST | Fully compacted D1 1,559.3 GB | v6 → v7 open-time switch; one-way; local and host rows use different populations | L · PZ; P · CC |
| v9 exact cold-pack, Snappy SST | 2.12 local settled SST | Fully compacted D1 1,167.2 GB; −25.1%, −24.5%/active series | Packs eligible time-bucketed blocks; not all v6 history; canary-host measurement | L · PZ; P · CC |
| v7, Zstd on L5/L6 | 2.08 versus 2.16 Snappy | Local 3.86 → 3.73 GB SST; −3.6% | All settled output at L6; upper-level churn unmeasured | L · PZ |
| v9, Zstd on L5/L6 | 1.90 versus 2.12 Snappy | Local 3.80 → 3.39 GB SST; −10.6% | Native codec unchanged; outer SST codec changes | L · PZ |
| Schema sensitivity, v7 model | 2.09–2.68 for ordinary dense policies; 5.92 for 1d:10y | Logical group values ~1.78–2.17 B/pt, keys ~0.33–1.07, headers/buffers ~0.037–6.80 | Physical referenced points include expired history; local schema mix, not current host B/pt | L · CY |
| Catalog / revision / manifests | Not point-proportional | ~194–206 / 33 / 113–135 B/metric logical | Before shared-SST compression and repeated LSM versions | L · CI/I |
| Writer metadata | Not point-proportional | Fallback / minutely **2,136 / 3,792 B/entry raw**, **1,484 / 2,221 Snappy**; header pages alone **1,215 / 2,735 B** | Real-writer synthetic histories; rollup buffers and retention layout dominate; ranked shrink options in HS | L · HS |
| Metadata checkpoint | Record bytes + 8 B/entry framing, plus 60 B/file | 10.5–93.5 GB loaded in snapshots | No dead arena capacity or hash slots on disk; packed records shrink future checkpoints | P · S; E · MD/code |
| Discovery FST / file-list gzip / MANIFEST | Unmeasured B/indexed metric | Resident `.fst` 1.04 / 0.470 / 0.800 / 0.221 / 0.157 GB | Mapping RSS is not file size; U-I for file lengths, `.meta`, manifest and file-list bytes | P · S/W; U-I · code |
| Trim | No logical B/pt saving | Releases discarded filesystem extents to SSD; compaction 580 → 930+ MB/s | Daily trim released; not equivalent to reducing live Pebble bytes | P · MT |

## Restart: sizes, gaps, load and CPU

| Last restart metric; not concurrent stages to sum | D1 | D2 | D3 | D4 | D5 | Kind · source |
| --- | --- | --- | --- | --- | --- | --- |
| Cache dump + input tail GB | 4.42 | 4.62 | 1.97 | 4.11 | 1.39 | P · S |
| Stop seconds | 38.7 | 42.3 | 94.8 | 13.7 | 21.5 | P · S |
| Start seconds | 2.16 | 1.44 | 2.69 | 0.872 | 0.989 | P · S |
| Input unavailable seconds | 2.81 | 1.63 | 2.92 | 1.05 | 1.29 | P · S |
| Reads unavailable seconds | 2.64 | 1.56 | 2.8 | 0.978 | 1.22 | P · S |
| Cache restore seconds | 51.7 | 49.9 | 71.5 | 18.6 | 20.5 | P · S |
| Metadata prewarm wall seconds, accumulated | 2.92 | 73.1 | 152 | 5.46 | 38.5 | P · S |

Snapshot dump B/pt and metadata checkpoint load time are unmeasured: current cache occupancy is not the restored point count, and prewarm duration is not checkpoint-load duration. [S]

| Restart object / phase | Before → after or measured cost | Scope / caveat | Kind · source |
| --- | --- | --- | --- |
| Earlier production cache dump | 5.3045 GB / 916.27M records ≈5.79 B/pt; ~2.5 s encode | Earlier supplied production observation; snapshot dump+tail metric has a different numerator | P · CI/I; E · ratio |
| Earlier production tail | 377.67 MB / 1.84856M pt ≈204 B/pt; 14.6 s load, input gap ~10–16 s | Source called it plaintext; code writes legacy binary; 14.6 s not reproduced locally | P · CI/I; E · ratio |
| Negotiated stream tail | 210 → 9.6 B/pt; ready 639 → 480 ms | Local tail admission; excludes checkpoint open and tail encoding/sealing; ≤2 B/pt target missed | L · RC |
| Compact → stream cache dump | 5.88 → 2.17 B/pt; encode 243 → 340 ms; restore 709 → 497 ms | 50M-pt local fixture; encode +40%, restore −30%; ≤1.6 B/pt target missed | L · RC |
| Stream checkpoint open | 72 → 272 ms; ~294 MB expanded heap for 50M points | File maps compressed data, reader retains expanded compact bytes; stream not mmap-only | L · RC |
| Metadata checkpoint ~1 kB shape | 28.3 GB load 40.7 → 13.0 s | Independent-window sums, 28M entries, 4 workers / 8-core allowance; not full-host restart | L · MD |
| Metadata checkpoint ~2.6 kB shape | 88.7 GB load 143 → 33.1 s | Independent-window sums, 34M entries, 8 workers / 23-core allowance; laptop paging present | L · MD |
| Restart CPU pacing | Historical 4–8× CPU spike; latest snapshot restart peak unmeasured | Record first 15 min process/phase CPU and writeout floor during a future authorized restart | P · MR; E · S |
| Reader/rollback boundary | Negotiated tail falls back for old peers; stream dump is opt-in | Old binaries can ignore crash-leftover stream dumps: recover/convert with capable binary first | L · RC |

## Threads and native allocation limits

| Per serving worker | D1 | D2 | D3 | D4 | D5 | Kind · source |
| --- | --- | --- | --- | --- | --- | --- |
| Worker `/proc/task` count, later sample | 264 | 203 | 210 | 207 | 199 | P · S/W |
| Go-created OS threads | 265 | 204 | 211 | 208 | 200 | P · S |
| Goroutines | 1,360 | 1,295 | 1,004 | 1,643 | 1,366 | P · S |
| Go stack-inuse MB | 48 | 38 | 38 | 48.4 | 48.4 | P · S |
| Configured max CPU / persister / compaction workers | 192 / 96 / 16 | 192 / 96 / 16 | 192 / 96 / 16 | 192 / 96 / 16 | 192 / 96 / 16 | P · S |

`go_threads` reports OS threads created by Go; the later worker `/proc/task` count is now measured separately; goroutines are multiplexed and neither count equals busy cores. [P · S, S/W]

`GLIBC_TUNABLES` caps malloc arenas at 64 with 67.1 MB top_pad and malloc trim every 5 min; arena count/resident bytes and per-native-thread stack RSS remain unmeasured; `[heap]` alone is not all malloc residency. [P · S]

## Historical reduction experiments: 2 November 2026

| Change | Before → after, unit and scope | CPU / compatibility / limitation | Status on 2 November 2026 | Kind · source |
| --- | --- | --- | --- | --- |
| cache-gorilla historical experiment | Point storage **5.10 → 1.58 B/pt** (≤1.6 target met); whole-cache heap **7.70 → 4.26 B/pt**; mean **105 pt/metric**, **70% integer** | Metadata + encoder state **56 → 80 B/series** remain; full lifecycle **+0.085 core at 3M pt/s**; **30% random-mantissa** mix **2.96 B/pt**; **no durable format changes** | Local result; **canary on one D1 host since 2026-11-02 22:44 CET, results pending** | L · CG; P · N (supplied canary) |
| name-registry historical experiment | Fixture heap **−22.8%**, **−167 B/name**; **3.66 → 2.82 GB at 5M names** | `[cache] shared-name-registry` **default off**; identities alone **+14 B/name**, pays through sharing; ingest CPU no increase; formats unchanged | **Local**; metadata/index integration not included | L · NR |
| restart-codec historical experiment | Handoff tail **210 → 9.6 B/pt**; opt-in stream dump **5.88 → 2.17 B/pt** | Negotiated, old peers get legacy; restore **−30%**, encode **+40%**; misses **≤2 / ≤1.6** targets; production **14.6 s** tail load not reproduced; **crash rollback with stream dumps unsafe for old binaries** | **Local**, experimental stream dumps | L · RC; P · CI/I (earlier load) |
| cache-items historical experiment | Bookkeeping **660 → 528 B/metric (−20%)**, **−660 MB at 5M**, **≈−1.1 GB/host at 8.07M**; map **16 → 9 B/slot**, staging **32 → 24 B/staged pt** | Item **184 (192 allocated) → 208 B**; no durable codec changes; exact breakdown above | **Local**; host saving is a live-heap estimate | L · IT; E · host scaling |
| Metadata pack `store-metadata-cache-pack` | Cache **42.9 → 33.3 GB (−22%) at 20.08M entries**; **RSS 224 → 203 GB** | Existing lossless Snappy path; future reseals/coverage matter; no universal RSS multiplier | **Canary on one D2 host since 2026-11-02 21:56 CET** | P · N (supplied canary) |
| pebble-zstd historical experiment | v7 **2.16 → 2.08 B/pt (−3.6%)**; packed v9 **2.12 → 1.90 (−10.6%)** | **+0.34–0.51 cores** compaction for v9 at 50–75 MB/s input; cold point p50 **108 → 223 µs** | **Local**, opt-in L5/L6 Zstd | L · PZ; E · core projection |
| Exact cold-pack | Fully compacted D1 **1,559.3 → 1,167.2 GB Pebble (−25.1%)**, **−24.5%/series** | Compare equally compacted v7/v9; eligible bucketed history only; fleet-wide conversion not established | **Canary host**, measured 2026-11-02; most fleet v6 at 19:00 | P · CC, MF |
| GOGC 150, D2 / D1 | **RSS −11.5% D2 / −7% D1; CPU unchanged** | Snapshot confirms GOGC=150 for D2/D1; before/after figures supplied, not re-derived from one window | **Canary-host result; setting present on both sampled workers**, fleet release scope unmeasured | P · N, S |
| metadata-compact historical experiment, final RESULT | Snappy → templates logical **720 → 565 B/entry**, heap **785 → 627 B/entry**; modeled extra **4.23 / 5.45 GB** D1/D2 beyond Snappy | Raw logical **1,233**; IDs **564**, both **406 B/entry**; local registry adds **~292 B/name**: ID heap **917**, both **754**, so IDs alone lose | **Local**; templates/IDs default off, shared registry integration required for ID savings | L/E · MC |

Local reductions overlap and use different populations: do not sum percentages or scale synthetic B/pt into an RSS promise; packed metadata can admit more entries at unchanged budgets. [CG, IT, NR, MC]

## Unmeasured gaps and next collection

| Gap | How to measure without guessing | Source / kind |
| --- | --- | --- |
| Go versus native C within anonymous RSS | Worker RSS/PSS/anonymous/file/`.fst`/task count are filled; collect aligned Go gauges, full per-VMA smaps and malloc arena statistics for the remaining split | S/W limitation · U |
| Long-run per-phase CPU / GC / ns per render | Repeat aligned CPU profiles over full GC and compaction cycles; retain accepted/committed points and successful request counts | S/C limitation · E |
| Stable write amplification and IOPS limit | Integrate WAL/flush/compaction/fs/device deltas over settled cycles; identify maintenance windows; mixed-read/write device qualification | S/D, DS limitation · E |
| Host B/durable point by v6/v7/v9 | Count physical referenced points per archive on a local store copy; separate SST/WAL/catalog/header sizes and expired history | CY limitation · E |
| Render-attributed physical reads / NIC share | Attribute filesystem/read calls by render versus persister/compaction and count compressed response bytes at request boundary | S, DR limitation · E |
| Current restart CPU peak, metadata load time, dump B/pt | Retain matched serialized point counts, checkpoint stage logs, input gap and first-15-min worker profiles | S, MD, RC limitation · E |
| 528 B/metric production transfer | Measure staged-row and capacity distributions, in-flight generations, map load and allocator class at equal cache occupancy | IT, N limitation · E |

## Evidence scope

All dates below are in 2026. Raw captures are unavailable in this repository;
source IDs retain the type, date and measurement scope, not a reproducibility
claim. Experimental rows are historical and do not establish current defaults,
feature availability or rollout status. Public implementation references:
[storage design](README.md), [cold-pack](../cold-pack.md),
[index snapshots](../index-snapshots.md), and [storectl](../../tool/storectl/README.md).

| ID | Evidence type, date and scope |
| --- | --- |
| S | Five production stores, D1–D5; 2 Nov 22:47–22:51 CET; paired metrics and timestamps, configuration and device/filesystem inventory. |
| S/H · S/C | Same stores/date: retained and allocated heap profiles; 30 s CPU profiles with labels/stacks. |
| S/W | Later worker status, smaps, I/O, tasks and mappings received 2 Nov ~23:11 CET; capture time unavailable. Truncated MiB converted to decimal GB. |
| S/D · S/N | Paired device-sector and bond-network counters; 512 B sectors, deltas divided by elapsed time, supervisor excluded. |
| N | Derived formulas, capacity replay, supplied canary/GOGC observations and normalized profile tables, 2 Nov. |
| CG · IT | Local cache codec and bookkeeping experiments, before/after heap and CPU; 2 Nov. |
| NR · RC | Local name-registry ingest and restart dump/tail experiments; 2 Nov. |
| MC | Local million-entry metadata raw/packed/templates/IDs/combined experiments; 2 Nov. |
| HS | Metadata arena and writer-header accounting, prototype CPU/size; 3 Nov. |
| PZ · CC | Local SST compression experiments; equally compacted production cold-pack comparison, 2 Nov. |
| CI · CI/I | Local identity/response format experiments and an earlier supplied production fixture, distinguished in each row; 2 Nov. |
| CY · CP · RL | Local cold-pack yield, parallel cold-pack and parallel relayout fixtures; 2 Nov. |
| AS · MD | Local sampler CPU and metadata-checkpoint loading experiments; 2 Nov. |
| SC · DR · DS | Stream CPU fixture, 1 Nov; production render replay, 2 Nov 19:28–20:28 CET; device samples, 2 Nov 10:00–16:00 CET. |
| MI · MW · MM | Historical production memory/CPU, 29–30 Oct; coalescing, 25 Oct; write amplification, 30 Oct. |
| MS · MT · MR | Historical write-byte mix, 25 Oct; trim, 25–29 Oct; restart CPU, 29–30 Oct. |
| MF · MN · MB | Historical fleet formats, 2 Nov; plaintext network, 31 Oct; store backfill, 30 Oct. |
| IX · WC · CM | Public index-snapshot design; local whole-store and metric-range transfer fixtures, 1–2 Nov. |
| code | Implementation boundary estimates: cache, stream codec, index snapshots, chunkstore, recovery, points, storecopy and handoff. Historical experiments may differ from the current implementation. |
