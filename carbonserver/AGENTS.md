<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-04-13 | Updated: 2026-04-13 -->

# carbonserver

## Purpose
Implements the HTTP and gRPC read API for go-carbon, providing Graphite-compatible
metric discovery and data retrieval. Manages a metric index (trigram or trie),
file list cache (FLC), query caches, rate limiting, and quota enforcement.
This is the largest package in go-carbon.

## Key Files
| File | Description |
|------|-------------|
| `carbonserver.go` | `CarbonserverListener` — core HTTP/gRPC server. Registers all HTTP endpoints, manages index lifecycle, file list cache, query caches, rate limiting, and Prometheus metrics. |
| `trie.go` | Trie-based metric index with DFA glob matching. Preferred for 10M+ metrics. Compiles NFA glob patterns to DFA at query time for efficient traversal. |
| `find.go` | `/metrics/find/` endpoint — glob-based metric discovery. |
| `render.go` | `/render/` endpoint — fetches time-series data for matched metrics. |
| `list.go` | `/metrics/list/` endpoint — enumerates all known metrics. |
| `namespace_hashes.go` | `/metrics/namespace-hashes/` endpoint — per-namespace metric counts and CityHash64 lists (NamespaceHashesFile format), computed once per snapshot generation. |
| `details.go` | `/metrics/details/` endpoint — returns metric metadata. |
| `info.go` | `/metrics/info/` endpoint — returns whisper file info (retentions, aggregation). |
| `fetchfromdisk.go` | Reads data points from `.wsp` whisper files on disk. |
| `fetchfromcache.go` | Reads in-flight data points from the in-memory cache. |
| `fetchsinglemetric.go` | Orchestrates disk + cache fetch and merges the results. |
| `flc.go` | File list cache — persists the metric index to LevelDB for fast restarts. |
| `format.go` | Response format handling: JSON, Protocol Buffers, pickle. |
| `pickle.go` | Python pickle encoding for Graphite wire compatibility. |
| `capability.go` | Reports server capabilities to clients. |
| `trace.go` | Per-request tracing support. |
| `trie_test.go` | Unit tests for trie index. |
| `trie_real_test.go` | Integration-style trie tests using real metric sets. |
| `trie_fuzz_index.go` | Fuzz target for trie index construction. |
| `trie_fuzz_query.go` | Fuzz target for trie glob queries. |
| `cache_index_test.go` | Tests for cache-based index operations. |
| `carbonserver_test.go` | General carbonserver tests. |
| `flc_test.go` | File list cache tests. |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Two index backends exist: **trigram** (`dgryski/go-trigram`) and **trie** (`trie.go`). The trie is preferred for large installations (10M+ metrics) and uses NFA-to-DFA compilation for glob patterns. Select via `use-trie-index` config option.
- The index is rebuilt on a `scan-frequency` interval by scanning the whisper root directory. During a rebuild the old index remains live; the new one is swapped atomically.
- The file list cache (FLC, `flc.go`) persists the index to LevelDB so restarts avoid a full filesystem scan. It is keyed by file path with bloom filters enabled.
- Data fetching pipeline: `fetchsinglemetric.go` calls `fetchfromdisk.go` and `fetchfromcache.go` in parallel, then merges, with cache points taking priority over disk points for the same timestamp.
- Rate limiting: `ApiPerPathRatelimiter` limits by HTTP path; `GlobQueryRateLimiter` limits concurrent glob expansions. Both reject with HTTP 429 when the limit is exceeded.
- gRPC is served alongside HTTP on the same port using `grpc.Server` with `carbonapi_v2_grpc` and `carbonapi_v3_pb` protocols. Do not add HTTP-only logic to the gRPC handlers or vice versa.
- Quota enforcement integrates with `persister.WhisperQuotas`; quota checks happen at query time, not just at write time.
- All handler functions follow the pattern: parse request → check rate limit → resolve globs via index → fetch data → encode response in requested format.

### Testing Requirements
```
go test ./carbonserver/
```
Fuzz targets can be run with:
```
go test -fuzz=FuzzTrieIndex ./carbonserver/
go test -fuzz=FuzzTrieQuery ./carbonserver/
```

### Common Patterns
- HTTP handlers use `httputil` wrappers and return structured errors with Prometheus counter increments before returning.
- `metricStruct` holds all internal counters as `uint64` fields updated via `atomic.AddUint64` — never use a mutex to update these.
- Response format is negotiated from the `format` query parameter and the `Accept` header; use `format.go` helpers, do not add ad-hoc format switches.
- Structured logging via `go.uber.org/zap` with request-scoped logger fields (path, remote addr, query).
- gRPC errors use `google.golang.org/grpc/status` codes, not raw `error` returns.
- `QueryItem` with `atomic.Value` and a `QueryFinished` channel implements request coalescing for identical in-flight glob queries.

## Dependencies

### Internal
- `github.com/go-graphite/go-carbon/helper` — `StatCallback`, `Stoppable`, `grpcutil`, `stat`
- `github.com/go-graphite/go-carbon/points` — `Points` for cache reads
- `github.com/go-graphite/go-carbon/helper/grpcutil` — gRPC server utilities

### External
- `github.com/dgryski/go-trigram` — Trigram index backend
- `github.com/dgryski/go-expirecache` — TTL-based query result cache
- `github.com/syndtr/goleveldb/leveldb` — LevelDB for file list cache persistence
- `github.com/go-graphite/protocol/carbonapi_v2_grpc` — gRPC v2 protocol
- `github.com/go-graphite/protocol/carbonapi_v3_pb` — Protobuf v3 protocol
- `google.golang.org/grpc` — gRPC server runtime
- `github.com/NYTimes/gziphandler` — gzip HTTP middleware
- `github.com/prometheus/client_golang/prometheus` — Prometheus metrics
- `go.uber.org/zap` — Structured logging
- `github.com/lomik/zapwriter` — zap writer initialization

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
