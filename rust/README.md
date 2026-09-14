# carbon-rs

Native Rust implementation alongside the unchanged Go daemon. This is a compatibility-tested initial implementation, not yet a production-qualified replacement for every go-carbon configuration.

## Build and check

Requires Rust 1.89+ on Unix, a C toolchain for the profiler's native dependencies, and the Go version from the repository's `go.mod` for the test oracle. The daemon itself does not invoke or link Go; CPU profiling uses native stack unwinding.

```sh
make -C rust build
make -C rust check
rust/target/release/carbon-rs --config rust/go-carbon.conf.example --check-config
rust/target/release/carbon-rs --config rust/go-carbon.conf.example
```

Example paths are relative to the repository root. Use absolute paths in deployment. The example binds plaintext TCP/UDP on localhost:2003 and HTTP on localhost:8080.

## Implemented

- `whisper-rs`: classic and compressed v1 `.wsp` create/open/update/fetch, all six standard aggregation methods, retention propagation and XFF, block coding/rotation/growth, live buffers, `.ooo` reads/writes and compaction. Path locks survive atomic replacement; compaction synchronizes the replacement before removing its sidecar. Go file-integrity checks and cross-language reads, writes, appends, and compaction run in tests.
- `carbon-rs`: bounded sharded cache with noop scheduling, pending/in-flight query visibility, exact batch confirmation, failed-write retry, Go-compatible cache dump/restore; bounded TCP framing and UDP datagram parsing.
- Namespace quotas: hierarchical last-matching glob rules, new-metric reservations, namespaces/metrics/archive-capacity/logical and physical storage/throughput limits, observe-only `dropping-policy=none`. Sidecar storage belongs to its main metric.
- HTTP carbonserver: JSON and carbonapi v2/v3 protobuf find/render/info/list/details, list-query, capabilities, force-scan, and quota/admin inspection. Protobuf transport is HTTP only. Trie and trigram discovery, realtime cache-only metrics, disk reconciliation, gzip FLC v1/v2, bounded query/find caches, concurrency and request limits.
- Optional Prometheus exposition using the `prometheus` crate, with Go's application metric names, types, labels, HELP text, and histogram buckets.

Storage schema and aggregation files retain their Graphite INI syntax; the main configuration is TOML. Select the trie with `trie-index=true`, or the trigram backend with `trie-index=false`. The catalog is updated synchronously on admission, so there is no lossy realtime notification queue.

## Logging

`[[logging]]` uses Go's `logger`, `file`, `level`, `encoding`, `encoding-time`, `encoding-duration`, and `sample-*` settings, implemented with `tracing` / `tracing-subscriber`. With no entries, logs go to stdout at info level in console format, as in Go. An explicit entry defaults to stderr/mixed/info. Legacy `[logging]` and `common.logfile` / `common.log-level` are accepted; the common options override the logging entries as in Go.

Outputs are local files, `stdout`, `stderr`, or `none`. A named logger replaces the default route (no prefix inheritance); repeated entries for the same logger write to each destination. Component names include `main`, `tcp`, `udp`, `cache`, `persister`, `whisper:new`, `carbonserver`, `access`, `dump`, `restore`, and `pprof`. Levels are `debug`, `info`, `warn`, `error`, `dpanic`, `panic`, and `fatal`; terminal daemon errors emit FATAL and exit unsuccessfully. Rust panics retain Rust's runtime handling rather than emulating Go panic/DPanic behavior.

JSON has top-level `timestamp`, `level`, `logger`, `message`, and typed event fields. Console uses tab-separated metadata plus JSON fields; mixed uses `[timestamp] LEVEL [logger] message {fields}`. Timestamps support local-time `iso8601`, Unix `epoch` seconds, `millis`, and `nanos`; durations support `seconds`, `nanos`, and Go-style `string`. These are compatible conventions, not byte-identical logs or complete copies of every Go message. HTTP access events include handler, method, URL, peer, status, and `runtime_seconds`; URL/debug-line payloads are bounded, and headers/bodies are not logged. Handler cancellation is recorded as 499 when its future is dropped, not as a guarantee of detecting every client disconnect.

Sampling is opt-in: set positive `sample-tick` (e.g. `1s`), `sample-initial`, and positive `sample-thereafter`. Each output emits the first N occurrences of a level/message pair per interval, then every Mth, with Go's bounded 4096 FNV buckets per level. Files append and reopen after external rename/deletion, checked on writes at most once per second; no idle reopen thread or size-based rotation is added. Writes are synchronous, with file synchronization at shutdown; sink errors fall back to stderr without recursively logging. Use stdout with a log collector, or sampling, when filesystem logging latency matters.

Local `file:/absolute/path` / `file:///absolute/path` and the four level/encoding query overrides are supported. Other URL schemes/query options and unknown logging keys fail validation. `--check-config` validates without creating logs. Logging configuration changes require restart; SIGHUP still reloads storage rules only, and file rotation needs no signal.

## Prometheus

Set `prometheus.enabled=true`. As in Go, `prometheus.endpoint` defaults to `/metrics` on **`pprof.listen`** (default `127.0.0.1:7007`), independently of carbonserver and `pprof.enabled`. The former unconditional carbonserver `/metrics` endpoint and `carbon_rs_*` pipeline counters are replaced. `[prometheus.labels]` adds constant labels to every exported sample; invalid names or collisions with collector labels fail configuration validation.

TCP counts successfully parsed points, including subsequent admission drops; updates are immediate rather than delayed until Go's periodic stats collection. UDP and the ingestion cache have no Prometheus collectors in Go. Whisper write lag observes every point reaching `UpdateMany`, including retries and negative lag for future timestamps. Carbonserver records bounded handler/status labels, cache hits/misses, cache wait/work time, disk fetches, returned series and point slots. Query-cache hits do not increment disk/returned counters. Find hits mean every requested expression was cached; Rust's caches are expression-based and generation-invalidated, so hit rates need not match Go's response caches.

Two intentional fixes to Go instrumentation: `cache_requests_total` actually increments, and `cache_duration_seconds_exp` is registered. Cancellation counts dropped handler futures; timeouts count separately. This does not guarantee observing every disconnected client or stopping already-running blocking disk work.

Linux exports Go's nine `process_*` families plus the crate's `process_threads`. CPU time and process start time use the crate's whole-second precision. The network counters use Go's network-namespace `/proc/self/net/netstat` totals, not per-socket attribution. Process collectors are unavailable on other platforms. `carbon_rs_build_info{version=...}` identifies the actual Rust build; Go runtime/GC/goroutine metrics and Go build metadata are not fabricated.

## CPU profiling

Set `pprof.enabled=true` (default false) on 64-bit Linux. It shares `pprof.listen` with Prometheus but works with Prometheus and carbonserver disabled. `/debug/pprof/` lists the supported CPU endpoint. Keep this unauthenticated diagnostics listener on localhost or a trusted management network: profiles expose stack symbols/source paths, and collection adds overhead.

```sh
curl --fail --max-time 45 -o cpu.pprof 'http://127.0.0.1:7007/debug/pprof/profile?seconds=30'
go tool pprof -top cpu.pprof
```

Profiles are gzip-compressed pprof protobufs with CPU samples at 100 Hz. Release builds retain line-table debug information for symbolization. `seconds` defaults to 30; invalid values or values outside 1–300 return `400`. Only one recording may run per process; overlapping requests return `409`. Collection/reporting runs off the async HTTP workers; dropped handler futures and daemon shutdown cancel collection. An already-running report build must finish. Prometheus endpoints under `/debug/pprof` are rejected when profiling is enabled.

This uses the `pprof` crate's signal-based sampler and recommended system-library exclusions, not Go runtime profiling. Do not run another SIGPROF/ITIMER_PROF profiler in the same process. Validate overhead and unwinding on the deployment platform before production use. Heap/allocations, goroutines, mutex/block profiles, and Go runtime traces are not implemented and return `404`.

macOS CPU profiling is rejected: the native unwinder lost sampled leaf functions in optimized-build qualification, even with frame pointers retained. Debug-build profiles were readable by Go's pprof tool, but this is not sufficient production evidence. The Linux-only recording/cancellation test must pass on Linux before deployment; live Linux sampling has not been validated in the macOS development environment.

## Recovery and rollout

SIGTERM/SIGINT stop admission, finish active writes, and drain the cache. Unpersisted data is saved as a Go-compatible binary dump if draining fails. SIGUSR2 dumps and stops when dumping is enabled. Recovery runs before receivers start; originals are renamed with `.restored` only after successful writes. A pending dump with restoration disabled prevents startup. SIGHUP reloads schema/aggregation rules atomically; other configuration changes require restart.

Validate on a **copy** of the Whisper tree first. Do not run Go and Rust writers against the same tree. Compare query results, quota reports, file-integrity checks, RSS, latency, and sustained ingestion under the intended workload before a canary. To roll back, stop Rust, retain both `.wsp` and `.ooo` files and pending dumps, then start the pinned Go daemon with dump restoration enabled.

## Deliberate limits and remaining qualification

- Only noop scheduling is implemented. gRPC, pickle, and Carbonlink are excluded, along with other receiver protocols.
- Mixed/percentile compressed archive policies and online retention/aggregation migration are not implemented. Files with unsupported aggregation metadata are rejected; they are not converted or overwritten. Standard average/sum/first/last/min/max archives are covered.
- This is not an exhaustive port of every Go configuration key. Nonzero `dump.restore-per-second` is rejected. Use the included configuration and inspect unsupported options before reusing an existing config.
- Invalid UTF-8, empty metric components, filesystem traversal/symlinks, oversized lines, non-finite timestamps, and timestamps outside Whisper's positive u32 range are rejected. Cache limits count in-flight points as well as pending points, and byte limits account for logical payload, not allocator/RSS overhead.
- Full power-loss fault injection, Linux deployment/packaging checks, millions-of-series benchmarks, and sustained Go-versus-Rust performance qualification remain release gates. The included benchmark is a local smoke benchmark, not evidence of a speedup.

```sh
cargo run --manifest-path rust/Cargo.toml --release -p carbon-rs \
  --example ingest_bench -- --metrics 100 --points 100
```

The benchmark traverses the real TCP receiver, cache, and disk writer, checks for drops and a drained cache, and verifies stored samples. The Go receiver test entrypoints were also fixed to execute their tests; previously their `TestMain` functions returned without calling `m.Run()`.

`whisper-rs` retains the vendored go-whisper BSD-3-Clause notice in its `LICENSE`. The daemon follows the repository's MIT license.
