# carbon-rs

Native Rust implementation alongside the unchanged Go daemon. This is a compatibility-tested initial implementation, not yet a production-qualified replacement for every go-carbon configuration.

## Build and check

Requires Rust 1.89+ on Unix and the Go version from the repository's `go.mod` for the test oracle. The daemon itself does not invoke Go, use FFI, or depend on Go libraries.

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
- HTTP carbonserver: JSON and carbonapi v2/v3 protobuf find/render/info/list/details, list-query, capabilities, force-scan, quota/admin inspection, and `/metrics`. Protobuf transport is HTTP only. Trie and trigram discovery, realtime cache-only metrics, disk reconciliation, gzip FLC v1/v2, bounded query/find caches, concurrency and request limits.

Storage schema and aggregation files retain their Graphite INI syntax; the main configuration is TOML. Select the trie with `trie-index=true`, or the trigram backend with `trie-index=false`. The catalog is updated synchronously on admission, so there is no lossy realtime notification queue.

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
