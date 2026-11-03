# Range reads of sealed continuation blocks

`[whisper] store-range-reads = true` enables time pruning during Fetch of v9
cold-packed continuation blocks. The default is false; changing it requires a
SIGHUP reload. No format version, keys, descriptors or stored values change. Disabling
it restores the full reader for subsequent Fetch calls; existing
v6/v7 and unsealed blocks always use that reader.

The compressed Whisper header already prunes native blocks. A selected native
block can contain several independently compressed 4,096-point sealed groups.
Previously Fetch decoded and allocated the whole native block before discarding
points outside the requested range. The optional `StorageBlockRangeReader` hook
passes the inclusive physical time window through `storedPoints` to the
continuation reader. Whisper still owns archive selection, circular slots,
out-of-order precedence, live rollups and final clipping.

The reader visits the same ordered group keys once. It checks their existing
16-byte headers, deriving an exact maximum from first/count/step when timestamps
are implicit. An irregular group has only a first timestamp in its header: its
maximum is conservatively `MaxUint32`, so groups before the window still decode.
Groups after the window can be skipped; the final irregular group still decodes
to validate the native block's final timestamp. Groups overlapping the window
receive the existing complete decompression, count/timestamp validation and CRC
check. Only their selected points are copied into the result. A window covering
the complete native block uses the existing full path.

This saves decode CPU and transient allocations, not Pebble read bytes. Values
are still visited, including values whose codec work is skipped. There is no
subgroup index, split compression frame or additional metadata cache. A query
inside one sealed group still decompresses that entire group. A new persistent
time directory or smaller subchunks would be a separate space/CPU trade-off.

The flag also changes corruption detection: skipped payloads are not decompressed
or checked by the sealed payload CRC. Their structural headers, group ordering,
counts and available outer time bounds are checked; Pebble retains its ordinary
SST checksums. Full-block reads used by writer planning, thawing, verification,
snapshots and transfers retain complete validation. This behavior change is why
the optimization remains opt-in despite having no durable-format change.

## Local qualification

`continuation_range_test.go` compares physical results to full reads for regular,
gapped and off-grid timestamps, special float bits, partial final groups,
ordinary groups, stale keys from circular reuse, random windows and malformed
inputs. A second oracle drives actual archive selection and v9 Fetch. The config
test pins default-off, parsing and SIGHUP reload detection.

`BenchmarkContinuationRangeFetch` drives the real writer and cold packer using
`1s:2d,60s:28d,1h:2y`, with deterministic numeric values and 700 days of history.
It is production-shaped data, not captured production values. Paired runs use
identical persisted data, shared fetch iterators, a warm Pebble cache, stopped
store background workers and two Go CPUs. `ns/op` is storage Fetch latency;
`cpu-ns/op` is process user+system CPU including GC. This does not measure HTTP
serialization, response-cache hits, physical I/O or server p99.

`BenchmarkContinuationRangeBlock` additionally isolates 7,200-point and maximum
65,536-point native ranges, reporting decoded points, decoded point-array bytes
(16 bytes/point) and encoded bytes submitted to the sealed decoder. Those bytes
are not physical reads or decompressed codec-body bytes. Supplied production
snapshots contain no sampled `DecodeSealedPoints` work, so their whole-block read
CPU is a ceiling, not a demonstrated v9 saving.

Balanced local medians (six observations per mode, counterbalanced order) showed
1h Fetch CPU 335→288 µs and allocated bytes 587→538 KB per series; 6h CPU
1,082→977 µs and bytes 2,458→2,360 KB. The 24h and 7d CPU differences were only
−1.4% / +1.8%, with small or zero byte savings. Elapsed timings were noisy and
long windows did not improve reliably. The isolated maximum-size native range
reduced 1h decoded points 65,536→4,096, codec-body bytes 524,288→32,768 and
allocations 2.63 MB→104 KB. These are local scope-specific gains, not host claims.
