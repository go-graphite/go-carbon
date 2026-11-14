# Cached full-history digest prototype

Full-history replica checks currently decode every selected archive and hash
finite `(timestamp, float64 bits)` pairs. `sum-v1` introduces a decomposable
64-bit sum plus finite-point count, with logical time buckets independent of
physical block boundaries. This is a probabilistic replica comparison, not a
cryptographic integrity or authenticity proof.

## Storage and canonical view

`internal/chunkstore` owns Pebble transactions, metric IDs/generations and
revisions. A continuation's generation-local keys are under
`u + ID + generation`; its file identities, headers, block descriptors and
128-point exact groups live there or in the time-partitioned block keyspace.
The descriptor in `internal/whisperio/continuation_blocks.go` selects native
128-point groups or cold-packed groups. Packed group sizes and physical block
boundaries are local storage decisions, not replica-comparison identities.

`Store.Fetch` delegates continuation reads to `ContinuationCodec.Fetch`, which
opens go-whisper over the virtual storage files. Its canonical view resolves
circular-slot ownership, duplicate timestamps, late sidecars and buffered
rollups. Summing physical blocks directly would double-count overwritten points
and miss virtual values. This prototype uses `FetchArchiveWindow` as the oracle
when filling a cache entry, rather than modifying the point writer or format.

```text
archive window -> complete time buckets -> cached uint64 sums
               -> open/sparse/partial buckets -> canonical Fetch -> point mix
                                              -> sum + finite count
```

Each archive's bucket is `floor(timestamp / (8192 * archive step))`, aligned to
Unix epoch. Its key is `ds/ + archive:uint16 + bucket:uint32` within the metric
generation, and its entire value is one big-endian uint64. A point hash is
computed in memory from the timestamp and exact float64 bits using two
SplitMix64 avalanches; no point hashes are stored. Addition wraps modulo 2^64.
NaNs and infinities are excluded, as in the existing digest. Signed zeros and
finite subnormals retain their bit patterns.

There is one additional 8-byte metric revision fence at `ds`, rather than a
revision or count per bucket. Cache publication rechecks ID, generation and
revision under the writer stripe and commits only cache keys. Normal writes
invalidate buckets covering input/propagation slots, buffered rollups, changed
blocks and retired blocks, then advance the fence atomically with source state.
Monotonic block extension preserves its untouched earlier buckets. File
replacement, classic repair, archive-preserving writes and actual maintenance
merges clear the cache conservatively. Supplying an idle maintenance policy
does not discard history.
Other mutation paths change the metric revision without advancing the fence,
forcing lazy invalidation next time. Transfers exclude all local cache keys.

## Count and format limit

Exact groups have **physical** presence bits. Cold-packed groups encode
increasing timestamps and have no presence bitmap. Neither representation alone
provides a canonical finite-point bitmap after overlap/sidecar/rollup resolution.
Therefore the prototype caches only complete finite 8192-slot buckets: cache
existence certifies that every slot was finite when filled, so the count is
implicitly 8192. **No count is stored.** Sparse and partially covered buckets
continue to decode. Ordinary non-continuation archives also use decoding.

This implements the dense-history fast path, not the final decode-free path for
every sparse history. Supporting sparse cached sums needs a proven cheap count
source that handles duplicates, nonfinite values and cold packing without
violating the disk budget. Adding an 8192-bit bitmap per bucket would itself be
much more expensive than the sum, so that is not introduced here.

Cache filling is lazy on a digest request. Complete buckets must end before
`now - max(DigestSettle, archive step)`; `DigestSettle` defaults to one hour.
Correctness depends on write invalidation, not age alone. Retention and request
edges always use the canonical range read.

## Usage

Authenticated `POST /v1/transfer/digest` with read permission:

```json
{"Names":["example.metric"],"Mode":"sum-v1","From":0,"Until":0}
```

Both zero bounds request all retained history of every archive. Positive bounds
request the canonical `[From,Until)` window independently in each archive,
clipped to retention. The response uses the existing per-name/per-archive shape:
`Hash` is a 16-character sum, `Count` is finite-point count, and `From`/`Until`
identify the clipped archive window. Omitting `Mode` preserves the existing
SHA-based, serving-archive semantics. Clients must negotiate the mode and never
compare sum-v1 results against SHA results.

Comparison across live replicas also needs agreed explicit bounds and a settled
comparison horizon: independent wall clocks and in-flight input can change the
visible retained view. The cache is persistent storage state, not a live-input
flush or an atomic snapshot spanning multiple stores.

## Validation and measurement

The differential tests compare warm/cold sums to independently decoded canonical
archive windows and compare equal/unequal verdicts with the existing SHA recipe.
They exercise randomized histories, overwrites, duplicate timestamps, late
sidecars, direct writes, NaNs/infinities, signed zero, subnormals, retention
edges, deliberately different physical block sizes, cold packing, database
reopen and stale revision fences. Export checks confirm cache keys never cross
a transfer, and source key/value bytes remain unchanged by digest requests.

The reproducible fixtures contain 700 days of history across `1s:2d`, `60s:28d`
and `3600s:2y` archives, generated by the real native writer and cold packer.
One uses wide float mantissas, another integer gauges that compress to about
1.63 bytes per finite archive point. These are deterministic synthetic values,
not a captured production workload or a fleet-scale many-name churn replay.
An additional integer-gauge fixture fills `60s:30d`, `3600s:1y` and `1d:10y`
archives with ten years of history, including archives smaller than a bucket.

```sh
GOTOOLCHAIN=go1.27.0 go test ./internal/whisperio -run '^TestSumDigest' -v
GOTOOLCHAIN=go1.27.0 GOMAXPROCS=2 go test ./internal/whisperio \
  -run '^$' -bench '^BenchmarkFullHistoryDigest$' -benchtime=1s -count=3
```

`codec-value-B/name` counts encoded value bytes delivered by the storage adapter;
`table-read-B/name` counts filesystem table reads. Warm block/page caches can
make the latter zero. Neither is claimed as physical device IO. Disk overhead
is the actual difference in live SST bytes after full compaction, including
keys, the metric fence, prefix compression, filters and table overhead. WAL,
manifest churn and temporary compaction amplification are separate costs.

Measured with 229,797 finite archive points, 23 cached buckets and one revision
fence, cache payload is 192 bytes and uncompressed full keys plus values are
809 bytes. Compacted SST growth is 462 bytes (0.00201047 bytes/point; 0.032086%)
for floats, and 430 bytes (0.00187122 bytes/point; 0.114602%) for integer gauges.
Both pass the 0.15% fixture budget. This is not a universal bound for every
schema, compression distribution or small database.
The ten-year fixture contains 55,606 finite points and five cached buckets plus
one revision fence: live SST grows from 93,201 to 93,299 bytes, an extra 98 bytes
(0.00176240 bytes/point; 0.105149%). Its baseline is 1.6761 bytes/point.

Median of three one-second runs on a shared darwin/arm64 host with
`GOMAXPROCS=2`. Process CPU includes GC; elapsed time includes scheduling waits:

| Fixture | SHA CPU ns/name | Warm sum CPU ns/name | SHA elapsed ns/name | Warm sum elapsed ns/name | SHA encoded B/name | Warm sum encoded B/name |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Wide floats | 11,003,970 | 2,011,306 | 15,361,200 | 2,148,383 | 1,446,318 | 451,099 |
| Integer gauges | 7,862,585 | 2,113,399 | 6,996,244 | 8,601,016 | 379,428 | 124,258 |
| Integer, long retentions | 3,409,400 | 991,832 | 13,347,431 | 2,047,921 | 98,186 | 53,222 |

CPU falls by 71–82% and encoded value reads by 46–69%. Elapsed measurements
vary sharply under concurrent host load, including a slower integer warm run;
they do not establish a wall-time speedup. Earlier lighter-load repetitions
were faster, but an idle native-host benchmark remains required.

The SHA baseline repeats the existing fetch-and-hash recipe for every archive
to compare identical history. First fill reads 2,630,228 / 755,410 encoded bytes
for the float / integer fixtures because adjacent bucket reads can revisit
packed groups. Initial filling and write/invalidation cost need further work;
these warm single-name timings do not predict fleet throughput.

## Remaining work before enabling automatic sync use

- Support cheap canonical sparse counts under the same storage budget.
- Integrate client capability negotiation, all-archive comparisons, bucket
  mismatch localization and point comparison restricted to differing buckets.
  Ranged server digests exist; the client still uses its current verifier.
- Consider returning bucket summaries or a batch summary to reduce round trips
  and equal-batch response size. No combined batch hash is added here.
- Avoid repeated adjacent-bucket decoding on initial fill, optionally fill sums
  in existing seal/cold-pack work, and measure invalidation/write overhead.
- Capture a representative many-name history/churn workload and validate on an
  idle native host before promoting the prototype from Draft.


## Sync integration

Full-history verification negotiates `sum-v1` automatically on each holder. Each
request specifies the archive `Step` and an explicit settled `[From,Until)`
window, using the same bounds as the point comparison. Omitting Step continues
to return all archives. The response echoes Mode even for missing names; clients
reject a missing or different mode, since older servers can ignore request fields.

Equal archive identity, clipped bounds, count and sum skip point reads. A mismatch
retains the directional finite-point comparison: existing target values win, so a
value difference alone is not a hole. Holders without sum support are read through
carbonserver. When the target supports sums, a locally accumulated sum of an older
holder's points can avoid a matching target read. Digest capabilities are cached
separately from the existing SHA window endpoint.

The 2048-name differential fixture compares complete ordered hole verdicts with
the original full-history reader across new, mixed and old-target fleets. It
includes holes in multiple archives, absent names, target-only values, signed zero,
NaN, infinity and subnormal values. Wire tests reject old mode responses; mismatched
archive bounds fall back to point comparison. Dense buckets retain their implicit
8192 count; sparse buckets still decode, with no additional on-disk count format.
