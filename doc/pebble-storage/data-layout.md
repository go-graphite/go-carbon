# Data layout

All keys live in one Pebble keyspace, sorted bytewise. Integers in keys are
big-endian so they sort numerically.

## Format marker

The store directory holds a `CHUNKSTORE` file naming the format. `Open`
refuses unknown or mismatched formats, and older binaries refuse newer ones.

| Marker | Format | Meaning |
|---|---|---|
| `go-carbon-chunks-v1` / `-v2` | legacy | Original chunk encoding (`store-encoding = "legacy"`). |
| `go-carbon-chunks-v4` | exact | Exact point blocks (`store-encoding = "exact"`). |
| `go-carbon-chunks-v5` | incremental | Exact blocks with per-metric file ownership. |
| `go-carbon-chunks-v6` | continuation | Metrics as virtual CWhisper files (incremental conversion). |
| `go-carbon-chunks-v7` | time buckets | v6 plus hourly time-partitioned point groups and paged headers. |

v6 → v7 is an in-place upgrade at open (`store-time-buckets = true`). It is
one-way: v6 blocks remain readable and move to v7 as they are rewritten, but a
v7 store keeps writing v7 blocks and v6 binaries refuse its marker. Paged
headers (written by v7 binaries since paged-header support) are unreadable by
earlier v7 binaries too, so roll back only to a build at least as new as the
one that wrote the store.

The incremental marker `INCREMENTAL-FILE-MIGRATION` records the source
directory's path, device and inode, so a store cannot silently pair with a
different Whisper tree.

## Global keys

| Key | Value |
|---|---|
| `m/` + name | Catalog record (below). |
| `r` + id(8) + generation(8) | Revision record: revision (8) and last-update time in ns (8). |
| `z/sequence` | Metric ID allocator high-water mark. |
| `z/relayout` | Relayout cursor (next metric ID) and done flag. |
| `f/cleanup/` + id(8) | Conversion cleanup queue: originals still to reclaim. |
| `c` + id + generation + archive(2) + index(4) | Chunk of a non-continuation (legacy/exact) store. |
| `u` + id(8) + generation(8) + suffix | Continuation key space of one metric generation. |
| `t` + bucket(4) + `u` + id + generation + suffix | v7 time-partitioned point groups. |

### Catalog record

Legacy stores keep JSON. Exact stores use a compact value
(`internal/chunkstore/catalog.go`): one state byte (`1 + FileMigration`,
bit 7 set for continuation metrics), then ID, generation, aggregation method,
xFilesFactor and retention count, then step and count per retention. Revision
and activity live in the separate `r` record so routine updates do not rewrite
the catalog.

`FileMigration` is `FileUnowned` (staged, original still authoritative),
`FileCleanupPending` (store owns the metric, originals not yet reclaimed) or
`FileReclaimed`.

A metric's *generation* changes when it is replaced (re-staged, recreated
with a new policy). Old generations are removed by range deletes of their
prefixes, together with the codec's `Drop` for bucketed keys.

## Continuation key space (v6)

Under `u | id | generation` the codec stores a tiny file system:

```
u|id|gen| n/<name>                      → file id (8)          namespace: main file, OOO sidecar
u|id|gen| f|fid(8)| h                   → manifest             layout, page allocation, header (unless paged)
u|id|gen| f|fid   | H|page(2)           → 128-byte header page (v7 paged headers)
u|id|gen| f|fid   | b|archive(2)|block(4)          → block descriptor
u|id|gen| f|fid   | c|archive(2)|block(4)|group(4) → 128-point group (v6 location)
u|id|gen| f|fid   | p|archive(2)|0|group(4)        → classic (uncompressed) archive group
```

- **Virtual files.** go-whisper opens `fs.main()` and, for out-of-order data,
  a sidecar file. The namespace `n/` maps names to file IDs. Per-operation
  state (`continuationFiles`) holds only headers and identities; point data
  is read on demand.
- **Manifest.** Version byte (1, or 2 when the header is paged), compressed
  flag, then varints: file size, header size, archive count, per archive
  offset, step, count, block size and block count, then the allocated classic
  pages. Without paging the whole CWhisper header follows.
- **Compressed archives.** A CWhisper compressed archive is a ring of blocks.
  Each block's points are stored as 128-point *groups* (`PointBlock`). Its
  *descriptor* records how many points the block holds.
- **Point group.** `PointBlock{Present [2]uint64; Points [128]Point}`, encoded
  by `encodeExactChunk`: version and flags, a CRC32, varint timestamp deltas,
  then values as varint integer deltas when every value is an exact integer
  below 2^53, otherwise the eight byte planes of the float64 bits. Bodies of
  128 bytes or more are zstd-compressed when that is smaller. Nothing is
  rounded or dropped.

## Time buckets (v7)

v6 puts a metric's hot tail beside its whole history, so every bottom-level
table overlaps recent writes and compaction keeps rewriting cold data.
v7 moves point groups into a separate, time-partitioned key space:

```
v6: u|id|gen| f|fid|c|archive|block|group
v7: t|bucket(4)| u|id|gen| f|fid|c|archive|block|group
```

- **Bucket.** The UTC hour (`unix / 3600`) in which the block was written from
  its first point, by a rotation or an out-of-order rewrite. Appends keep the
  block's bucket. Conversion puts each archive's newest block, the one still
  receiving appends, in the current bucket and all older history in bucket 0,
  the *history bucket*.
- **Descriptor.** Stays metric-prefixed. 4 bytes (count) means v6; 8 bytes
  (count + bucket) means v7. The length selects the layout per block, so both
  coexist in one file:

  ```go
  type blockDescriptor struct { count int; bucket uint32; bucketed bool }
  func (d blockDescriptor) groupPrefix(id uint64, archive, block int) []byte
  ```

- **Addressing.** The codec writes bucketed suffixes as `T` + bucket + rest.
  `continuationIO` maps them to `t` + bucket + generation prefix + rest and
  back in `Visit`. A committed bucketed key is reached only through a prefix
  naming its bucket; only the descriptors know which buckets a file uses.
- **Why hours.** A 1-second archive's 7,200-point block fills in two hours.
  With day buckets the range receiving writes held a whole day of data; with
  hourly buckets it holds roughly the last two to three hours. Descriptors
  record each block's bucket, so the width can change without touching
  existing blocks.

### Paged headers

CWhisper rewrites the whole header (up to a few KiB: buffers, encoder state,
current block range) on every update, though only a few hundred bytes change.
v7 files store the header as 128-byte pages under `H` and write only the
pages that changed (`continuationFileData.writeHeader` compares page by
page). The manifest is rewritten only when layout or allocation changes.
Files move to paged headers on their first write in a v7 store.

### Invariants

1. Every bucketed group belongs to a block whose descriptor is v7, names that
   bucket, and has more points than the group's first slot.
2. A block rewritten from its first point first deletes its old groups,
   wherever the old descriptor placed them.
3. Removing a file removes its bucketed groups, located through its
   descriptors (`continuationFiles.deleteFile`).
4. Removing a generation calls the codec's `Drop` in the same batch, before
   the range delete of the metric prefix that removes the descriptors.
5. Ingested conversions write one table of metric-prefixed keys and one table
   per bucket, each covering only freshly reserved IDs, so none overlaps
   existing data.

## Pebble configuration that follows from the layout

- **Merger.** `exactChunkMerger` for exact stores.
- **Table sizes.** L0 2 MiB, doubling per level (Pebble defaults), unless
  `store-target-file-size` or `store-l0-target-file-size` overrides them.
- **Bloom filters.** Optional whole-key filters (`store-bloom-filter-bits`).
  They help point `Get`s (descriptors, groups, metadata). They do not help
  prefix scans (`Visit`), which still seek every overlapping L0 table and
  every level.
