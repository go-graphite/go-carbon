# Read path

carbonserver answers graphite-web and carbonapi `find`, `render` and `info`
requests. With the Pebble store, names come from the index and points come
from `Store.Fetch`, merged with unpersisted points still in the cache.

## Find

Unchanged from the file backend: the trie index (required for incremental
conversion) and the file-list cache serve glob expansion. In incremental mode
the index covers both converted and unconverted metrics; the scan walks the
source tree and the catalog.

## Render

`carbonserver.fetchFromDisk` (`carbonserver/fetchfromdisk.go`):

```
OriginalReadGuard(metric)                       shared stripe of fileReadMu
   │
   ▼
Store.Fetch(name, from, until)
   ├─ catalogMembership.mayContain(name)?      in-memory filter; "no" = not in store
   ├─ NewSnapshot                               consistent view for the whole read
   ├─ metadataFrom(snapshot)                    m/<name>, r|id|gen
   └─ ContinuationCodec.Fetch(read-only continuationIO over the snapshot)
        ├─ whisper.OpenWithOptions(main file)   manifest + header pages
        └─ go-whisper Fetch(from, until)
             └─ ReadStorageBlock per block in range
                  Get descriptor  ─▶ Visit the block's groups
                                     (u-space for v6 blocks, t|bucket for v7)
   │
   ├─ found ─▶ merge cache points ─▶ reply
   └─ ErrNotFound (incremental mode) ─▶ read the original .wsp with CWhisper
                                         (retry the store if the file vanished:
                                          it may have just been converted)
```

- **Coherence.** `OriginalReadGuard` spans the store read and the cache read.
  Publication of a conversion drains these readers before reclaiming
  originals, and a persister confirms popped points under the exclusive
  `SharedWriteGuard`. A reader never misses a batch that is between the cache
  and the store.
- **Snapshots** are released by a background worker
  (`startSnapshotRelease`), so request goroutines do not pay for Pebble's
  snapshot list maintenance.

## What a fetch reads

| Read | Kind | Notes |
|---|---|---|
| Catalog membership | memory | Proves absence only after a complete catalog scan; false positives just cost a lookup. |
| Metadata | point `Get` ×2 | Catalog and revision record. |
| Manifest | point `Get` | Plus a prefix scan of header pages on v7. |
| Block descriptors | point `Get` per block | Bloom filters help. |
| Point groups | prefix scan per block | One bucket per v7 block; recent ranges touch only the newest blocks. |

## What makes reads expensive

1. **Table-cache churn.** Pebble keeps at most `store-max-open-files` table
   readers open. A store with tens of thousands of tables and the default
   budget reopens tables constantly: tens of thousands of opens per second
   on a busy host, with the CPU and lock contention that brings. Size the
   budget above the table count (see [tuning.md](tuning.md)).
2. **L0 sublevels.** Every iterator seek visits each overlapping L0 table and
   one table per lower level. When compaction falls behind, L0 grows from one
   or two sublevels to fifteen or more, and every read slows with it.
3. **Cold block cache.** After a restart the block cache is empty, and reads
   compete for the disk with compaction until it warms.
4. **Memtable range tombstones.** Every new range tombstone resets the
   memtable's fragment cache and serializes readers behind re-fragmentation.
   The write path avoids writing them ([write-path.md](write-path.md#deletes-inside-an-update)).
5. **Persister reads.** Most read traffic on a busy host comes from the
   persister itself (headers, descriptors, predecessor groups on every
   update). Render latency suffers through contention for the same caches and
   disk, not through its own CPU.

## Tail latency

Large renders (thousands of metrics per request) dominate the p99 on either
backend. With exponential latency buckets, small shifts in the share of slow
requests move the interpolated p99 a lot. Track the share of requests above
a fixed threshold (for example 256 ms and 512 ms) as well as the p99.

## Response cache coherence

Expiration batches retain the existing HTTP and streaming render-cache entries.
An expired metric has had no writes for its expiration window, so cached replies
may continue to include it until the normal query-cache TTL expires (60 seconds)
or the cache is cleared. Deleting an unread backlog does not invalidate unrelated
cached renders or add work to cache hits. Ordinary ingestion also retains this TTL.

Expiration still publishes catalog changes. Fully shared catalogs bypass find/glob
response caches, so discovery drops deleted names when the index refresher processes
the catalog journal. Existing index/discovery cache policies still apply during
incremental migration; info reads current store metadata.

Administrative mutations retain their existing global cache invalidation policy,
including administrative deletes, replacements and archive writes. New store
handles also receive distinct cache namespaces.
