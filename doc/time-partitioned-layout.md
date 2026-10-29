# Time-partitioned continuation layout (store format v7)

> Design overview, data layout, write and read paths, tuning and production
> lessons: [Pebble store design notes](pebble-storage/README.md).

Experimental. Disabled by default (`store-time-buckets = false`). Not yet
qualified on a production node.

## Problem

In v6 every key of a metric generation shares the prefix `u` + ID +
generation. A metric's live tail blocks, descriptors and headers therefore
sit beside its entire cold history. New writes reach every metric, so every
bottom-level table overlaps recent writes and Pebble keeps rewriting cold
history to merge a few hot keys into it. On a node whose writes mostly go to
the shared store, compaction wrote 340–560 MB/s and stalled writes 20–40% of
the time. About 62% of that work landed in L6 (write amplification ~9.5x).

## Layout

A compressed block's 128-point groups move to a separate key space:

    v6: u | id | generation | f | file | c | archive | block | group
    v7: t | bucket | u | id | generation | f | file | c | archive | block | group

- **Bucket:** the UTC hour (Unix seconds / 3600) in which the block was
  written from its first point, by a block rotation or an out-of-order
  rewrite. A conversion places each archive's newest block, the one that
  keeps receiving appends, in that day's bucket. It places the rest of the
  imported history in bucket 0, the history bucket. Appends keep the block's
  bucket.
- **Descriptor:** the block descriptor (`... | b | archive | block`) stays
  metric-prefixed. In v6 it holds the point count (4 bytes). In v7 it holds
  the count and the bucket (8 bytes). The descriptor length selects the layout
  per block, so v6 and v7 blocks coexist within one file.
- **Unchanged:** headers, manifests, names, classic (`p`) archives and
  out-of-order sidecar slots.

Recent buckets receive the writes; older buckets are written only when a
ring rotation deletes their blocks. Compactions of recent writes no longer
reach cold history.

The codec addresses bucketed keys as suffix `T` + bucket + rest.
`ContinuationIO` maps such suffixes to the `t` space and maps them back in
`Visit`. A committed bucketed key is reached only through a prefix naming its
bucket; only the codec, through the descriptors, knows which buckets a
generation uses.

## Invariants

1. Every bucketed group key belongs to a block whose descriptor is v7, names
   that bucket and has more points than the group's first slot.
2. A block rewritten from its first point first deletes its old groups,
   wherever the old descriptor placed them.
3. Removing a file (truncate, unlink at flush) removes its bucketed groups,
   located through the file's descriptors.
4. Removing a generation (metric delete, replacement, restage) calls the
   codec's `Drop` in the same batch, before the metric-prefixed range delete
   that removes the descriptors.
5. Ingested conversions write one table of metric-prefixed keys and one
   table per bucket, normally the history bucket and today's. Each covers
   only the group's freshly reserved IDs within one key range, so none
   overlaps existing data.

The oracle tests check invariants 1–3 against an on-disk CWhisper file while
the clock crosses many days, starting in v6 and switching to v7 mid-way. Store
tests cover key scoping, the marker upgrade and deletion.
`GO_CARBON_TEST_TIME_BUCKETS=1` runs the existing whisperio and persister
suites in v7 mode.

## Upgrade and rollback

- **Enabling:** with `store-time-buckets = true` a new store is created as
  v7, and an existing v6 store's marker is replaced at open. Existing blocks
  stay v6 and move as they are rewritten.
- **One-way:** a v7 store keeps writing v7 blocks even if the option is later
  cleared, and older binaries refuse the marker. Rolling a node back to a v6
  binary therefore needs a replica copy or a reverse re-layout, which does
  not exist yet.
- **Paged headers:** the first v7 build (`a28224db`) accepts the same marker
  but cannot read paged headers; nothing fences it out
  ([review](time-partitioned-layout-review.md#status-on-the-integration-line)).

## Costs

- **Full-history reads:** these touch one bucket per block instead of one
  contiguous range. Recent-range reads touch the newest blocks only.
- **Deletes:** deleting a v7 generation first visits the generation's
  metric-prefixed keys to find its buckets.
- **Stranded history:** a conflicting staged conversion leaves unreferenced
  history in the `t` space as well as the `u` space. This is the same
  behaviour as v6.

## Not done

- **Re-layout of converted v6 data** is implemented (`store-time-buckets-relayout-rate`,
  bytes per second; 0 disables).
  1. It scans the catalog once, then walks v6 metrics existing at start in
     ID order.
  2. The codec plans each metric: every v6 block except each archive's newest
     is copied byte-for-byte to the history bucket.
  3. A group of metrics (64 MiB or 4096 metrics) is written as one table.
     The table covers only that ID range in the history bucket, so it ingests
     without overlapping anything.
  4. Each block then switches under its metric lock only if its descriptor
     still matches the plan: the descriptor flips and the old groups are
     point-deleted. A block rewritten meanwhile keeps its new state and its
     copies are deleted.
  5. A cursor (`z/relayout`) resumes after restarts. Progress is reported as
     `Relayout` in the store statistics.
  6. A crash between ingestion and the switches leaves copies that the
     resumed run writes again. Newest blocks stay v6 until they rotate.
- **Production qualification:** compaction bytes, stall fraction, disk and
  read latency on one canary node against a v6 peer.

## Measurements so far

`GO_CARBON_BENCH_TIME_BUCKETS=1` runs a two-layout comparison: convert
history, compact fully, then run live updates over simulated days.
- **Per-update writes:** equal in both layouts (same keys and bytes). About
  70% of each update's bytes is the CWhisper header, which v7 leaves
  metric-prefixed.
- **Compaction totals:** at 200 metrics they differed by less than the
  run-to-run noise (about ±15%). No run compacted into L6, the level that
  dominates production cost, so the benchmark does not yet reach the
  workload v7 targets. Production evidence must come from a canary.

## What drives dense compaction (25 Oct 2026 analysis)

- **Production (a large aggregation host, 20 min):** flush 33 MB/s against 364 MB/s of
  compaction (about 11x). 64% lands in L5 and L6 (L6 724 GB, L5 138 GB),
  i.e. hot keys spread over the whole store keep rewriting the bottom levels.
- **Workload:** most points are `secondly` metrics whose fine archive keeps
  only 1-2 days. Updates carry 190-340 points (queue lag batches them), about
  1.9M points/s.
- **Header churn:** the CWhisper header is rewritten whole on every update,
  yet only a small part changes (300-point updates, measured):

  | Retention | Header | Changed | Dirty 256 B pages |
  | --- | ---: | ---: | ---: |
  | `1s:1d,1m:30d,1h:1y,1d:10y` | 1,915 B | 302 B | 3.7 (953 B) |
  | `1s:2d` | 609 B | 31 B | 2.0 (512 B) |

- **Key-level Pebble model** (a standalone simulation, not part of the repository):
  2,000 metrics; 300-point updates; 2 KB header; 20-group blocks in a 4-block
  ring (the 1-2 day fine archive); 100 KB of cold coarse history per metric.
  Compaction relative to v6:

  | Variant | Compaction |
  | --- | ---: |
  | v6 without header churn | -27% (flush -34%) |
  | daily buckets (v7), cold history not relaid | -18% |
  | daily buckets, cold history relaid | -17% |
  | hourly block buckets | -23% |
  | group-creation buckets | -25% |
  | group-creation buckets + 256 B header pages | -51% (flush -30%) |

  The model is only about two levels deep (WA about 2, production 11), so
  layout effects are likely larger in production. **Relaying out cold history
  did not matter**; on dense the v6 fine-archive blocks also age out within
  1-2 days. So the relayout tool is not needed to unblock dense.

### Successor priorities

1. **Paged headers:** store the header as small pages and write only the
   dirty ones. Cuts header bytes 2-5x per update, in every layout.
2. **Group-creation time buckets at hourly granularity:** new groups land in
   a strictly recent key range whatever the block lifetime (coarse archives
   included). The descriptor records each group's bucket as run lengths.
3. **Open-group rewrite:** the partly filled 128-point group is rewritten on
   every update. Measure before changing it.

### Header and bucket follow-up (25 Oct 2026)

- **Header write share:** for the multi-archive `secondly` retention, the main
  header is 4,511 B and about 713 B change per 300-point update. Most of that
  is CWhisper's propagation buffers, which shift on every update, so 128 B
  pages write 2,173 B per update (about half). The `1s:2d` header of dense's
  `aggregations.secondly` metrics is 591 B, with 31 B changed.
- **Paged headers:** v7 files now store the header as 128 B pages (manifest
  version 2) and write only the changed pages; the manifest is rewritten only
  when layout or allocation changes. Files move to paged headers on their
  first write in a v7 store. Binaries older than this change cannot read
  them.
- **Hourly buckets:** on dense most bytes are data groups, so the size of the
  key range receiving writes matters most. A 1-second block fills in two
  hours; day buckets kept a whole day of data hot, while hourly buckets keep
  about the last two to three hours hot.
