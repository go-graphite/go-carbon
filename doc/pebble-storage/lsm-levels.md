# How data moves between LSM levels

The Pebble store is a log-structured merge tree (LSM). This page explains how
data moves through its levels and what the "SSTs" row of the go-carbon
dashboard shows.

1. **Writes go into a memtable**, an in-memory sorted buffer, plus the WAL on
   disk. When the memtable fills, it is flushed as a new SST file into L0.
   That is the "L0 writes (flush)" panel.

2. **L0 is unsorted across files.** Each flush covers whatever keys it happened
   to contain, so L0 files overlap each other. "L0 sublevels" counts how many
   overlapping layers there are. A lookup must check every overlapping L0
   file, which is why many sublevels make reads slow, and why Pebble stops
   writes at its limit (the 12 and 32 threshold lines).

3. **L1–L6 are each one sorted run.** Within a level, files cover disjoint key
   ranges, so a lookup checks at most one file per level. Each level has a
   target size, about 10× the one above. L6 holds roughly 90% of the data, and
   the targets are computed down from L6's actual size ("Ln target").

4. **Compaction moves data down.** When a level is over target ("size /
   target" above 1, "score" at or above 1), Pebble picks a file or range from
   it plus the overlapping files in the next level, merge-sorts them, writes
   new files into the next level and deletes the inputs:
   - reads are the input level's file plus the overlapping part of the level
     below ("Ln reads");
   - writes are the merged output ("Ln writes");
   - dropped is what the merge discards: older versions of the same key,
     deleted keys and expired range deletes ("compaction input dropped");
   - when nothing in the next level overlaps, a file just moves down by a
     metadata change with no I/O (the "move" kind in "compactions by kind").

5. **Why it costs so much I/O.** Each byte is rewritten roughly once per level
   it passes through. Reads exceed writes per level because a compaction must
   also rewrite the overlapping data already in the lower level. Write
   amplification (compaction writes ÷ incoming bytes) is the total. Compaction
   debt is what is still owed before every level is back under target.

6. **Two ways it goes wrong in production.**
   - *Drive-bound compaction.* Compaction write bandwidth is capped by the
     drive. On NVMe drives that are not trimmed, the drive's own garbage
     collection keeps copying space the filesystem already freed, write
     latency doubles and compaction throughput falls. Debt then piles up in
     L0 and L1, and reads check more overlapping files. A trim restores the
     throughput; see [challenges.md](challenges.md).
   - *Very long keys.* Each lookup opens the L0 files that overlap its key and
     loads their index before the bloom filter can rule them out. Metric names
     several KB long make every catalog key larger than an index block, so a
     table of them gets a top-level index of tens of MB. Once such tables sit
     in L0, nearly every lookup loads those indexes, and concurrent reads can
     pin hundreds of GB of memory. Names a Whisper file could not hold are
     therefore refused (`ErrNameTooLong`, see
     [tuning.md](tuning.md) for `store-purge-invalid-names`).
