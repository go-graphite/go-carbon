# Saved read indexes

With `trie-index`, `concurrent-index`, and file-list-cache version 2 enabled,
carbonserver uses an additional saved index. If the accelerator is missing but a
complete, ordered version-2 file list exists, startup builds the compact index
directly from that catalogue without reconstructing the large heap trie. This
one-time bootstrap takes longer than opening a prepared generation. It avoids
waiting for a filesystem scan interrupted by legacy full-tree quota accounting.

Without a usable saved catalogue, the first filesystem scan still publishes the
normal trie as soon as it is complete. Subsequent complete scans write and install
the accelerator while reads remain available. No additional configuration is
required.

The accelerator consists of an immutable finite-state transducer (FST), packed
file metadata, and an atomically published manifest next to the existing file
list cache. The manifest binds both files to their checksums, format version,
data-root identity, and source cache generation. A missing, damaged, incompatible,
or stale accelerator is rebuilt from a supported saved catalogue. An unordered
or older cache, failed bootstrap, or unavailable accelerator storage uses the
ordinary trie loader. The cache remains compatible with older binaries.

On a restart with a valid accelerator, carbonserver maps and validates the saved
files instead of reconstructing a heap node graph for every metric. Initial
quota assignments and complete usage totals are calculated before publication.
Metric discovery uses the existing Graphite glob compiler. Exact existence,
metadata, namespace listing, and quota checks include both the saved index and a
small mutable trie of newly observed metrics.

The initial mutable trie uses the bulk loader and its construction counters,
avoiding an immediate prune/count walk of a fresh tree. Quota accounting sums
subtrees in one pass and retains totals only for the root and configured quota
namespaces; every configured quota is still enforced before reads become ready.
The loader reuses path decode buffers while retaining owned labels in the tree.
Private construction avoids atomic child updates; published trees keep their
existing concurrency protections.
Overlay decoding runs alongside base-file validation. The decoded overlay stays
private until both jobs finish and their generation identities match.

A later complete filesystem scan writes a replacement generation. Queued metric
notifications continue updating the live overlay during that scan. Pending
metrics still in cache survive reconciliation; metrics now included in the new
snapshot leave the overlay. An incomplete scan cannot replace the saved index.
Queries already using a retired generation keep its mappings alive until they
finish. Old result metadata owns its storage and remains valid after the mapping
is released.

Quota totals and `metricsKnown` cover the complete logical index. `trieFiles`
includes saved and mutable metrics; `trieNodes` and `trieDirs` describe the mutable
trie representation, which is much smaller after snapshot startup. The quota
administration view identifies the snapshot and lists assigned namespaces.

Building the next generation temporarily requires space for both the current and
replacement accelerator. Accelerator write failures leave reads available and
are logged; the legacy cache still provides the recovery fallback. The first
complete background generation also replaces the live heap trie, so future
restarts do not need to release the original large node graph at exit.

## Reads during compressed-history recovery

For compressed Whisper with untagged input, graceful shutdown saves a small
index overlay plus a pointer-free lookup table over the ordinary cache and input
`.bin` files. The point manifest binds these files to the exact saved read-index
generation, data-root identity, sizes and checksums. Shutdown stops index workers
while persistence remains active, then stops persistence, diverts input, dumps
the stable cache, and waits for input cleanup.
Catalogue membership classification runs only after both source files have been
closed and synchronized; optional checkpoint work cannot delay their durable save.
Read listeners remain available until the durable checkpoint is complete.
The point index classifies names against that frozen saved catalogue, including
its overlay, so startup inserts only names that arrived after the index froze.

On the next start, go-carbon validates/maps both checkpoints in parallel, installs
all saved metric names in the private index, applies initial quotas once, and
opens read listeners. Initial publication waits for checkpoint validation and
name insertion; no partially accounted index becomes visible. A cache read
can fetch unclaimed points directly from their saved records. Recovery transfers
a whole metric under its cache-shard lock, keeping it visible in either the saved
source, cache, or in-flight write list. The lookup compares full names after
hashing; cache records precede input records, preserving later-value precedence.
The three point-checkpoint files are checksum-validated concurrently. Startup
uses fixed 16 MiB SHA-256 chunks when available, with at most four checksum
workers per file, and checks every byte before use. Writers calculate these
digests alongside the whole-file digest while writing the authoritative bytes.
The whole-file digest remains available to older readers; checkpoints without
chunk digests retain whole-file validation. Invalid chunk sizes or counts reject
the checkpoint rather than bypassing validation. Startup
also validates the two saved-catalogue files concurrently. Large recovery record
tables use bounded parallel validation, including every range boundary. Startup
logs distinguish checkpoint validation, remaining index wait, and insertion of
previously unindexed names. The pending-name timer is nested inside warmup and
may overlap checkpoint/index loading; do not add it again to the readiness duration.

Input receivers retain their existing recovery gate until all old history has
persisted. This prevents newer live points from advancing compressed block
watermarks during replay. Filesystem reconciliation also waits for that drain.
Only then are source files removed and the dump directory synchronized before
intake opens. A process crash during recovery can replay the retained legacy
files; no newer live values have been persisted over that history.

A missing, corrupt, incompatible or mismatched checkpoint uses ordinary ordered
restore before reads open. Extra dump generations also force that fallback.
Tagged input, noncanonical metric names (for example `a..b` or `a/b`), and other
storage modes keep their existing startup path. There is no additional
configuration switch. The accelerator becomes usable after a complete saved
index and a subsequent graceful stop; it does not make an uncached first boot
instantaneous or eliminate the process handoff gap.

For an opt-in offline test, `TestCapturedIndexSnapshot` reads the cache selected
by `GO_CARBON_SNAPSHOT_FLC` and writes only into `GO_CARBON_SNAPSHOT_DIR`. It checks
sampled metric queries and all configured quota totals against an independently
calculated reference. `GO_CARBON_SNAPSHOT_QUOTAS` optionally supplies a JSON array
of quota rules. `TestCapturedIndexSnapshotOpen` measures initial publication of
that completed generation separately. These probes never scan the live Whisper
data directory.
