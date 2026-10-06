# Saved read indexes

With `trie-index`, `concurrent-index`, and file-list-cache version 2 enabled,
carbonserver builds an additional saved index during complete filesystem scans.
The first scan without a usable cache still publishes the normal trie as soon as
it is complete. Subsequent scans write the accelerator while that index serves
requests. No additional configuration is required.

The accelerator consists of an immutable finite-state transducer (FST), packed
file metadata, and an atomically published manifest next to the existing file
list cache. The manifest binds both files to their checksums, format version,
data-root identity, and source cache generation. A missing, damaged, incompatible,
or stale accelerator falls back to loading the ordinary file list cache. That
cache remains compatible with older binaries.

On a restart with a valid accelerator, carbonserver maps and validates the saved
files instead of reconstructing a heap node graph for every metric. Initial
quota assignments and complete usage totals are calculated before publication.
Metric discovery uses the existing Graphite glob compiler. Exact existence,
metadata, namespace listing, and quota checks include both the saved index and a
small mutable trie of newly observed metrics.

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
are logged; the legacy cache still provides the recovery fallback. The saved
index only removes index reconstruction from startup. Cache/WAL recovery and
other application initialization have their own correctness and readiness gates.

For an opt-in offline test, `TestCapturedIndexSnapshot` reads the cache selected
by `GO_CARBON_SNAPSHOT_FLC` and writes only into `GO_CARBON_SNAPSHOT_DIR`. It checks
sampled metric queries and all configured quota totals against an independently
calculated reference. `GO_CARBON_SNAPSHOT_QUOTAS` optionally supplies a JSON array
of quota rules. `TestCapturedIndexSnapshotOpen` measures initial publication of
that completed generation separately. These probes never scan the live Whisper
data directory.
