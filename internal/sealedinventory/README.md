# Immutable SST inventory

`storectl inventory sealed -sst-dir DIR -out inventory.json -files 20 -read-mb-s 200 -procs 4`

Reads immutable SST files directly without opening a Pebble database, acquiring
its lock, reading MANIFEST or writing to either input directory. Pin files from
an independently obtained snapshot before scanning. SST symlinks are rejected.
Reports use exclusive creation outside the input directories.

`-metadata-dir` supplies all immutable SSTs for descriptor/manifest joins;
it defaults to `-sst-dir`. `-files 0` scans all files in sorted order. Joins use
a bounded reader and per-metric LRU. `-as-of` and `-write-window` fix the ordinary
eligibility model's clock. Unknown schemas and metadata remain explicit.

Compressed data bytes are apportioned by logical key/value bytes. Every internal
point version is counted; obsolete versions and cross-file deletions mean this
is a physical inventory, not a count of live points. Same-file range tombstones
are reported. No policy/transcode CLI is shipped and no SST output is produced.
