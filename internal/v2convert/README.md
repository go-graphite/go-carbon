# Offline v2 conversion boundary (experimental)

This package supplies a forward SST scanner and a checksummed per-input-part
transaction for **materialized current-core GCP3 rollup records**. The command
is its first consumer. It never opens, changes, or adopts a running database.

```sh
go run ./internal/v2convert/cmd -source /owned/frozen-ssts
# Repack only independently resolved, materialized GCP3 input:
go run ./internal/v2convert/cmd -source /owned/gcp3-input -output /owned/output
```

Native chunkstore SSTs are scan-only. `CoreRecords` rejects their keys rather
than inventing a conversion of continuation buffers, merge operands, physical
archive slots, policy, LastUpdate, or revision. A loose set of live SST links is
not a coherent logical snapshot: manifests, sequence visibility, range deletion
and obsolete generations must be resolved by the source owner before converting.

`Adapter` maps an authoritative source entry to an owned core record. `Writer`
is the boundary for a future dense writer and its matching reader. The current
`CoreWriter` emits unchanged `partsformat` GCP3 keys and singleton
`sealrollup.EncodeRecord` values, with Zstd/256KiB blocks. Each SST stays within
one placement group/vshard. This does **not** fix singleton density or remove
small ownership tails; native-density and disk-speed conversion are unqualified.

A conversion transaction is one source SST. Its encoded SHA-256 plus adapter
and writer version select the output directory. The writer receives a bounded
batch, outputs closed/synced files, and those files are re-read through its own
reader. A single ordered part digest compares every transformed key/value byte
and record count. Files also receive encoded SHA-256 checksums. After directory
sync, a receipt and all outputs publish together by directory rename and parent
sync. Resume validates source identity and every output file checksum; there is
no per-name persistent state or per-name verification digest. Source files must
remain immutable for the entire operation.

The decoded size limit charges key/value bytes and record headers. Oversized
input tables fail; they are not split or silently converted with unbounded
memory. The current implementation reads the source for its checksum and again
for decoding, and re-reads output for exact verification. Those passes and
ownership tails must be included in any future throughput/density measurement.
Separate source SSTs are not yet coalesced. Interrupted in-process errors clean
staging; a killed process may leave `.building-*` directories that its owner may
remove after confirming no converter is using them.

This package provides neither activation in core, source reclamation, replay
frontiers, nor a migration claim. The next required piece is a certified native
source adapter and a shared dense value format understood by core readers.
