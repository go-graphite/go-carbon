This package is an unfrozen format contract for copy experiments. The running
offline owner engine in `tool/immutable-parts/core.go` still uses format 1 and
host-local numeric IDs; it does not yet write this portable format.

The proposed key order is:

```text
GCP2 | virtual shard:u16 | signed time partition:u64 |
part UUID:16 bytes | canonical name length:u32 | exact canonical name bytes |
archive:u32 | signed block start:u64 | block ordinal:u32
```

All integers use big endian encoding; signed fields flip the sign bit to keep
time ordering. Routing is CRC32C of canonical name bytes modulo 4096. Ownership
maps sets of virtual shards onto physical Pebble owners. Routing hashes never
replace exact identity. Replicas use the same normalized canonical name bytes,
so copied keys need neither ID remapping nor a collision-resolution protocol.
Repeated name prefixes cost bytes and must be measured with SST compression.

A globally unique immutable part ID precedes metric ID so separate part files
have non-overlapping ranges. Each file must stay within one virtual shard and
time partition; `ValidateFileRange` checks that requirement. Merely adding a
prefix to mixed SSTs does not enable whole-file transfer. Stock Pebble compaction
can merge these boundaries, so a copy implementation must retain original
logical part files or provide a separately proved boundary-preserving storage
path. No vendor patch is proposed here.

A manifest binds the partition, policy digest, immutable part IDs, lengths and
streaming SHA256 hashes. Its deterministic inventory digest permits cheap
partition comparison without per-name reads. It is deliberately conservative:
equal logical archives sealed into different parts can have different inventory
digests. It does not prove logical data equality.

The format-1 atomic raw/frontier SST cannot simply be reused. Portable data SSTs
must have bounded disjoint ranges; owner-global frontier metadata needs its own
non-overlapping SST, ingested atomically with all data files. Preserve original
apply batch/clock/policy metadata before integrating authoritative rollups.
Reader publication, retained-file lifetime, snapshot/copy pins, garbage collection,
write fencing during ownership changes, and log retirement remain integration
requirements before this format can be frozen or enabled in a daemon.
