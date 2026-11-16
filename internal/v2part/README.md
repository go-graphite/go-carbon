# Portable v2 parts

`v2part` is the common SST/identity/inventory boundary for core archive seals,
offline conversion and whole-file transfer. It preserves the published
`partsformat` GCP3 key bytes through type aliases and delegation, rather than
introducing an incompatible second key codec. The core rollup planner uses this
boundary now; `v2core.WritePortableRollupPart` adapts its existing seal records.

| Layer | Version / contract |
| --- | --- |
| Key | GCP3: exact placement group, CRC32C(name) mod 4096 vshard, signed partition, 16-byte part ID, exact canonical name, archive, signed physical block/slot, ordinal |
| SST | Core-compatible RocksDB v2 container, Snappy, bytewise comparer, sequence-zero SETs only; no range keys/deletes, merges or point tombstones |
| Descriptor | Schema 1 (existing) / 2 (dense); physical size/SHA-256, complete scope, value encoding, original source incarnation/stream/order bounds, key bounds, event-time bounds, exact distinct-name/record counts, archive policy definitions and their SHA-256 |
| Manifest | Schema 1; one PG, generation, canonical catalog/policy/tombstone roots, ordered stream frontiers and sorted descriptors |
| Receipt | Schema 1; manifest digest + descriptor digest + target storage incarnation + durably installed target generation |

Schema-1 files contain exactly one `(group,vshard,partition,part)` scope.
The explicit schema-2 extension below allows same-group vshard packing. An old core
seal can reuse its UUID in several scopes: the UUID alone is not the file ID.
No physical owner, DB count, core count, target host or placement-map epoch
participates in persistent key identity. A manifest may span many vshards/time
partitions in its PG, but never independently movable PGs. Current 4096 routing
is the published experimental contract, not production sizing approval.

`Scope.Group` is the exact externally assigned persistent label. The placement
layer must freeze its namespace/bootstrap-cell/vshard membership; this package
does not derive PGs from the current machine count or a mutable routing rule.

## Writing and reading

Supply `Spec{Scope, Encoding, Source, MinTime, MaxTime, Policies}` to
`NewWriter(path, spec)`, then strictly sorted `Set(Key,value)` or
`SetEncoded(key,value)` calls, and `Close() (Descriptor,error)`. Creation is
exclusive; existing files are never truncated. `Close` hashes during output,
flushes and fsyncs the SST. Errors/`Abort` discard staging. Counts are computed
from the stream using only the previous name, with no per-name inventory.
Policies are exact versioned definition bytes with SHA-256 digests. Archive
`max-u32` is reserved for the core metadata envelope and needs no archive policy.

Values are opaque to the generic writer. Time coverage, source ordering and
policy associations are producer assertions about authoritative records;
checksums bind these claims but cannot establish their semantic correctness.
Do not use `Key.BlockStart` as event time: core uses physical ring slots and
partition zero. Its adapter derives actual event times, carried order bounds,
and archive definitions from canonical metadata and `sealrollup` records. It
preserves every key/value byte, including occupied NaNs, signed zeros, original
orders, policy metadata and its local numeric ID field.

`Read(ctx,path,&descriptor,visit)` verifies the full physical checksum before
visiting records, then validates every key, policy-archive presence, counts and
key bounds. Callbacks borrow values; retain copies if necessary, and do not
publish callback side effects until the complete call succeeds. `Read` with a
nil descriptor is the one-way reader for existing published core GCP3 SSTs:
same record/scope checks, without a preexisting descriptor/hash. It accepts the
old core's SST container version as well as compatible Pebble v4 files. New
files ingest into core's current DB format without a format-major ratchet.

`VerifyStream(ctx,dst,src,descriptor)` hashes inline while copying to staging;
it verifies exact size and checksum without decoding names. Routine resume
compares manifest/descriptor digests and receipts, never per-name digests.
Semantic decoding is an initial qualification step, not a mandatory second
full-file pass after every verified transfer.

## Canonical inventory and receipts

Descriptor/manifest `MarshalBinary` emits fixed-field canonical JSON, using Go
`encoding/json` integer arrays for fixed byte arrays and base64 for byte slices.
Policies sort by `(archive,digest)`, parts by their complete binary GCP3 scope,
and frontiers by `(incarnation,stream)`. No maps/floats enter canonical encoding.
Unknown schema versions, duplicate identities and noncanonical serialized
forms (including unknown/duplicate fields) are rejected. Inventory digests are
SHA-256 with domain prefixes `go-carbon-v2part-descriptor-v1\0` and
`go-carbon-v2part-manifest-v1\0`, followed by canonical JSON. Golden key, SST and
descriptor vectors are committed under `testdata/`.

Catalog/policy/tombstone roots must be known nonzero digests; an empty inventory
needs an explicit digest. Frontiers must cover the actual journal/owner streams,
not an event-time maximum. `Receipt.Covers` checks membership in the exact
manifest, descriptor coverage, target incarnation and target generation.
Receipts may be issued only after durable install/publication by their owner.
The type checks identity; it cannot enforce that an external installer fsynced.
It is not an authenticated transport protocol or a declaration of READY.

## Remaining integration boundaries

The package does not retain authoritative seal originals through Pebble
compaction, create/pin snapshots, install into a running engine, coordinate
cross-DB publication, fence placement epochs, implement deletion/reset
incarnations, authorize sources, or certify all historical donor coverage.
Those owners must bind their catalogs, policies, tombstones and frontiers into
the manifest before claiming whole-engine movement. Numeric raw records and
local replay IDs in the current offline engine remain outside portable archive
identity. The adapter is not a complete runtime importer for those namespaces.

The schema-1 writer preserves existing singleton values. The opt-in dense
extension below improves packing; real-source density and disk-speed conversion
remain separate qualification gates.

## Dense archive values and larger physical files

`NewDenseWriter(path, spec)` accepts the published sorted singleton stream and
produces `core-rollup-dense-v1` values. A value holds at most 4,096 occupied
physical slots of one metric/archive. Slot deltas encode sparse archives without
padding; timestamp delta-of-deltas retain physical ring wrap; XOR varints retain
original replacement orders; a bitmap retains correction flags. Values use
bit-checked integer deltas or eight exact byte planes. Zstd/256KiB SST blocks
compress these columns. No floating-point arithmetic changes noninteger bits.
`DecodeSlots` accepts both dense and singleton values. `ReadExpanded` reconstructs
the exact original singleton keys/values and rejects overlapping runs. Coverage
record counts refer to physical SST records, not expanded slot counts.

Dense descriptors use **schema 2**, including files with a single vshard.
Existing schema-1 descriptor and golden SST bytes remain unchanged; older
readers reject schema 2. Dense files can set `Scope.VShard = GroupVShard` (65535)
to span the vshards of **one placement group**, retaining one partition and
original part ID. Every GCP3 key still carries its actual routed vshard; 65535
never appears in a key. This is a physical packing boundary, not a routing
change. `Read` requires the descriptor for a file spanning vshards; its nil
compatibility mode stays strict. Manifests reject overlapping group-wide and
single-vshard descriptors for the same original part/partition.

`v2core.WriteDenseRollupPart` derives coverage and archive policies from complete
core metadata, and `ScanRollups` resolves dense and singleton replacements by
original order. Offline conversion is the first dense writer consumer. Repacking
preserves original IDs: combining unrelated singleton parts requires a certified
materialized source/compaction contract and is not inferred from filenames.
The synthetic byte gate covers encoded SST bytes against native exact point
blocks; it does not certify captured-data or whole-engine at-rest density.
