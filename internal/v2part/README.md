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
| Descriptor | Schema 1; physical size/SHA-256, complete scope, value encoding, original source incarnation/stream/order bounds, key bounds, event-time bounds, exact distinct-name/record counts, archive policy definitions and their SHA-256 |
| Manifest | Schema 1; one PG, generation, canonical catalog/policy/tombstone roots, ordered stream frontiers and sorted descriptors |
| Receipt | Schema 1; manifest digest + descriptor digest + target storage incarnation + durably installed target generation |

One file contains exactly one `(group,vshard,partition,part)` scope. An old core
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

This change preserves current value density and the one-vshard file boundary;
it does not introduce a dense multi-slot codec or qualify conversion's disk
amplification/throughput gate. Core readers continue to read the same GCP3 keys.
