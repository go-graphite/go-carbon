# Placement contract v1

This package is the shared go-carbon contract for an explicit placement map.
It does not change relay routing or claim that the current experimental core's
numeric files are portable between hosts. The daemon consumes its admission
interface; `storectl placement` consumes the same selector for read-only planning.

## Frozen identity and golden vectors

Input is the exact post-rewrite routing name. No tag sorting, alias rewriting,
case folding or Unicode normalization occurs here. `name_version` is
`post-rewrite-bytes-v1`. Longest byte-prefix match chooses a frozen namespace,
including a mandatory empty fallback. Aliases must already have their agreed
routing bytes; these vectors do not qualify a relay's rewrite configuration.

Legacy selection is 64-bit FNV-1a (offset 14695981039346656037, prime
1099511628211, unsigned overflow), then jump consistent hash with multiplier
2862933555777941757 and IEEE-754 binary64 arithmetic. For sorted frozen bootstrap
ring lengths, the cell contains each legacy slot. A larger ring's slot either
matches the preceding slot or is in the larger ring's new suffix. Maps enumerate
all such cells, including empty cells, crossed with every namespace and range.

Routing vshard is CRC32C/Castagnoli of the same bytes, low 12 bits. Span divides
4096; ranges are half open. The persistent PG ID is:

```text
base64url_without_padding(namespace) + "/cell-" + decimal_slots_joined_by_hyphen
    + "/v" + decimal_start + "-" + decimal_end
```

`testdata/routing-v1.json` freezes independently calculated selectors for three
ring sizes, aliases/prefixes, UTF-8 and tagged names. It includes two names with
the same vshard and different legacy owners, so a vshard-only bootstrap fails.
`testdata/map-v1.json` freezes a complete map with repeated bootstrap destinations.
Consumers must compare these fixtures before sharing placement documents.
Explicit legacy holes are `blackhole:true` store entries without endpoints;
they preserve bootstrap slots but provide no historical coverage. Their replica
read/source lists are empty. Repeated slots may use the same incarnation ID.

Document digest is lowercase SHA-256 of Go `encoding/json.Marshal(Document)`:
compact struct fields in the declared source order, arrays preserved, `nil`
arrays encoded as `null`, and Go JSON string escaping (including HTML escaping).
Hash decoded typed documents, never arbitrary input formatting. Schema v1 rejects
unknown and duplicate fields. The map fixture and digest test freeze this byte
contract for other implementations; changes require a schema version bump.

## Map and receipt rules

Maps contain explicit write, read, and copy-source authorities. Store IDs name
storage incarnations, independently from addresses. Every group/replica includes
a unique authenticated producer-incarnation/stream inventory. Revision one must
exactly match every legacy slot at epoch one. Successors preserve all selectors,
bootstrap slots and replica identities; require revision +1, predecessor digest,
a new operation ID and a forward epoch step for a writer change. Read/source
changes preserve the epoch. Producer census changes require a fenced epoch.
Read/source retirement is deliberately rejected until a separate coverage and
reader-pin retirement protocol is integrated.

`ValidateSuccessor` only checks structural compatibility. Adoption uses
`ValidateTransition`, requiring exactly one activation for every changed epoch.
A fence binds the predecessor map, PG, replica, old store incarnation, operation,
epoch, complete producer census and final durable sequence for each lane, plus
owner frontier vector and part/catalog/policy/tombstone roots. Coverage binds the
new map/epoch, target incarnation, exact fence digest, dominating frontier and
all declared historical coverage roots. An activation cannot be inferred from a
phase string, elapsed time, an empty queue or a successful socket write.

These are content-addressed attestations, not signatures or proofs derived from
raw bytes. An authenticated trusted storage/transport adapter must issue them.
The receipt validator checks identities and complete declared coverage, not the
truth of an external adapter's attestation. No per-name digest is introduced.

## Durable authority and daemon integration

`OpenAuthority` binds a private durable root to one PG/replica/store incarnation.
A fresh root starts at revision one. Reopening requires the persisted map digest;
a stale supplied document fails. Control updates use temp file, fsync, rename,
and directory fsync. The owning daemon must enforce exclusive process ownership
of that root and keep it durable; do not instantiate two live authorities there.

`Register` creates a serial sink admission lane. `Begin` checks an immutable map,
store, epoch and authenticated producer identity; one local atomic counter covers
the batch until release. Writers take no shared mutex. A fence first blocks and
persists the closed-epoch floor, waits for active batches, and invokes a durable
seal callback. Failure or interruption keeps the epoch closed across restart.
Completed fences and accepted maps are persisted; a config rollback never lowers
the floor. Adoption requires quiescent sinks and verified activation receipts.

Configure `v2daemon.Config.Placement` with this authority. Use `AppendPlaced` or
`AppendApplicationsPlaced`; the old append APIs and numeric append reject an
unversioned request when placement is configured. Entire batches are checked
before catalog/journal mutation. `FencePlacement` drains accepted journals,
synchronously seals each core producer and gives its durable owner cursor vector
to the coverage certifier. The certifier must check real durable stream ACKs,
pin a coherent immutable manifest, and return roots. A nil/failed certifier emits
no receipt and leaves intake closed. Producer lifecycle calls must be quiescent
during this control operation. Direct core APIs remain trusted internal APIs.

Placed journal application envelopes retain the original group, replica, map
digest, epoch and authenticated lane under the record checksum. Startup validates
each retained record against the persisted authority and checks that its names
belong to the group before submitting the original applications to core. Reopen
requires placement for placed records; enabling placement on legacy unstamped
records is rejected. Replay can recover the accepted prefix of a closed current
epoch while fresh intake stays fenced. A changed map, epoch or writer incarnation
is rejected; historical replay through certified activation coverage remains a
separate integration seam. Replay never restamps a record with the current map.

Duplicate receipt/ACK storage, actual transport authentication, whole-part
coverage attestation and ordered-tail integration are remaining seams. Plain TCP
does not authenticate a producer lane and remains unavailable for placed L3
intake. This package does not fabricate those proofs or authorize online moves.

## Storectl use

```sh
storectl placement --map placement.json --name metric.path --replica site-a \
  --expect-digest <document-sha256> --expect-epoch 1
```

Output separates writer, readers and copy sources and includes the stable PG,
vshard, map digest and revision. Expected digest and epoch are mandatory; stale
selection fails before output. This command selects exact names only; it does
not infer groups from globs, run a copy or make `storectl sync` epoch-safe.
