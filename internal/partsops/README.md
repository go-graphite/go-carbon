# Immutable-parts operational prototype

This package supplies owner-local catalog expiration, whole-partition retention
and a catalog adapter for the existing discovery/quota engine. It is experimental
and has no receiver, server, scheduler or daemon activation path.

`tool/parts-ops-check` is the first consumer: it previews decisions from an
exported catalog and the existing policy files without opening a serving store.

```sh
go run ./tool/parts-ops-check -input catalog.json \
  -now 2026-01-02T00:00:00Z -expiration-default 192h \
  -expiration storage-expiration.conf -quotas storage-quotas.conf \
  -schemas storage-schemas.conf -report-frequency 1m
```

The input has `Metrics`, `Partitions`, `Queries` and `Events` arrays. Metric rows
contain `Scope` (`PlacementGroup`, `VShard`), `Name`, `Retentions` (`Step`, `Count`),
`Generation`, `Revision`, `LastUpdate` (RFC3339) and `Pending`. Events contain
`Name`, `Points` and `InCache`. A missing activity time exercises initialization;
it never means the metric is already old. Example:

```json
{
  "Metrics": [{
    "Scope": {"PlacementGroup": "service", "VShard": 0},
    "Name": "service.cpu", "Retentions": [{"Step": 1, "Count": 60}],
    "Generation": 1, "Revision": 1, "LastUpdate": "2026-01-01T00:00:00Z"
  }],
  "Queries": ["service.*"],
  "Events": [{"Name": "service.cpu", "Points": 1, "InCache": true}]
}
```

The example scope is illustrative; an actual owner uses
`partsformat.Route(partsformat.ID(name))`. The checker previews the supplied
export and never applies it. `-trie=false` selects the existing trigram matcher.
Physical quotas follow shared-storage rules: finite positive physical limits
require explicit `-ignore-physical-quotas`; other quota controls remain active.

## Owner and persistence boundary

Each `(exact placement group, routing vshard)` has one serial control/seal owner.
`Owner.Apply` compares observed metadata revisions, calls a durable `Committer`,
then publishes a copied immutable catalog image through an atomic pointer.
`Snapshot` readers share no lock with the owner. Activity is the wall-clock
arrival/commit time, never the newest sample timestamp. Pending buffers and
unfinished replay must be included before making expiration decisions.

`OpenCatalog` implements the metadata committer on an existing Pebble DB. It
reads only its `GCOP` metadata prefix when opening. Each mutation writes catalog
rows, incarnation tombstones, retired-partition markers and the owner revision
in one synced batch. A failed durable commit fences the committer until reopen.
The caller owns the database lifetime and exclusive ownership of each scope.

`EncodeEdits` exposes the same edits for an eventual data/catalog/frontier atomic
ingestion. It returns point edits and whole-range tombstones separately; SST
staging must sort them using the engine comparator and publish them with the
matching data/frontier. The standalone `OpenCatalog` adapter does **not** make
independent data SST ingestion atomic with metadata.

`Batch.Activity` updates an existing incarnation's exact arrival time and
revision without repeating its policy definition. The daemon uses a full metric
upsert for a new name and activity edits for later seals, publishing them in the
same atomic ingest as points and the replay frontier. The scoped `a` record has
a versioned 30-byte value containing generation, revision, Unix seconds and
nanoseconds. `OpenCatalog` overlays activity on its matching definition; a newer
full definition wins, and tombstones or a newer incarnation prevent stale
activity from reviving a name. Activity without a matching definition is an
error unless a tombstone already covers that incarnation.

This extends the `GCOP` metadata format. Readers, copy validation and catalog
operators must understand activity records before opening stores written by
this daemon. Older catalog readers reject the unknown record kind; downgrade
requires rebuilding the catalog with complete current metadata first. GCP3
data keys, point values and private replay checkpoint encodings are unchanged.

Expiration reuses `persister.WhisperExpirationRules`: first active matching rule,
exclusions, grace, timed exemptions and fallback all retain their existing
meaning. Dry-run never initializes activity or deletes. A missing LastUpdate
gets a fresh activity revision before it can expire. A stale observed revision
cannot delete more recent committed activity. Existing tag normalization is
called only when that ingestion option is enabled; opaque names remain opaque.

## Deletion and visibility contract

Part-first GCP3 keys do not permit one metric to be range-deleted. An inactivity
delete removes its catalog row and persists a scoped name-incarnation tombstone
without visiting data keys. Readers and copies must apply it to **all** archives,
head parts and the legacy base. Occupied NaNs remain data. `Visible` describes
the single-authority incarnation predicate; a missing tombstone keeps legacy
generation zero visible. A recreated name needs a strictly newer durable epoch.

`Metadata.Generation` here must be a **portable monotonic incarnation from an
agreed authority**, never an existing replica-local chunkstore generation.
Independent writers need either a fenced shared epoch protocol or explicit
origin-qualified tombstone coverage. That authority, live-over-converted-legacy
priority, copy target-wins behavior and the production value envelope remain
unimplemented. This package must not authorize live copies, READY or production
expiration until the integration establishes them.

## Retention and indexes

Partitions record the maximum actual stored policy horizon of their contents
and exclusive end of every represented block interval, including rollups.
`Owner.Reclaim` only retires sealed partitions whose entire interval is beyond
that longest horizon, recording the marker and one GCP3 range delete atomically.
Existing Pebble snapshot pins retain the older view. Repeating a reclaim is
idempotent. Importers and sealers must reject retired partitions. Physical file
reclamation, replay/copy frontier pinning and retirement-marker garbage
collection belong to the part engine.

Mixed-policy partitions conservatively use the largest contained horizon. To
reclaim physical bytes at each schema's exact horizon, the engine must emit
policy-homogeneous deletion units; it must not delete shorter-policy rows one
point at a time. Policy reload must not replace the manifest's stored horizon
with a shorter current configuration.

`carbonserver.PartsCatalogIndex` calls the existing trie/trigram/brace matcher
and quota throttler. `Next` constructs a new find image while preserving consumed
throughput for surviving configured namespaces and recalculating storage usage
after removals. Serialize `Next` and quota reload with admission; keep old find
objects pinned for in-flight reads. Only `RefreshUsage`, on the existing report
schedule, rotates throughput windows. Never rebuild a fresh admission index at
every seal. Root/namespace matching, schema point capacity, logical size, empty
packets, cache bypass, dropping-policy and rule order remain the existing engine's
decisions. This is no new quota algorithm.

Catalog publication currently copies the shard's metadata map/slices and rebuilds
its find image. That establishes semantics, not a large-catalog allocation or
scaling result. Incremental publication needs measurement before deployment.
Host-wide quota admission requires a coherent union of all hosted owner catalogs
and one ordered admission state. Building separate quota indexes per vshard
would multiply limits and violate parity. The host snapshot vector and scalable
admission integration are not supplied by this prototype.

## Verification

The tests compare expiration to the current `carbon.metricExpirer`, quota
translation to the existing config converter, throttling to the current trie,
and find to both existing backends over service/namespace/host-shaped names.
They cover retention horizons against classic Whisper, commit failures, stale
deletes, recreation, durable reopen, abrupt process exit, old snapshot pins and
cross-placement-group partition isolation.

An optional `PARTSOPS_EXPIRATION_FIXTURE` points at a private JSON array of
`{ "File": "rendered-policy.conf", "Names": ["..."], "DefaultHours": 192 }`
for differential testing locally rendered deployment policies without putting
private configuration in the repository.

```sh
GOTOOLCHAIN=go1.27.0 go test ./internal/partsops ./carbonserver ./carbon ./tool/parts-ops-check
```
