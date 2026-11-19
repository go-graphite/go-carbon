# v2 catalog discovery

`Catalog` is an immutable projection of live names, not another durable catalog.
`CarbonserverListener.SetV2Catalog(catalog)` selects the configured classic trie
or trigram implementation for find and wildcard expansion. Discovery caches are
bypassed while attached, so neither a cached miss nor a cached expired name can
outlive a publication. No filesystem names are mixed into the attached view.

The serial lifecycle/admission control owner publishes a **host-wide union**:

```go
catalog := &v2index.Catalog{}
// Complete recovery and capture the coherent all-owner live-name view first.
if err := catalog.Publish(1, liveNames); err != nil { return err }
if err := listener.SetV2Catalog(catalog); err != nil { return err }
// Publish only accepted arrivals and committed, incarnation-fenced expiry.
return catalog.Apply(2, admittedNames, expiredNames)
```

`PublishCore(ctx, revision, snapshots, live)` is the first core consumer of
`v2core.ScanCatalogNames`. Supply pinned snapshots for **all** hosted owners and
a visibility predicate from lifecycle metadata pinned with those snapshots.
L3 core metadata preserves exact canonical names. L0/L1/L2 numeric raw keys do
not; those layers cannot recover names with this adapter. `live == nil` is valid
only when lifecycle deletion is disabled. The separate lifecycle checkpoint can
also publish its `Snapshot.Metrics` names directly with `Publish`.

The integration owner must attach this source and publish it at admission/seal,
expiry and replay boundaries. This PR does not alter daemon/receiver ownership,
automatically wire a new listener, implement expiry or create durable IDs.
Never concurrently inspect a producer's mutable `partlog.NameCatalog`.

Each query pins an immutable image. The first query after membership changes
builds a replacement off the writer path; other builders serialize, existing
readers keep their old image. Failed builds return an error without replacing
the preceding image. Both backends rebuild completely on membership changes.

The snapshot keeps names in a few immutable, sorted, disjoint levels, each a
front-coded byte slice (LevelDB/Pebble data blocks, VictoriaMetrics mergeset
items): 32 names per block, every entry stores the prefix length it shares with
the previous name plus the rest, and each block starts with a whole name. A
seek binary-searches block starts and decodes at most one block; position `i`
decodes block `i/32` only. A publication encodes its new names as a small level
and shares untouched levels; a level merges into its predecessor once it is
more than 1/8 of it (fanout 8), so 10M names keep about 3-4 levels. Name bytes
are copied out of the caller's strings, so the index holds no string headers.
This deliberately reuses the earlier operational-parity prototype rather than
introducing another glob engine. It is **not** an incremental 10M-name design:
latency, old/new overlapping images and rebuild cost must be budgeted before
activation. Core refresh scans keys but does not decode archive values; it has
no dedicated metadata directory, so scan work scales with stored keys.

Run functional/race checks normally. The opt-in 10M measurement is reproducible:

```sh
V2_INDEX_SCALE=10000000 V2_INDEX_BACKEND=trie GOTOOLCHAIN=go1.27.0 \
  go test -mod=vendor ./carbonserver -run '^TestV2CatalogScale$' -v -count=1 -timeout=15m
# Repeat with V2_INDEX_BACKEND=trigram, in a separate process.
```

The test reports catalog construction time, index build time, retained Go heap,
index heap delta, and total allocations. Use `/usr/bin/time -l` on the compiled
test binary for peak RSS. Synthetic names are public, deterministic service/host
names; these numbers are laptop diagnostics, not production qualification.
