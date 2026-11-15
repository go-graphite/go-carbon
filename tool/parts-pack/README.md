# Offline packed-part accounting

This diagnostic reads closed fixture databases. It never opens a running daemon's
database and is not a migration or production publication command. Use a fresh
output directory; failed runs can leave partial diagnostic output there.

```sh
go run ./tool/parts-pack /offline/fixture/legacy /offline/fixture/converted
go run ./tool/parts-pack -repack /offline/fixture/converted /new/daily-output
go run ./tool/parts-pack -repartition /offline/fixture/converted /new/wide-output 4
```

The input values are the converter's `GCA2` archive images and keys use the existing
portable format. `-repack` preserves the keys. `-repartition` is an explicit,
offline experiment: choose each archive's width as the greater of one day and the
least of 4,096 archive steps, 30 days, and its horizon divided by the last argument
(default 8). It does not change the portable key encoding or pack an archive
ordinal into the partition number. Part identities are deterministic fixture
identities, not a production writer-incarnation allocator.

The writer emits direct cold-pack values and a shared `policy`, ingests SSTs,
compacts, closes, reopens read-only, and verifies the persisted key/value digest.
Every block is decoded and compared by timestamp and IEEE bits before writing.
`layout.json` records the experimental version, policy digest, archive intervals
and geometry, and explicitly sets `production_ready` to false. These intervals
are diagnostic records; compaction may put multiple archives into one physical
SST. They do not implement a production manifest or physical expiry protocol.

The reported bytes include all live SSTs, the policy and the layout file. WAL,
MANIFEST and filesystem allocation are excluded, matching the fixture's live-SST
comparison. Production provenance/catalog costs remain unknown. Report physical
SST size separately from raw keys/values and uncompressed index properties: those
properties cannot be added to compressed SST bytes. Legacy inspection accepts
only fully compacted fixtures and rejects merge operands.

The tool bounds input key/value data to 128 MiB; sorting and decoded point images
consume additional memory. It rejects mixed policies, changing ownership, empty
archive images and duplicate stored timestamps rather than inventing catalog or
version semantics. It is not a general overlapping-part compactor.

`-sample DIR` reads bounded `sample-*.pb` carbonapi v3 MultiFetchResponse files.
It compares payload bytes and alternating seal CPU/decode p99 arms against the
same existing cold codec. Render responses include missing-slot NaNs and do not
expose physical occupancy or every stored archive, so this mode cannot establish
stored B/point or end-to-end render latency. It fabricates no archive policy;
its before-payload estimate adds only the one-archive GCA2 envelope. It excludes
all SST/key/catalog costs and is labeled accordingly in its output.

Validation:

```sh
GOTOOLCHAIN=go1.27.0 go build ./internal/partspack ./tool/parts-pack
GOTOOLCHAIN=go1.27.0 go vet ./internal/partspack ./tool/parts-pack
GOTOOLCHAIN=go1.27.0 go test -race ./internal/partspack ./tool/parts-pack
```
