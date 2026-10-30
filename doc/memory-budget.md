# Memory budget

`[common] memory-budget` is an optional startup policy. The default, `""`, keeps
the existing cache sizes and Go runtime limit. `"0"` also disables the policy.
Set it to a byte count with an optional `B`, `KiB`, `MiB`, `GiB` or `TiB`
suffix (the suffixes `GOMEMLIMIT` accepts) or an integer percentage from 1% to
100%. The resolved budget must be at least 64 MiB.

```toml
[common]
memory-budget = "100GiB" # alternatively "48%" or "107374182400"
```

For a percentage, Linux uses the smaller of physical host RAM and the process's
visible cgroup v1/v2 memory limits, including parent limits. macOS uses physical
RAM. On other operating systems, use a byte count. Detection errors reject the
configuration. The policy does not watch changing limits after startup.

| Setting when omitted from TOML | Share | At 100 GiB |
| --- | ---: | ---: |
| `[whisper] store-cache-size` | 13% | 13 GiB |
| `[whisper] store-metadata-cache-size` | 13% | 13 GiB |
| Go runtime soft memory limit | 70% | 70 GiB |

The Go limit includes the metadata and write caches; those shares are not added
to it. The remaining 30% holds the native Pebble cache (13%) and other native
allocations, memtables, mappings and headroom. RAM outside the budget serves the
page cache, which holds compressed table blocks for reads.

The shares come from a fleet of shared-store hosts sized by hand, which ran the
block cache at about 6% and the Go limit at about 33% of RAM: `memory-budget =
"48%"` reproduces both within a few percent, so one value replaces two per-host
sizes and follows the hardware when hosts change. Hosts whose metadata cache
needs differ keep an explicit `store-metadata-cache-size`.

The write cache's byte limit (`[cache] max-bytes`) is not derived. Its byte
estimate is approximate, and a byte limit rejects live points; the write cache
stays bounded by `max-size` (and `max-bytes` when set) and lies inside the Go
limit. Pebble's cache budget also accounts
for memtable reservations. Without cgo, Pebble allocations use the Go heap, so
the same Go limit also covers that cache and leaves less Go heap headroom.

Explicit TOML cache keys always win, including zero. An existing `GOMEMLIMIT`
environment variable, including `off`, wins over the derived Go limit. Overrides
do not redistribute the other shares and can exceed the envelope. A config
generated with every default cache key is therefore fully explicit: remove those
keys if the policy should derive them. `max-size` still applies independently.

The budget is an estimate, not an RSS cap. Metadata occupancy differs from arena
capacity and transient allocations. The Go limit is soft
and excludes native allocation and file-backed mappings. Compression, admission,
query caches, input logging, memtables and compaction are otherwise configured
as before. The fractions are a starting policy, not a measured read-working-set
recommendation. Size the read cache and Go headroom for the workload before use.

Startup logs the resolved values once (`memory budget resolved`: budget bytes,
both cache sizes, the Go limit and whether it came from the budget or
`GOMEMLIMIT`). Changing the budget or resolved Go limit requires a restart; a
reload that changes them fails with an error and changes nothing. Ordinary reloads
still apply the existing individually reloadable settings. The feature adds no
work to ingestion, state lookup, discovery or fetch paths.

To reproduce a deployment's current settings, leave `memory-budget = ""`, retain
its explicit cache sizes and keep its existing `GOMEMLIMIT` and `GOGC`. The
repository defaults remain a 256 MiB store cache, a disabled metadata cache and
a disabled write-cache byte limit. A single set of fixed fractions cannot
reproduce deployments with different cache/heap proportions; preserve their
explicit overrides when enabling the policy incrementally.
