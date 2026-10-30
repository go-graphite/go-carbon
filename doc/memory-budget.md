# Memory budget

`[common] memory-budget` is an optional startup policy. The default, `""`, keeps
the existing cache sizes and Go runtime limit. `"0"` also disables the policy.
Set it to a decimal byte count in a string or an integer percentage from 1% to
100%. The resolved budget must be at least 64 MiB.

```toml
[common]
memory-budget = "107374182400" # 100 GiB; alternatively "80%"
```

For a percentage, Linux uses the smaller of physical host RAM and the process's
visible cgroup v1/v2 memory limits, including parent limits. macOS uses physical
RAM. On other operating systems, use a byte count. Detection errors reject the
configuration. The policy does not watch changing limits after startup.

| Setting when omitted from TOML | Share | At 100 GiB |
| --- | ---: | ---: |
| `[whisper] store-cache-size` | 20% | 20 GiB |
| `[whisper] store-metadata-cache-size` | 20% | 20 GiB |
| `[cache] max-bytes` | 10% | 10 GiB |
| Go runtime soft memory limit | 70% | 70 GiB |

The Go limit includes the metadata and write caches; those shares are not added
to it. The remaining 30% allows 20% for the native Pebble cache and 10% for other
native allocations, mappings and headroom. Pebble's cache budget also accounts
for memtable reservations. Without cgo, Pebble allocations use the Go heap, so
the same Go limit also covers that cache and leaves less Go heap headroom.

Explicit TOML cache keys always win, including zero. An existing `GOMEMLIMIT`
environment variable, including `off`, wins over the derived Go limit. Overrides
do not redistribute the other shares and can exceed the envelope. A config
generated with every default cache key is therefore fully explicit: remove those
keys if the policy should derive them. `max-size` still applies independently.

The budget is an estimate, not an RSS cap. Metadata occupancy differs from arena
capacity and transient allocations; the write cache limit is approximate and
can reject live points, as its existing byte limit does. The Go limit is soft
and excludes native allocation and file-backed mappings. Compression, admission,
query caches, input logging, memtables and compaction are otherwise configured
as before. The fractions are a starting policy, not a measured read-working-set
recommendation. Size the read cache and Go headroom for the workload before use.

Changing the budget or resolved Go limit requires a restart. Ordinary reloads
still apply the existing individually reloadable settings. The feature adds no
work to ingestion, state lookup, discovery or fetch paths.

To reproduce a deployment's current settings, leave `memory-budget = ""`, retain
its explicit cache sizes and keep its existing `GOMEMLIMIT` and `GOGC`. The
repository defaults remain a 256 MiB store cache, a disabled metadata cache and
a disabled write-cache byte limit. A single set of fixed fractions cannot
reproduce deployments with different cache/heap proportions; preserve their
explicit overrides when enabling the policy incrementally.
