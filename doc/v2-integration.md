# Experimental v2 daemon integration

The experimental v2 work is integrated on `atimofieiev/v2` in
[Draft #1488](https://github.com/go-graphite/go-carbon/pull/1488), based on master.
It joins the component Drafts through real merge commits and connects TCP intake,
the mandatory journal, canonical rollups, immutable read snapshots, discovery,
logical inactivity expiry and host-wide quota admission.

```toml
[v2]
enabled = true
path = "/absolute/path/to/v2-root"
producers = 2
shards = 2
read-budget-bytes = 268435456
```

The selector defaults off and is restart-only. Enable TCP and carbonserver, and
disable UDP, pickle, CarbonLink, cache gRPC, custom receivers, buckyd, classic dump
and classic input-log. The mandatory v2 journal lives below its separate root;
disabling classic input-log never disables v2 durability. Geometry is persisted.
The experimental runtime rejects policy reloads; apply configuration at restart.

TCP points pass through bounded producer queues and producer-local policy caches
and journals. Each point is an explicit application with original order, policy
and admission clock, independent of socket batch cuts. Journal sync and idle seal
run every second. Rollups run at seal time. Normal runtime seals use bounded dense
slot runs, preserving all value bits and replacement orders. The public core hook
`DenseRollups` opts into that codec; existing core callers retain their singleton
encoding. Read, conversion and export understand the dense runtime output.

Readers pin immutable owner snapshots and detached pending applications. Render,
info and find use those publications; the catalog refreshes once a second outside
writer callbacks. Render caches without snapshot identities are disabled. No
reader acquires a writer lock. First access to a sealed generation still scans an
owner, and catalog/quota refreshes rebuild metadata indexes; large-catalog cost,
complete retained-memory bounds and production capacity remain unqualified.

## Historical classic data

Existing Whisper files and shared classic stores open read-only. Initial discovery
includes classic names before the first v2 write. Union reads use the stored
retention ladder and overlay occupied v2 physical slots, including NaNs and newer
ring replacements outside the requested interval. Unoccupied slots retain their
classic values. Data and reset authority are taken from the same pinned v2 image.
The classic roots and v2 root must be separate.

Overlapping intake requires identical stored policies. Single-archive overlap is
supported. **Multiarchive overlap is rejected** because a reducer starting empty
cannot correctly update a coarse boundary bucket containing classic fine slots.
Classic-only multiarchive history remains readable. A short-retention multiarchive
canary therefore remains blocked until boundary seeding or an equivalent qualified
union reducer is available. Arbitrary classic cache/persister UpdateMany grouping
parity is also unqualified: TCP's explicit point applications are a different
application boundary.

Classic remains immutable throughout v2 operation. Safe classic-write fallback
requires durable cross-engine write epochs, which are absent. The current binary
refuses classic-only startup when a configured v2 root is nonempty. Do not use an
older classic-only binary to roll back; it would hide the v2 interval and later
classic corrections have no precedence authority on a subsequent v2 reopen.

## Expiry and quotas

Existing `whisper.store-expiration`, expiration-file rules, grace, exclusions and
dry-run settings control logical v2 inactivity expiry. Original activity clocks,
not sample timestamps, select expiry. Replay finishes before maintenance activates;
decoded, queued and journal-buffered input holds an expiry reservation until its
core seal is durable. Those reservations use per-name atomic counters; their
metadata cells are retired when the count reaches zero.

An idle owner seal writes an expired flag and reset-order barrier with its durable
frontier. Reset records and expiry-enabled owner frontiers use version 2 so an
older runtime fails closed on reopen.
Readers remove the name and mask all covered archive slots and classic
history. A resumed name retains its mask and advances the existing producer order;
old slots cannot reappear after restart or after expiry is disabled. An old pinned
snapshot remains valid. These barriers belong to the original journal stream;
cross-stream installation and generation allocation remain external contracts.

Existing quota rules use one host-wide classic quota index, covering classic,
active v2 and accepted pending names. New-name reservations serialize metadata;
existing-name admission uses atomic throughput accounting. Catalog replacement
releases expired logical storage while retaining consumed throughput. Physical
quotas are rejected unless explicitly ignored by the existing setting, because
shared immutable files have no per-name physical byte ownership. `Rejected()`
reports quota admission rejections independently of TCP decode counters. These
experimental counters are not yet a complete operator metrics surface.

Logical expiry does not physically reclaim old part bytes. Partition zero contains
ring replacements and is not an aged deletion unit. Whole-part reclamation,
compaction of reset history and safe reset/tombstone GC remain unimplemented; never
range-delete that partition or open a standalone lifecycle writer on a core root.

## Placement and recovery

Startup replays through the normal owner queues before listeners open. Owner
frontiers suppress committed prefixes; original application order is recovered
before further intake. Placement journals retain exact checksummed stamps and
validate original group, map, epoch, producer lane and owned names during replay.
An accepted prefix in a fenced current epoch can recover while intake stays closed.
Historical map/epoch changes remain fail-closed pending certified coverage.
Authenticated stamped TCP is absent; placed plaintext/stream TCP is rejected.

Portable dense exports, manifest snapshots and whole-file copy helpers are joined,
but live install/copy endpoints, authenticated adoption and historical coverage
are not wired. Copies retain immutable files and use complete part checksums;
there is no per-name digest path.

The [part-copy commands](../tool/storectl/README.md) stage retained files and
resume by whole-part checksums. The [shadow harness](../tool/v2-shadow/README.md)
is an explicit diagnostic command; it does not enable daemon shadowing. The
[converter](../internal/v2convert/README.md) has bounded parallel scan workers;
verified conversion remains serial by input transaction. These tools do not
qualify live adoption, rollup parity or disk-speed conversion. New v2 changes
target the integration branch.

## Local verification

Use Go 1.27 with vendoring. Component ordinary/race suites cover the codec,
canonical classic matrix, placement replay and SIGKILL cuts. Application tests
cover real TCP -> HTTP render/find/info, independent Whisper values, Whisper and
shared-store union, restart with an open sender, quota rejection/release, actual
expiry and resumed-name masking. These local tests do not authorize or qualify a
production canary, relay change, live copy adoption or general rollback.

## Sealed archive merging

Writable v2 owners automatically merge sixteen local archive parts in the same
size tier into one immutable part. Tiers span a factor of sixteen in file bytes;
when merge service keeps up, each group retains at most fifteen local parts per
tier. Background workers scale with the owner count and `GOMAXPROCS`: one
worker per two scheduler CPUs, bounded by the number of owners (at least one
worker on small stores). Set the restart-only `CORE_MERGE_WORKERS` environment
variable to `0` to disable merges for comparison, or to a smaller positive worker
count to reserve more CPU for intake. Counts above the owner/CPU budget fail
configuration validation. Workers yield for 1ms after each 4096 input records,
stop before owner shutdown, and skip unchanged inventories with no merge set.

The inventory target is soft: a seal never waits for a merge to catch up. Owners
with 32 or more catalog-live parts increment `MergeOverBudgetSeals` while
continuing intake. This total-inventory pressure signal includes all tiers and
copied parts; it is not a per-tier backlog limit. `v2-write` reports per-owner
`part_gc` statistics; Prometheus exports `v2_sealed_parts`,
`v2_merge_over_budget_seals_total` and `v2_merge_runs_total`. Sustained growth means
merge capacity or seal sizing needs attention; the target is not a hard disk or
memory bound.

The existing descriptor catalog atomically replaces all inputs with the synced
output. Old snapshots keep their input files behind the existing GC fence and
one-minute grace. Interrupted output is an unpublished orphan and is collected
on restart; interrupted private merge directories are removed before opening
readers. `PartGCStats` reports live original parts, completed merge runs, and
input/output bytes in addition to retirement statistics.

Merges preserve original timestamps, value bits, correction flags and source
orders. They copy authoritative coarse slots directly. Durable reset and expiry
masks remove obsolete records before winner selection. Age-only slots remain
available because later coarse boundary calculations may need their exact
values. Copied parts keep their original move catalog and authority; background
merges combine only local originals from the owner's persisted source stream.
