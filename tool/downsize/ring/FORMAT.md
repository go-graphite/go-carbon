# Ring plan format, version 1

Published 2026-10-28. This is the contract for `ds-move`. The planner only reads
inventories and writes local files; it never changes a store or configuration.

A plan is a directory. `manifest.json` is the commit marker: consumers MUST NOT
execute a directory without it. It is published atomically after all inputs
finish. `state.json` and `input-*.state.json` are planner-private resumable state,
not permission to move.
All paths in the manifest are relative to this directory. Reject unknown versions,
absolute paths, parent traversal, truncated streams, count or SHA-256 mismatches.

`manifest.json` has `format: "go-carbon-ring"`, `version: 1`, `plan_id` (SHA-256
of normalized input specification), `spec` (the complete old/new topology and input
specification), `summary`, and `streams`. Each stream has `path`, `source`,
`destination`, `metrics`, `estimated_bytes`, `sha256` (whole file bytes).
The summary counts inventory **records**, records requiring at least one copy,
copy operations, estimated bytes copied, unexpected old owners, and moved fraction
(`moved_metrics / metrics`). This fraction describes physical inventory records,
not unique names across replicated inventories. Byte estimates are not exact
Pebble SST sizes or transfer sizes.

Each UTF-8 JSON-lines stream describes one source → destination pair. Order is
input order within an inventory segment. A pair can have many streams; consume
all of them. Newlines terminate every record, including the final record.

```jsonl
{"type":"header","format":"go-carbon-ring","version":1,"plan_id":"example","source":"metrics-003","destination":"metrics-000"}
{"type":"move","metric":"example.metric","estimated_bytes":600}
{"type":"end","metrics":1,"estimated_bytes":600}
```

The header source/destination are physical member IDs, resolved through `spec`.
IDs identify machines/storage incarnations, not hashing identities: a replacement
machine MUST get a new ID even when its ring index/instance is unchanged.
`metric` is the exact name hashed by ingestion, without a trailing `.wsp`, newline,
normalization, or routing rewrite. A move means **copy and merge then verify**;
it never authorizes deletion. On replication-factor changes, one record may
produce several destination copies. A destination already represented by the
same physical ID among the old owners needs no copy. Unexpected source ownership
aborts by default. With explicit `allow_unowned`, such records are reported and
copied to missing destinations; the mover must reconcile duplicates before deleting.

Deletion requires the mover's separate durable verification, ingestion cutover
barrier, drained old writers, final delta merge and guarded source-version check.
Old owners and read backends remain online until that process completes. Inventory
is not a consistent snapshot of ongoing ingestion: re-list and reconcile at the
barrier. `plan_id` identifies routing and inventory specification, not live data
versions. Mover state should key on `(plan_id, stream sha256, record ordinal)` and
make repeats idempotent. Consume only committed streams listed by the manifest.

Input specification (`--spec` JSON):

```json
{
  "version": 1, "cluster": "metrics", "site": "region-a",
  "old": {"hash":"jump_fnv1a","replicas":1,"nodes":[
    {"id":"a","node":"old-a:2003=000","endpoint":"http://old-a:4242"},
    {"id":"b","node":"old-b:2003=001","endpoint":"http://old-b:4242"}
  ]},
  "new": {"hash":"jump_fnv1a","replicas":1,"nodes":[
    {"id":"a","node":"old-a:2003=000","endpoint":"http://old-a:4242"}
  ]},
  "inputs":[
    {"source":"a","path":"a.txt","format":"lines","estimated_bytes":600},
    {"source":"b","path":"b.txt","format":"lines","estimated_bytes":600}
  ]
}
```

Relative input paths resolve against the specification file. Files must be in
strict bytewise name order with no duplicates within an input. Split a source's
sorted inventory into nonoverlapping files for more concurrency; overlap across
files is the operator's responsibility. `lines` is one raw metric per line;
`tsv` is `metric<TAB>nonnegative_estimated_bytes`; `jsonl` is
`{"metric":"...","estimated_bytes":600}`. Zero bytes means unknown/zero,
never a measured estimate. `format: "buckyd"` uses `url` instead of `path` and
streams the existing GET `/metrics` JSON array. File checkpoints resume at a
durable segment byte offset; HTTP inventories restart an incomplete input because
the existing endpoint has no bounded snapshot paging contract. Authentication is
read from a named environment variable `token_env`, sent only as
`X-Buckyd-Authorization`, and never stored in plan files.

Every old physical member needs an inventory, including survivors. An explicit
`partial_inventory: true` permits a scoped plan and its fraction then describes
only that subset. A completed HTTP inventory is reused on resume; use a new
output directory for a fresh live inventory. File inputs are immutable: resume
checks size and nanosecond mtime and stream checksums, not a fresh full input hash.
Do not edit/replace input files or restore their mtimes. A stale file replaced
with exactly matching size/mtime is outside this contract.

Hashes: `carbon`/`carbon_ch`, `fnv1a`/`fnv1a_ch`, `jump_fnv1a`/`jump_fnv1a_ch`.
`replicas` means physical copies (default 1); `virtual_nodes` means token count
per member for carbon/fnv1a (default 100). Jump buckets follow buckytools instance
ordering (lexical; use zero-padded `000`, `001`, …), then configuration order for
uninstanced members. Production jump relay has one copy per site and explicit
contiguous integer indices. Do not mistake virtual tokens or site replication
for `replicas`. Changing node order/index, algorithm or carbon hostname can move
surviving owners too; the planner compares both complete rings.

Jump replication greater than one is rejected because neither production relay
supports it and the reused buckytools implementation is unsafe for that case.
Carbon/FNV support up to 64 distinct physical replicas. Their legacy buckytools
`Replicas()` accessor reports virtual tokens; this format deliberately names both
concepts explicitly. Carbon/FNV copies already owned by another old member are
assumed healthy; this is a resize plan, not a missing-replica repair inventory.
Names duplicated across old replica inventories can yield duplicate copy/merge
operations. The mover must tolerate repeats. Use explicit decimal `:2003` for
FNV: an omitted port is represented as zero by buckytools/buckyd, unlike the
ingestion listener's default. Version 1 member syntax supports DNS/IPv4, not IPv6.
