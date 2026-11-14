# Classic archive anchor maintenance

The per-name continuation importer formerly placed classic archive points in
absolute epoch slots. Classic Fetch instead uses physical slot zero's timestamp
as its ring anchor. A sparse restored archive can therefore contain history that
Fetch cannot see.

`POST /v1/maintenance/archive-anchors` accepts
`{"names":["metric.name"],"apply":false}` (at most 32 explicit names). Dry-run
requires root read authorization; apply requires root update authorization and is
fenced while whole-store copy is active. Each result reports archive point count,
misplaced slots, changed archives and any blocked layout. No input flush occurs.

One request prepares up to `[buckyd] maintenance-workers` names at once
(default 8, at most 32) and replies in request order; at most
`max-maintenance-requests` requests (default 2, at most 64) are admitted, else
429. Each name still reads its own snapshot and applies under its own transfer
stripe; a repeated name runs in request order. A request lasts as long as its
slowest name, so a census client gains most by keeping several requests in
flight (raise `max-maintenance-requests` to match).

The daemon reads immutable committed bytes and prepares an exact classic-slot
rotation outside writer locks. Under its existing transfer stripe, it compares
ID, generation, revision, policy and migration ownership before atomically
publishing only changed groups/allocation metadata and a new revision. A racing
write causes preparation to retry; WAL durability waits after releasing stripes.
The archive point multiset, including every float bit, must remain identical.
Compressed main files, sidecars and healthy classic archives keep their bytes.
Off-grid points or physical points aliasing the same retention slot block the
entire metric for source-aware reconciliation. They are never silently clipped.

The client enumerates the existing catalog or accepts a newline-separated names
file. It writes an fsynced per-name journal and resumes after process or HTTP
failure. Repeating an already successful repair is safe. Catalog enumeration is
temporal; after fixing every producer of malformed archives, repeat a dry-run
until no changed/blocked/error entries remain. Keep all failed names and donor
history until their source-aware reconciliation is independently verified.

Example on a store, with task-owned artifact paths:

```sh
storectl maintain anchors --secret-file /path/to/auth-secret --journal dry-run.jsonl
storectl maintain anchors --secret-file /path/to/auth-secret --names approved-names.txt \
  --journal apply.jsonl --apply --plan approved-plan.md --write-ok WRITE-OK
```

The client never opens a store database. Authorization secrets and JWTs remain
in memory. Deploy/restart and live repair approval are external operational gates;
the client additionally requires a WRITE-OK containing the plan's actual SHA256.
A repair restores visibility of points still present; it cannot recreate points
already overwritten after a bad import. Compare repaired physical points and
renders against retained donors and fixed-importer expectations separately.
