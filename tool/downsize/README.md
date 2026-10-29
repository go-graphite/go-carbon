# Resize a store cluster or replace its hardware

`resize` accepts current and desired members and runs one plan, transfer engine,
resource governor and resume journal. It supports mixed shrink/replacement plans.
All commands below are operator recipes; generating a plan changes local files
only. This work was tested locally, without production access.

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o /tmp/resize ./tool/downsize
/tmp/resize plan --spec members.json --out ./plan
/tmp/resize plan --spec members.json --out ./plan --apply
/tmp/resize run --plan ./plan --state ./resize-state.jsonl
/tmp/resize status --plan ./plan --state ./resize-state.jsonl
```

The first plan command previews. `plan --apply` commits an immutable manifest and
checksummed streams. `run` previews; `run --apply` executes. `status` reads an
existing journal. `config` generates local routing/configuration drafts;
`compare` compares inventories. `snapshot` exposes the lower-level checkpoint
API for diagnostics and loopback benchmarks. Legacy `move`, `copy` and `ring`
binaries delegate to the same Go packages.

## Members and planning

```json
{
  "version": 1,
  "cluster": "metrics",
  "site": "region-a",
  "old": {
    "hash": "jump_fnv1a", "replicas": 1,
    "nodes": [
      {"id": "a", "node": "store-a:2003=000", "endpoint": "http://store-a:4242"},
      {"id": "b", "node": "store-b:2003=001", "endpoint": "http://store-b:4242"},
      {"id": "c", "node": "store-c:2003=002", "endpoint": "http://store-c:4242"}
    ]
  },
  "new": {
    "hash": "jump_fnv1a", "replicas": 1,
    "nodes": [
      {"id": "a", "node": "store-a:2003=000", "endpoint": "http://store-a:4242"},
      {"id": "b", "node": "store-b:2003=001", "endpoint": "http://store-b:4242"}
    ]
  }
}
```

`id` identifies physical hardware; change it for a replacement. `node` determines
hash identity, using the existing buckytools hash implementation. Jump hashes use
explicit instance order (`=000`); changing the hostname while retaining that
instance preserves its identity. Carbon/FNV rings use their own node hash keys;
retain those keys when replacing hardware. Hash names accept the existing
`carbon[_ch]`, `fnv1a[_ch]`, and `jump_fnv1a[_ch]` forms; `virtual_nodes` defaults
to 100. Keep the actual relay's hash settings and order.

The planner automatically reads bounded `/v1/transfer/list` pages from old
members. Set `BUCKYD_TOKEN` for authenticated inventories. Alternatively provide
`inputs` with `source`, `path`, `format` (`lines`, `tsv`, `jsonl`) and optional
`estimated_bytes`; file paths are relative to the specification. Legacy HTTP
`buckyd` inventories remain readable. Inventories must be sorted, complete and
reconciled with the writer cutover. An unchanged identity set on new hardware
needs no metric inventory: its whole-store checkpoints provide complete coverage.

A new physical member retaining an old hash identity with unchanged hash settings
gets a whole-store job. Other ownership changes get metric jobs. A checkpoint
covers its old-source-to-replacement route; that redundant metric route is
omitted. Other incoming/outgoing routes remain. The summary describes ownership
changes from inventories; `streams` describes the remaining metric transfers and
`whole` the checkpoint jobs. Whole jobs finish before dependent metric jobs start.

## Shared host controls

Every participating live host runs `move/host.py serve` with the application's
PID file, physical NVMe device(s), NIC, store directory, admin URL and Prometheus
URL. The helper reads local Linux telemetry. Expose it only on the management
network. Set the same `MOVE_CONTROL_TOKEN` in helpers, workers and controller;
set `BUCKYD_TOKEN` in controller and replacement workers. Credentials stay out of
plans/journals. Buckyd's whole-store admin ACL protects checkpoints; transfer ACLs
protect raw exports/imports. Redirects are rejected.

```sh
python3 tool/downsize/move/host.py serve --bind 127.0.0.1:9079 \
  --pid-file /run/go-carbon/go-carbon.pid --devices nvme0n1 --nic eth0 \
  --cluster metrics --site region-a --store-dir /data/store \
  --admin-url http://127.0.0.1:8080 --metrics-url http://127.0.0.1:7007/metrics
```

Use reachable management addresses when the controller runs elsewhere. Supply
`--health-map health.json`: a JSON map from each Buckyd origin to its helper
origin. Replacement targets instead use their fixed worker origin from
`placements`; the worker proxies the same helper endpoints.

Defaults allow 1,000 MB/s ingress **and** egress per host, 2,000 MB/s disk writes,
and up to 64 transfers. The governor subtracts observed foreground CPU and a core
reserve (8 on a 96-core host), respects runtime CPU/server/body/memory limits, and
ramps from one stream. `--streams`, `--max-mbps`, `--batch`, `--batch-bytes`,
`--delete-rate` and `--read-p99-ms` lower operator ceilings. Buckyd
`max-transfers = 0` chooses the host-derived ceiling. An explicit small
`common.max-cpu` still limits throughput; review it alongside ingest/read capacity.

CPU, NIC directions, disk bandwidth/utilization/await, L0/debt/stalls, free space,
RSS/threads, ingest backlog, write failures, unlogged points and read p99 govern
the ramp. Lost/stale telemetry or durability guards abort. Each `tune` JSON log
reports `binding_resource`, sampled values, foreground CPU and the active ceiling.
Streams pause when pressure persists. Worker admission leases expire when the
controller stops; already admitted chunks are bounded and cancel on lease expiry.

For extra headroom, `--block-reads --replica-url http://other-site:8080
--replica-site region-b` renews read-isolation leases after probing affected data on a
replica in another site. Helpers must run with `--apply` to insert rules. IPv4 and
IPv6 reject rules have independent systemd expiry timers, and are removed at
normal exit. Read capacity remains protected by the replica. Without isolation,
keep union(old,new) read coverage throughout transfers. Read isolation is optional;
never infer complete reads from successful file copying.

## Recipe: shrink N to M

1. Set `old` to all current members and `new` to survivors; review the plan.
   Verify target capacity, policy compatibility and carbonapi coverage. Keep old
   read backends online. A copy-only canary uses `run --apply` with a separate
   journal and leaves all source data present.
2. Switch **all** relay writers to the reviewed desired ring. Drain old relay
   queues, connections, receiver/cache/persister work and input-log replay. Stop
   internal metric emission on a retiring host. Reconcile newly created names and
   ownership; regenerate an incomplete plan before starting deletion.
3. Record the observed cutover in `barrier.json` (expiry within 24 hours):

   ```json
   {
     "plan_id": "<manifest plan_id>", "expires_at": 1793300000,
     "relay_switched": true, "old_writers_drained": true,
     "reconciled": true, "sources": ["c"]
   }
   ```

4. Preview and run, keeping the plan, journal path and flags for every resume:

   ```sh
   /tmp/resize run --plan ./plan --state ./resize-state.jsonl \
     --health-map health.json --delete --barrier barrier.json
   /tmp/resize run --plan ./plan --state ./resize-state.jsonl \
     --health-map health.json --delete --barrier barrier.json --apply
   ```

   Default cleanup stages conditional versions and retires an entirely removed
   source only after final delta refresh and proof coverage. Retained sources use
   conditional batch deletion. A pending or new unplanned name prevents retirement.
   `allow_unowned` and replicated layouts are copy-only; replicated deletion must
   wait for separately verified complete destination coverage.
5. Verify target reads/history, absence of unlogged/write errors and source
   emptiness/residual ownership before removing read backends or reclaiming hosts.

## Recipe: replace a host with a smaller server

Use this recipe for an emptied, reprovisioned host restored from its equivalent
replica as well. The whole-store job streams immutable checkpoint files, including
SSTs, without decoding metrics. The empty target opens the copied Pebble manifest
and SSTs directly, then replays the input-log gap. The metric-move throughput
figures do not describe this path. See the [whole-store measurements](copy/BENCHMARKS.md)
for local results and a foreground-load benchmark.

Keep node count/hash identity unchanged; replace physical `a` by `a-small` in
`new`. For jump, use `new-host:2003=000`. Add:

```json
"placements": {
  "a-small": {
    "worker": "http://new-host:9080",
    "directory": "/data/resize-stage",
    "preflight": "/data/resize-preflight.json"
  }
}
```

The target starts empty and stopped. Its configuration must point `store-dir` and
input-log `path` into the stage, use captured `stage/config/storage-*.conf` policy
files, and use the desired Buckyd node/ring. Encoding/conversion mode must match
source; a converted store uses `stage/empty-originals` for its original directory.
Input logging is required on source and target. Whole copy requires completed
shared conversion with tags, expiration, live migration and rollback mirroring
disabled; readiness rejects unsupported modes. Size the target caches, index,
memtables and point buffers for its RAM and retain disk/log replay reserves.

```sh
python3 tool/downsize/copy/control.py preflight \
  --destination /data/resize-stage --config /etc/go-carbon-small.conf \
  --store-bytes 1850000000000 --log-reserve-bytes 100000000000 \
  --output /data/resize-preflight.json --apply
/tmp/resize worker --target a-small --directory /data/resize-stage \
  --preflight /data/resize-preflight.json --helper http://127.0.0.1:9079 \
  --pid-file /run/resize-worker.pid --bind 0.0.0.0:9080
```

While the target is stopped, run its host helper with `--offline`, the worker PID
file and the existing data-drive parent as `--store-dir`. This samples real host
resources without inventing live application metrics. Restart that helper without
`--offline`, using the target application's PID, before asserting readiness.

1. Generate/review the plan, then `run --apply`. The source keeps ingesting during
   checkpoint and baseline copy. The controller records a checkpoint and stops at
   `needs_input_fence`; this is a resumable operator gate, with a nonzero exit.
2. Pause/redirect old-host relay input and drain old connections/queues. Record
   `input_fenced: true`, the source physical ID in `sources`, the actual `plan_id`
   and a fresh `expires_at` in the barrier. Resume the **same** run with
   `--barrier barrier.json`. Source seal stops receivers, captures the retained
   input-log gap, then the worker copies that gap, verifies staged bytes and
   prepares startup/replay guards. It stops at `needs_target_replay`.
3. Start the target using its reviewed configuration. Complete captured input-log
   replay, rebuild/verify the catalog index, test render/find/history completeness
   and restart live telemetry. Keep old reads until the target serves complete
   data. Switch routing to the desired member and drain any old generation.
4. Add `"targets_ready": {"a-small": true}` to the barrier and resume. The engine
   also requires the durable replay marker to have cleared and a live target
   transfer API/telemetry. It releases the source checkpoint after this gate,
   retaining source data. Resume input and verify reads/errors/lag before manual
   removal of the old hardware. Source removal is never implied by copy completion.

For **shrink plus replacement**, combine the member edits in one specification.
Start with `--delete` if source cleanup is intended, and keep that flag throughout
resume. At the final readiness gate include the ordinary shrink barrier fields,
all range sources, and reconciled inventories after the writer switch. Whole jobs
prepare first; the same engine then performs incoming/outgoing metric moves.

## Data correctness and resume

Metric-range exports use the live Pebble block cache, epoch-valid metadata/state/tails and
mapped catalog pages. Hot pending points are flushed through the existing
persister/input-log confirmation path before snapshotting. Ordered catalog/ID
cursors read stored exact/compressed key/value bytes into bounded arenas; binary
wire v2 avoids JSON descriptors. V1 readers/exports remain compatible. No additional
wire compression is enabled for the measured CPU-bound sparse case.

Disjoint records become prepared SSTs with reserved target IDs. Full independent
SST read-back compares every key/value before Pebble Ingest and grouped durable
catalog/receipt publication. Independent source/target sampled point checks remain.
Overlaps use the verified target-wins merge: target values win equal timestamps,
source fills holes; policies and archival values are checked. Committed target
cache state is captured under writer locks before invalidation, with a database
fallback when it is unavailable. Receipt encoding reuses scratch within each batch
while preserving the existing JSON bytes and receipt hashes. Fresh source versions
and target receipts are required before cleanup. Crash/damaged/repeated requests
cannot turn a saved journal entry into deletion authority without re-verification.

Whole-store jobs retain the checkpoint's original IDs and stored file bytes.
They read file ranges through the operating system's page cache, without a
per-entry Pebble cache lookup, point codec or per-metric WAL write. Each source
stream and active target worker reuse a 1 MiB buffer; target writes coalesce
fragmented HTTP reads. Every range still checks its length and SHA-256 trailer,
and completion waits for file syncs. Input fencing, retained log replay and target
readiness remain required after the baseline copy.

Resume with the identical immutable plan, flags and state path. Journals and
workers are locked. Lost checkpoint-begin responses reuse an operation ID; chunks
are verified before reuse, or reused only with synced checksums and unchanged
inode/size/mtime/ctime. Stale lock pathnames do not block a restarted pull; live
kernel locks do. Prepared or running target stores cannot be overwritten.

Whole checkpoints expire after six hours and have source disk/log guards. A source
process restart loses the active checkpoint session: the controller fails closed;
retain old data and establish a new baseline/session before cutover. Do not delete
an incomplete staging directory or edit journal proofs to force resume. Worker
restart and controller response-loss resume are tested. There is no automatic
relay deployment, target service start, routing change or host decommission.

See [measured benchmarks](move/BENCHMARKS.md) for local limitations and the remaining
sparse CPU gap. Hardware bandwidth figures are ceilings, not guaranteed resize rates.

Batch proof responses negotiate a compact binary encoding. Verification falls
back to JSON if a peer does not advertise support or is downgraded after
negotiation. The proof contents and durable verification rules are unchanged.

Generated `transfer-sources` lists include old-ring node names and their explicit
HTTP endpoint hostnames, including management aliases. Apply these drafts before
metric pulls so the source token is sent only to configured old-ring origins.
