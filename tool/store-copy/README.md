# Store copy

`store-copy` grows the existing `downsize` controller. Both entrypoints call the
same code and use identical plans, manifest hashes, operation IDs and journals.
The resize/merge native baseline is the first consumer; replacement, gap repair
and remaining legacy frontends migrate after its canary.

Build with `GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build ./tool/store-copy`.
The owning donor and receiver daemons must also contain the native engine.
Existing metric commands keep their behavior unless `--engine bulk` is selected.

## Resize / merge baseline

After the campaign owner pauses the existing controller, retains its journal and
approves the concrete binary/plan through the campaign's PLAN-READY/WRITE-OK flow:

```sh
store-copy run --plan /path/to/existing-plan --state /path/to/existing-journal \
  --engine bulk --bulk-only --apply
store-copy status --plan /path/to/existing-plan --state /path/to/existing-journal
```

Do not create another journal or change the operation identity. The existing
journal lock excludes overlapping controllers. Existing completed ranges are
skipped; an incomplete native intent resolves its daemon receipt before replay.
`--max-batches 1` bounds a canary's scope. The default concurrency comes from
receiver CPUs and the existing independent plan streams; `--streams` can specify
concurrency explicitly. Batch bytes/name pacing, transfer slots, tuner, debt,
cache-age and read-p99 guards do not govern native work.

A pinned donor snapshot exports native stored blocks through the existing v2
framing. Queued/recovery source names and revisions changed since the pin are
conservatively deferred. No cache flush, Whisper conversion or expiration gate
runs on this path. The receiver reserves IDs, writes namespace/time-bucket SSTs,
ingests hidden IDs, then atomically publishes only names still absent under the
normal creation lock. Existing/live-created names remain deferred. The atomic
range receipt binds the plan/name digest, copied bitmap, reserved ID mapping,
stream digest and SST bounds/size/entry identities. Ordinary live writes continue.

Durable native receipts, not client responses alone, authorize completion. A lost
acknowledgement is resolved by receipt query without importing history twice.
Unpublished reserved IDs and private SST directories recover on daemon open.
Write/ingest errors, integrity failures, crashes and less than 10% free disk stop
native work. Native pins expire after inactivity and are released on controller
exit. No store lifecycle, routing or configuration action is performed by this CLI.

`--bulk-only` leaves deferred work for a reviewed handoff. Without it, the same
controller switches to the existing metric importer after the native baseline.
Resuming `--engine metric` also handles only unfinished deferred ordinals. An old
controller must not resume a mixed journal: it fails on the empty-proof native
intent; update the controller/status reader first. `completed_names` and
`native_deferred_names` are the native-aware status counters. Counting entire
`verified` ranges as completed is incorrect for mixed journals.

Phase 1 is copy-only. Retirement and a writer-drained final delta reconciliation
still require the campaign's separate existing approval. Overlap combining is
currently the reviewed importer and retains its existing admission policy;
phase 2 will replace that combine stage with exact native unions.

## Proof input and profiles

These commands write private proof artifacts and stores, never a live destination.
Use a root read token in `BUCKYD_TOKEN`; no token is written to capture metadata.
Check free disk before capture, and remove private captures/profiles afterward.

```sh
store-copy capture-native --source http://DONOR:PORT \
  --names planned-sample-names.json --out /private/scratch/native.json
store-copy bulk-eval --native --native-capture /private/scratch/native.json \
  --scratch /private/scratch --batch 100000 --workers 32 \
  --overlap-percent 50 --profile-prefix /private/scratch/native-proof
```

The evaluator seeds its donor through the reviewed importer, compares the native
candidate with that importer, includes native build/ingest/publication in timing,
and compares every archive's retention and point bits. Deferred combines run
outside native timing. `--memory` uses virtual Pebble files and must not be reported
as physical disk/NIC performance. Classic `--fixtures` inputs retain real archive
values but do not prove original production native layout. `--repeat` is reported
as replication and is allowed only for classic fixtures.

`GET /v1/native/stats` reports native wire/SST bytes, build/ingest time and creation
lock wait/hold/max-hold. CPU/mutex/block profiles and Linux device/NIC/CPU counters
must establish what binds before claiming sustained physical throughput.
