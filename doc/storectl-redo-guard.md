# Observing repeated sync work

`storectl sync` records completed verification requests with the stable catalog
IDs and the exact store, operation, archive and time range. Receipts live in the
existing fsynced journal and survive its verification checkpoint. Changing the
client revision, stream count or verification batch size does not change those
IDs. Sorted ID runs coalesce in memory; the guard keeps no second name catalog.

The guard reports a repeated request for previously clean data when no mutation
proof change was recorded. It emits a JSON event `sync_redo` with `level: error`,
its reason, store, range and count, and preserves the count in `progress.json`.
It observes work and does not cancel a copy, alter verification or change READY
requirements. Explicit proof changes are attached to the new work receipts;
fill receipts record `invalidation_reason: verified_holes`.

The existing Graphite exporter sends cumulative counters:

```text
tools.storectl.<host>.<unit>.sync.verify.redo_names
tools.storectl.<host>.<unit>.sync.fetch.redo_names
tools.storectl.<host>.<unit>.sync.import.redo_names
```

Each counter counts repeated name/range requests across stores, rather than
bytes or distinct names across the whole run. `verify` covers digest requests;
`fetch` covers point reads; `import` covers initial-copy submissions already
covered by successful copy receipts. An authenticated completed-copy checkpoint
also proves that another initial-copy submission repeats the completed phase.
A fill follows a new hole observation and is an explicitly recorded repair.
Counters survive client replacement, include completed READY recovery children,
and appear as `redo_names` in `ready.json`. A positive value warrants inspection
of the matching ERROR events. Zero means no unexpected redo was observed within
the coverage below.

## State intent

Use `--resume STATE` to continue an existing run. A fresh command refuses a state
directory containing prior work, and a missing primary journal alongside work
artifacts refuses to start again. Invalid complete journal rows and missing
journal headers return errors before backend work.

An intentional independent run can preserve all old state and create a child:

```sh
storectl sync --source http://donor:2004 --to http://target:2004 \
  --state ./copy-state --allow-fresh-start='independent scope requested'
```

The `sync_fresh_start` warning records the reason and the new child state path.
Resume that child path on subsequent invocations. This flag never deletes or
truncates the previous run and cannot be combined with `--resume` or a legacy
file journal.

## Current coverage and pending contracts

This is an observational guard, not an exactly-once backend protocol. It tracks
calls committed with verified clean batches and successful copy receipts. It
cannot reconstruct backend calls made before this version first recorded them,
or a successful subcall killed before its batch receipt committed. Unresolved
names do not become clean verification receipts. The operation identity uses
exact ranges; partially overlapping but unequal requests remain distinct.
Final READY policy/parity/read-back calls and native transport's internal retries
are not yet part of these operation receipts. Removing every piece of run state
also removes the evidence required to distinguish a new run from a lost one.

Optional torn mutation-proof records remain pending and are conservatively
revalidated with an ERROR event; they never authorize a skipped read. Corrupt
primary journal state fails closed. Unknown mutation proofs continue to require
verification. The guard never substitutes counters for mutation evidence.

`TestRedoAcceptancePendingContracts` names the owners of unfinished mid-batch,
store-gap, live-write and terminal-READY guarantees. These tests are skipped by
default and contain runnable assertions. To evaluate one contract:

```sh
STORECTL_REDO_CONTRACT=kill_mid_batch go test ./internal/downsize \
  -run 'TestRedoAcceptancePendingContracts/kill_mid_batch$' -count=1
```

Remove each owning test's skip when its protocol is implemented. The ordinary
suite also checks durable phase boundaries, changed client/batch budgets,
backend call counts, lost/corrupt journal refusal, READY counter propagation,
Graphite samples and compact ID runs.
