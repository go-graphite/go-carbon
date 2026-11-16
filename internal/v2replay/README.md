# Core-first restart experiment

`v2daemon.Open` reopens journal layers 2 and 3 under an exclusive process lock.
It verifies persisted layer/producer/shard geometry, reopens each durable name
catalog, and decodes each producer segment in original order. Application
boundaries, orders, admission clocks, policy bytes, point sequences and value
bits come from the existing V2A1/PDC4 envelope. L2 named appends now use that
same envelope; older bare-PDC4 experiments lack sequences and are refused.
Numeric and name-only control layers remain non-restartable.

Replay submits the original batches to `AppendBatchesPipelined`, exactly as live
Drain does after catalog sync and journal fsync. Each core owner filters packets
against its own committed cursor before reduction, so differing owner frontiers
and repeated restart do not reapply an application. The final replay seal is
synchronous: successful Open exposes the complete recovered state to readers
and returns any replay ingest failure before admitting live input. There is no separate replay writer,
point lookup, sender queue or change to the core transaction layout.

Every reopened writer starts a new predictor segment. Recovery always decodes
complete prefixes to rebuild predictor state, even when core skips their packets.
Only an incomplete EOF in the newest segment is truncated and synced. Complete
checksum failures, invalid lengths, cursor gaps/regressions, original-order
regressions and a core frontier beyond retained input fail startup. Duplicate
packets submitted to core are suppressed by its cursor; duplicated journal
records violate the journal's strictly increasing cursor contract and fail closed.
No journal retirement or asynchronous backlog follower is implemented here.
Startup scans retained segments before opening intake, so the 100ms target for
large journals is not qualified. This is an offline daemon API, without a listener.

The crash test uses actual child SIGKILL and a parent-owned expected input:

| Cut | Injection point |
| --- | --- |
| append | Length prefix flushed, payload not written; prior fsynced group remains exact |
| seal | Owner seal entered, before planning/publication |
| sort | First comparison in the ordinary comparator ordering, before sort completes |
| part-write | First buffered SST bytes flushed to the staging file, before SST completion/sync |
| ingest | Pebble TableCreated `ingesting` event, after first file link/copy inside Ingest, before manifest commit |
| commit | Atomic SST/frontier ingest returned, before in-memory planner Commit/frontier publication |
| rollup | First metric reducer plan computed, before the remaining metric plans and transaction publication |

The optional core Stage hook changes sorting/file buffering only when a fault
observer is explicitly supplied. It is unused by normal Open and has no environment
switch. Tests check every raw tuple, every occupied classic Whisper physical slot,
value bits, rollup order, revision and LastUpdate, then reopen again to check core
cursor suppression. Other tests truncate a frame at every byte and exercise lock
exclusion and continued append across predictor epochs. These are process-crash
tests on a laptop; power-loss/filesystem-fault qualification is separate.
