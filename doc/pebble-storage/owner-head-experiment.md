# Decoded owner-head experiment

This default-off adapter tests how much repeated physical-group read/decode work
can be removed while preserving the current archive writer. It is a partial
experiment: it **still opens virtual Whisper handles and re-encodes changed
groups on every writeout**. It is not a resumable Whisper writer or a new storage
backend, and it does not implement immutable time blocks or correction deltas.

The app creates the adapter for the shared continuation codec. Stable name hashing
assigns one preparation owner. The existing store reservations continue to order
writes/imports; the adapter adds per-owner preparation serialization. Each owner
retains immutable copies of recently used 128-point groups, including value bits
and physical slot order. A write uses a private candidate and the existing exact
codec for all archive decisions, rollups and output encoding.

The store invokes a candidate's publication callback only after the enclosing
single-metric or grouped transaction succeeds, before the persister can confirm
input. A failed transaction discards its candidate. A hit requires matching store
instance, metric ID, generation and revision. Imports, deletion/recreation and
reopen therefore cannot reuse incompatible state. Repairs discard speculative
groups. Readers continue to use committed database snapshots; they never consult
uncommitted candidates. No persisted keys, output formats, input-log boundaries,
Pebble WAL settings or acknowledgement contracts change.

## Configuration

Both options are startup settings; restart to change them. Defaults are zero.

```toml
[whisper]
store-owner-head-owners = 0
store-owner-head-bytes = 0
```

| Owners | Bytes | Behavior |
| --- | --- | --- |
| 0 | any | Existing codec; no owner adapter |
| positive | 0 | Stable preparation owners only; no retention |
| positive | positive | Stable owners and bounded decoded-group retention |

Owner count is capped at 1024. The retained byte allowance is divided equally
between owners; unused space in one owner is not borrowed by another. Entries
are conservatively charged 4096 bytes per group plus key/name and entry overhead.
An owner clears its old entries when a new candidate exceeds its remaining
allowance. Each transaction candidate also keeps at most 64 groups. Normal codec
scratch, temporary encodings and in-flight commit candidates are additional;
existing worker/transaction admission bounds their concurrency. This allowance
is not a total process RSS cap.

## Local validation

```sh
GOTOOLCHAIN=go1.27.0 go test ./internal/whisperio ./internal/chunkstore -run '^(TestOwnerHeads|TestContinuationCommitObserver)' -count=1
GOTOOLCHAIN=go1.27.0 go test -race ./internal/whisperio ./internal/chunkstore -run '^(TestOwnerHeads|TestContinuationCommitObserver)' -count=1
GOTOOLCHAIN=go1.27.0 go test ./internal/whisperio -run '^$' -bench '^BenchmarkOwnerHeads$' -benchtime=200x -benchmem
```

The deterministic differential checks every write boundary, all ordered archive
points and Float64 bits, metadata, raw encoded output keys/bytes and archive
reads. It covers six aggregation methods, both key layouts, three densities
(14/60/480 input points), integer/random floats, NaN payloads, signed zeros,
subnormals, infinities, sparse names, duplicate corrections, late/future input,
owner migration, overlapping import, deletion/recreation and seal/reopen.
A staged-write test checks reads before commit and after both commit and abort.

Subprocess fault tests use the real input journal and Pebble WAL with synchronous
durable acceptance. They exit before/after journal sync, before DB commit,
before/after retained-state publication and before/after acknowledgement, then
recover the DB and replay the journal. This is a process-crash harness; it does
not certify power loss during fsync, asynchronous production retirement,
SST/manifest sync failures or checkpoint retirement.

The benchmark uses the real codec over a transactional in-memory KV shim. It
reports actual group reads, decodes and retained hits per write. Baseline,
owners-only and retained modes use identical generated input. All virtual opens
remain. Allocation counts can reveal mechanism costs; timings on shared machines
cannot establish production CPU savings. The initial implementation increases
allocations, so read/decode avoidance alone is insufficient evidence to enable it.

The fixture is small and synthetic. A frozen representative trace with at least
one million names, larger memory/churn studies and isolated Linux paired replays
remain required. Keep this experimental option disabled pending those results.
