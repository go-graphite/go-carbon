# Resumable owner-head experiment

This default-off adapter retains a compressed writer's parsed archive/header and
encoder state, decoded physical groups, and reusable input buffers between
writeouts. A matching retained writer skips the virtual open and header parse;
matching groups skip backing reads and decodes. The existing exact archive
engine still decides rollups, late points and output encoding. Persisted formats,
input-log boundaries, Pebble WAL settings and acknowledgement policy are unchanged.

The app creates the adapter for its shared continuation codec. Stable metric-name
hashing selects one preparation owner; existing store reservations still order
writes and imports. This is preparation ownership, not an end-to-end ingest owner.

## Transaction boundary

The local continuation layer provides **exclusive checkout**, rather than a clone
of the dependency's private Whisper fields. Before advancing a retained capsule,
the owner removes its sole committed reference and its retained charge. The
capsule belongs only to that transaction until the whole database commit succeeds.
Only the store's post-commit callback can publish it again. Any failure, including
an abort after successful preparation, drops the capsule; the next update recovers
from the committed database. No mutable committed copy survives checkout, so
failed attempts cannot leak private encoder state into a later transaction.

A retained capsule contains only a virtual file and Go state, with no OS handle.
Its virtual file can resume against a new indexed batch after close. Transaction
IO wrappers, probes and context timers are detached before retention; the clock
is rebound for each update. Readers always use committed DB snapshots. A hit
requires the same open-store scope, metric ID, generation and revision. Imports,
expiry/deletion, recreation and reopen therefore invalidate stale entries before
reuse. Ownership migration discards the old pool and starts cold.

Only a compressed main file with no remaining sidecar and a single unchanged
virtual file is resumable. Classic sources, sidecars and rewritten file identities
retain the ordinary open path. Repair retries discard all speculative state.
A supplied maintenance policy does not prevent retention when no sidecar remains.
This restriction avoids a dependency fork and keeps the old engine as the oracle;
it can be removed when that engine exposes a suitable general resume API.

Decoded groups transfer into the private capsule without whole-group copies in
the adapter. Retained input value/pointer buffers are reused after warmup. The
ordinary codec can still allocate scratch and encode modified groups; this is
not an immutable-block backend or a general independently mutable clone API.

## Configuration and memory

Both options are startup settings; restart to change them. Defaults are zero.

```toml
[whisper]
store-owner-head-owners = 0
store-owner-head-bytes = 0
```

| Owners | Bytes | Behavior |
| --- | --- | --- |
| 0 | any | Existing codec |
| positive | 0 | Preparation ownership/control; ordinary codec materialization |
| positive | positive | Ownership plus bounded resumable writers and groups |

Owner count is capped at 1024. The retained allowance is divided equally between
owners. Each group is conservatively charged 4096 bytes plus its key. Writer
state is charged 16 KiB plus four times its header size, reusable input-buffer
capacity and name/entry overhead. A candidate holds at most 64 groups. Publication
evicts least recently used groups first to fit its writer; an oversized writer falls back to groups,
and owner pressure clears old entries. Eviction always returns to the normal path.

The allowance covers retained state, including conservative bookkeeping charges.
Normal codec scratch, temporary encodings and checked-out/in-flight transactions
are additional under the existing worker/transaction admission. It is not a total
RSS cap; measured retained-memory/churn qualification remains required.

## Local validation

```sh
GOTOOLCHAIN=go1.27.0 go build ./carbon ./internal/whisperio ./internal/chunkstore
GOTOOLCHAIN=go1.27.0 go vet ./carbon ./internal/whisperio ./internal/chunkstore
GOTOOLCHAIN=go1.27.0 go test ./carbon ./internal/whisperio ./internal/chunkstore -count=1
GOTOOLCHAIN=go1.27.0 go test -race ./internal/whisperio -count=1
GOTOOLCHAIN=go1.27.0 go test ./internal/whisperio -run '^$' -bench '^BenchmarkOwnerHeads$' -benchtime=200x -benchmem
```

The deterministic differential checks every write boundary, all ordered archive
points and Float64 bits, retention/metadata, raw encoded keys/bytes and archive
reads. It covers six aggregation methods, both key layouts, 14/60/480 input
points, integer/random floats, NaN payloads, signed zeros, subnormals, infinities,
sparse names, corrections, late/future input, expiry, owner migration, overlapping
import, deletion/recreation and compact/reopen. Additional cases cover imported
classic/compressed layouts, warm resume with a maintenance policy, budget eviction,
failed preparation and a dropped transaction after preparation. Staged-write tests
check reads during both commit and abort.

Seven real process-crash boundaries around journal sync, DB commit, publication
and acknowledgement exercise a warmed writer, then compare DB recovery and journal
replay across all archives and encoded output. This is synchronous acceptance and
process-crash coverage. It does not certify power loss during fsync, asynchronous
production retirement, SST/manifest sync failures or checkpoint retirement.

The microbenchmark uses the real codec over a transactional in-memory KV shim.
It reports actual virtual writer opens, group reads, decodes and allocations per
write. Cold opens and bounded-cache misses remain in the reported averages. The
baseline/control/retained arms receive identical generated input. Shared-machine
timings cannot establish production CPU savings. Keep this PR draft and disabled
until representative million-name correctness, memory/churn and isolated Linux
paired replay gates are complete.
