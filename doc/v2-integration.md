# Experimental v2 daemon integration

This branch combines the seven core-first read, discovery, lifecycle, TCP,
replay, placement and portable-part Drafts with merge commits. It adds a real
`carbon.App` startup path for **fresh local stores**. It is incomplete and must
remain Draft; it does not qualify an existing-store cutover.

```toml
[v2]
enabled = true
path = "/absolute/path/to/fresh-v2-root"
producers = 2
shards = 2
read-budget-bytes = 268435456
```

The selector defaults off and is restart-only. Existing schema and aggregation
files select immutable per-name policies. Enable TCP and carbonserver, and
disable UDP, pickle, CarbonLink, cache gRPC, custom receivers, buckyd, classic
dump and classic input-log. Both classic data roots must be empty. The mandatory
v2 journal lives below its own root; disabling classic input-log never disables
v2 durability. Geometry persists across restart and cannot change on reopen.

Data flows through the existing bounded receiver queues to serial producers,
producer-local catalogs and one-second journal sync, then the canonical core
owner queues and rollup planner. Each TCP point is an explicit application with
persisted order, policy and clock. This is independent of socket batch cuts;
**equivalence to classic cache/persister UpdateMany grouping is unqualified**.
Do not infer classic rollup parity for arbitrary streams from the small primary
archive integration fixture.

Core hands immutable sealed snapshots and detached pending applications to L4.
Carbonserver render and info use that reader; find uses the existing classic
trie/trigram projection of names obtained from pinned reader publications.
Discovery refreshes once a second, outside writer callbacks. First lookup of a
new sealed generation still scans an entire owner; catalog membership changes
still rebuild the classic image. Neither path is qualified for large churn.

Restart synchronously scans the journal and submits original applications to
normal core queues before opening listeners. Owner frontiers suppress committed
prefixes. Repeated reopen and continued corrections use recovered application
order. The inherited crash matrix exercises actual SIGKILL cuts; startup latency
and retained-log scale remain unqualified.

| Boundary | Current integrated behavior |
| --- | --- |
| TCP / render / find / info | Connected for a fresh unplaced local store |
| L3 | Uses existing canonical rollups; no replacement reducer or per-point rollup I/O |
| Lifecycle / quota | Libraries merged; configured activation explicitly rejected |
| Placement | Direct stamped producer API merged; legacy L2 adaptation fixed; TCP rejects placed input; placed journal reopen rejected |
| Portable parts | Shared format and core adapter merged; no live installer/copy endpoint |
| Classic + v2 union | Nonempty classic roots rejected; no silent fallback |
| Read memory | Render reservation bounded; complete sealed index and pinned SST accounting still missing |
| Shutdown | Stop intake, drain decoded prefix, stop HTTP, close reader pins, close journal/core |

Lifecycle cannot safely be enabled until portable metric incarnations and reset
records fence old archive slots, planner hooks compose lifecycle with canonical
rollups, readers pin masks with data, and host-wide quota usage is updated without
resetting throughput. Current ring partition zero is not a safe aged deletion
unit. A standalone lifecycle database must never be opened on a core root.

Placement activation additionally needs authenticated stamped transport, original
stamp retention in the journal, authority checks during replay and coherent
coverage receipts. Replaying an existing placed journal is explicitly refused
until that contract exists. Whole-part copy/import and legacy overlay also need
the same durable incarnation and authority boundary.

Run Go 1.27 build/vet/tests on `carbon`, `carbonserver`, `internal/v2*` and
`tool/storectl`. `TestV2TCPRenderFindInfoRestart` sends actual TCP input, checks
find/info and HTTP render against a real Whisper file, reopens the same root,
applies a correction, and verifies another reopen. This does not test expiry or
a mixed-format source. Existing component suites retain their stronger codec,
classic reducer and SIGKILL fixtures.
