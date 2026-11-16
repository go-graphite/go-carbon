# Durable part publication

This owner-side publisher composes the existing `partsread` file source with the
writer's atomic SST transaction. It is not an enabled daemon backend. Its first
composition is `TestCoreDurablePartPublisher`: the actual core's `PlanSeal` adds
the envelope SST and `Commit` publishes only after raw data, envelope and journal
frontier have committed together. The test exercises SST ingest and sync batches;
it does not qualify a numeric core benchmark as a portable daemon writer.

```text
frozen authoritative archive replacements
  -> stage complete fixed24 V1 part; fsync data/manifest/directory
  -> rename unique part; fsync parent
  -> open + validate full portable keys, scope, policy, extents and CRCs
  -> atomic writer ingest: raw/archive SSTs + envelope + owner frontier
  -> Plan.Commit: publish one coherent immutable View
  -> collect retired parts after committed refs, read views and copy pins drain
```

`Envelope` persists a version, generation, complete opaque catalog checkpoint,
sealed frontier vector, and all active `Ref`s. Every ref persists its part name,
generation, manifest version, explicit `fixed24-le-v1` codec and exact
`partsformat.PartitionPrefixForGroup` scope. V1 data is still the reader's fixed
24-byte time/IEEE-bits/sequence format. Future compact codecs must have an explicit
new discriminator and checked decoder; unknown codecs fail before publication.
Each part contains one nonzero globally unique GCP3 part UUID within one
placement-group/vshard/partition. The publisher uses the existing GCP3 codec and
never manufactures identity from a local numeric ID.

The caller supplies `DecodeCatalog`. It must rebuild an immutable catalog and
Graphite index from the persisted checkpoint and resolve complete keys against
that catalog's exact canonical names, persisted group assignments and policies.
Local numeric read IDs can change after restart. The publisher additionally
checks full GCP3 keys, exact scope, consistent part UUID, catalog/read-ID agreement,
and archive dimensions before admitting any new descriptor set. Sources surviving
across generations share one refcount. A changed catalog opens a new descriptor
set so old readers retain their own ID mapping. Retirement checks all descriptor
sets for the physical part, so a pin on an older mapping still prevents deletion.

Call `PlanSeal` before ingest. Add `Plan.Value` at a reserved owner-local metadata
key in the SAME atomic durable ingest as the raw/archive SSTs and owner frontier.
Call `Plan.Commit` only after success; it cannot fail. On a known failed ingest,
`Plan.Abort` releases staged resources and requires restart. On ambiguous outcomes,
stop intake and recover from storage. The current core has no abort callback, so
its adapter must close the publisher on every engine error. Staged/complete orphan
directories are retained for later reconciliation, never adopted on restart.
`Recover` consumes only the latest committed envelope value. Missing, truncated,
corrupt, foreign-scope or unsupported-codec referenced parts fail recovery; they
are never silently skipped. The caller must replay after the committed frontier
through the same intake/seal path, and trim only below both the input log's fsynced
frontier and the conservative committed owner frontier. This package never trims
an input log.

`Pin` goes directly to the atomic publication and never takes the writer mutex.
Views contain detached catalog and frontier data and the complete sealed part set.
`AcquireView` transfers another coherent view to an existing backend publication
without reopening files. It retains the same shared part references; each
recipient publication owns its returned Close callback. Call it during owner
publication, while ordinary queries continue to use their existing lock-free Pin.
Unsealed-head publication remains the backend's responsibility: an unsealed head
must be frozen and cannot advance this durable frontier. Whole-part copies or any
external database snapshot retaining an envelope must also hold `PinEnvelope`.
Its `Snapshot.Value` and file pins form a coherent set; release with `Close` after
the copy/snapshot drains. Retiring without such a pin is unsupported.

`Collect` only deletes parts whose persistent references were removed by a
successful commit in this process and whose read/copy pins have drained. It
synchronizes the root after deletion. It deliberately does not enumerate/delete
recovery orphans: orphan reconciliation needs the backend's full persistent
snapshot inventory and ownership/fencing rules. The caller must provide a durable
parent/root, serialize all other access to these part names, and never reuse a
name. Replicated incarnation/tombstone and legacy-vs-live authority remain backend
contracts; a GCP3 key alone does not grant them.

Ordinary Go tests cover portable-ID remap, orphan rejection, shared descriptors,
concurrent readers, copy pins, corruption and invalid references. The optional
`sealrollup_integration` suite uses the sibling exact reducer to construct the
same 70-schema × 6-method × 4-range oracle as the reader. Set
`PARTS_PUBLISH_CRASHES=200` to SIGKILL subprocesses at staged, synced, committed,
published and retiring cuts; every restart checks all 1,680 queries, the active
reference count/uniqueness and atomic raw/frontier generation. The stage cut is
after complete content fsync but before final rename; the sync cut is after rename
and root fsync. These are process-crash tests, not power-loss/faulty-device tests.

### Engine isolation and publication cost experiments

`TestPublisherWriterIsolation` in `tool/immutable-parts` composes this publisher
with the actual core `PlanSeal`/`Commit` transaction, extending the durable adapter
correctness test. Every arm writes fixed24 immutable parts and atomically ingests
their envelope with the raw data/frontier. Readers pin committed Views and call
`partsread.Render`. The synthetic workload has 1,024 portable names across 16
routing scopes, one scope per owner, and one `1s:1d` archive. This is an engine
composition experiment; it does not qualify the production daemon's policy,
journal, rollup, retention, admission or mixed-history adapter.

Opt in with `PARTS_PUBLISH_BENCH_DIR` pointing to a nonexistent private directory
and `PARTS_PUBLISH_READERS` set to `0`, `8`, `32` or `64`. Each process offers
input for 20 seconds, waits for all producer barriers, reports durable throughput
including that final barrier, then settles for three seconds. Seed transactions
precede timing. The test retains every generated part through process shutdown.
The default does not impose an offered-rate ceiling. On Linux, launch under a
fixed writer `taskset` mask and set `PARTS_READ_BENCH_CPUS` to a disjoint reader
mask. Keep `GOMAXPROCS`, masks, memory limit and profiles identical across arms.

Preregister the random seed, order, equal durations and analysis before running.
Use at least five independently randomized repeats of every loaded count on
each topology, with zero-reader baselines between loaded arms. A fresh process
and directory per arm gives each arm the same initial state. Keep every repeat.
For each loaded arm, pair its rate with the mean of its immediately bracketing
zero-reader rates on the same topology. Report the mean loss with a paired
bootstrap confidence interval, resampling complete randomized rounds to retain
shared-baseline dependence. The at-most-2% gate passes only when the confidence
interval's upper bound is at most 2%; an interval crossing 2% is inconclusive.
Also report writer rate intervals, baseline variation, query throughput and
publication refresh time. Never qualify from a static source or a memory-only
publication. Publication nanoseconds sum owner work, rather than measuring
elapsed wall time.

Each reader has an independent cancellation context with a background parent;
sharing a cancellation context introduces a reader-only contention point. Save
an allocation profile with `-test.memprofile` alongside the other profiles. The
receipt reports writer-interval GC cycle and CPU-assist deltas, `GOGC` and
`GOMEMLIMIT`. `gc_assist_pct_capacity` divides process-wide assist CPU seconds
by writer elapsed seconds times `GOMAXPROCS`; it includes assists by readers
and does not attribute that time to writers. Compare runtime settings in
separate exploratory arms, keeping the registered curve unchanged.

`PARTS_PUBLISH_BATCHES_PER_PRODUCER` selects fixed input instead of a timed
offering interval. Each producer submits that many 8,192-point batches; the
receipt checks the total committed count excluding seed points. For example,
`2560` submits exactly 83,886,080 points over the four producers. This holds
input and history growth constant across loaded and control arms; throughput
still includes the final durable barriers. Register this choice before the
first arm. It does not establish settled compaction or eliminate daemon noise.

`PARTS_PUBLISH_COST_DIR=<nonexistent-directory> go test -v
./internal/partspublish -run '^TestPublicationCosts$' -count=1` measures 16 seals
of 4,096 points and compares a catalog-stable publication with a catalog-byte
change that reopens and verifies all 16 active parts (1,572,864 payload bytes).
It prints publication ns/point and descriptor reopen milliseconds. Run the
compiled test under `strace -f -e trace=fsync,fdatasync` for measured syscall
counts. The receipt's source-derived count of five fsyncs per added part covers
data, manifest, staging directory, intermediate parent and publisher root; it
excludes the core's atomic-ingest synchronization. Do not substitute that source
count for a measured syscall result.
Where tracing is unavailable, a separate untimed build can instrument actual
`os.File.Sync` calls in both the block writer and publisher. Preserve the exact
instrumentation diff and original source hashes, report failed calls separately,
and keep its timings separate from the uninstrumented cost measurement.

### Reader process isolation experiment

`TestPublisherWriterIsolation` accepts `PARTS_PUBLISH_PROCESS=1` to launch the
same render workload in a child process with its own Go runtime, publication and
block caches. `PARTS_PUBLISH_READER_PROCS` bounds that runtime (default 64), while
`PARTS_PUBLISH_READERS` still sets concurrent query loops. On Linux the child
inherits the disjoint reader CPU mask from `PARTS_READ_BENCH_CPUS`; writer CPU
placement and runtime settings are independent.

On Linux, `PARTS_PUBLISH_READER_NODE` optionally launches the child through
`numactl --membind=<node>`; use an explicit writer memory policy as well when
comparing sockets. Record actual NUMA placement, CPU frequency, daemon load and
disk/compaction activity. Affinity alone does not fix page placement or eliminate
shared hardware contention. `PARTS_PUBLISH_SETTLE_MS` overrides the default
three-second settle period equally across a registered curve.

For an external PMU coordinator, `PARTS_PUBLISH_EXTERNAL_COUNTERS=1` adds two
bounded handshakes. After `writer-ready` appears (containing the parent PID),
attach writer counters and create `writer-go`. After `writer-done` appears,
stop counters and create `writer-counters-stopped`. Both waits time out after
30 seconds. The writer throughput clock excludes both waits. Polling markers
still gives the PMU a small boundary uncertainty: record it explicitly. Attach
after child startup so the separate reader runtime is excluded; verify the
counter tool's thread inheritance behavior. These markers only coordinate the
opt-in experiment and never change daemon operation.

The first real consumer is this opt-in core/publication benchmark: each successful
core commit atomically renames its committed envelope into a per-owner handoff
file. The child polls these files and incrementally installs references through
`PlanSeal` with no added blocks. Writer baselines in process mode perform the same
handoff with zero query loops. The fixture is append-only and retains every part
until the child joins; the child must acknowledge every final generation. This is
an experiment transport, not a production cross-process retirement or recovery
protocol. Neither daemon operation nor the on-disk publication format changes.

CPU profiles label in-process readers and writers separately. Results additionally
report allocation bytes, GC assists, dedicated marking and pause CPU over the
writer interval; process-wide GC values cannot assign assists to a particular
writer goroutine. Compare complete within-topology curves and retain all repeats.
The child reports query-loop wall time, total/max refresh wall time and final
catch-up time separately. Those times include scheduling and I/O waits; query
counts span the child's lifetime, not just the writer interval. Final-generation
acknowledgement is a correctness check and does not establish a freshness bound
during load.
