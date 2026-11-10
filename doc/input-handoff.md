# Shared-store input handoff

A shared Pebble store without mirrored files needs **both** dump settings below.
`handoff-supervisor` alone does not enable shared-store input handoff.

```toml
[dump]
enabled = true
format = "compact"
path = "/var/lib/graphite/dump"
restore-per-second = 0
handoff-supervisor = true
store-input-handoff = true

[tcp]
enabled = true
listen = ":2003"
buffer-size = 0
compression = ""

[input-log]
enabled = true
```

Use only the built-in TCP receiver, with other input receivers disabled, tags
disabled, carbonserver enabled, and `whisper.storage-backend = "pebble-chunk"`
without mirrored files. Automatic protocol detection supports plaintext,
independent zstd frames, and stream input. Explicit connection compression and
buffered/custom receivers use ordinary stop/rebind.

The service manager must let the worker survive its supervisor's exit. For
systemd, use `KillMode=process` and `KillSignal=USR2`, with enough stop time for
the dump. The host also needs room for the successor while the old worker exits.
Because that lifecycle is not safe under service managers that kill the whole
process group, `store-input-handoff` defaults to `false`.

The stopping worker must have loaded `store-input-handoff = true` before USR2.
When adding it to an already supervised service, reload the active configuration
with SIGHUP and verify `config successfully reloaded` before the planned restart.
Editing only the next instance's configuration leaves the first stop on the old
setting. Enabling the supervisor itself requires starting under that supervisor.

When `require-startup-capture` is omitted, `store-input-handoff = true` also
enables strict restart admission. Before stopping any component, the incumbent
checks its recovery state and the actual successor's configuration, capacity
probe and `-startup-generation-protocol` capability. It then synchronizes a
small `.startup-input-generation.json` record while intake and persistence
remain active. Failure to prepare that record refuses the stop and keeps the
incumbent running. An explicit `require-startup-capture = false` selects the
legacy fallback and cannot provide the same planned-restart guarantee.

The generation record supplies only cache/WAL names for early intake routing.
It is not a point source or a read checkpoint. Once the actual cache dump and
WAL are durable, an unavailable optional read index or failed checkpoint
publication no longer closes input: the successor captures into its ordinary
sharded spool before restoring the authoritative dump/WAL in order. Malformed
or ambiguous generation names are refused; other generations still fail the
single-generation check. Keep the record with its recovery files when moving
or restoring a dump directory. Actual dump/WAL I/O errors, insufficient resource
headroom and an unsuccessful socket transfer remain separate failure cases;
admission is not a throughput certificate.

On USR2, the worker keeps receiving while it checkpoints the cache and finishes
store flushes. It then diverts input to a tail log, closes reads and the shared
database, and releases its supervisor. The successor takes the listening and
accepted sockets immediately, before opening storage or validating the pending
checkpoint. Before requesting sockets it creates and syncs its independent
capture shards while the predecessor still reads. With a successor advertising
`go-carbon-async-tail-seal/1`, the predecessor flushes all accepted tail bytes
before transferring sockets, then completes final file and directory syncs
while the successor captures. The predecessor joins that seal before exiting.
Historical replay waits for the old tail's exclusive lock and samples its final
size after acquiring it; live capture never waits for that lock. Strict restart
admission probes the actual successor's `-async-tail-seal-protocol` before any
stop mutation. Older non-strict handoffs retain their synchronous seal.

Its handlers restore decoder state and encode complete legacy
records into connection-local batches, partitioned by metric hash. Each partition
has one append owner and an independent FIFO. Connection-local acknowledgements
are reused across flushes. Appenders coalesce submissions, write independently
checksummed zstd frames, and publish complete-frame byte frontiers atomically.
Replay prefetches published prefixes with independent `ReadAt` windows and decoders; no
producer mutex protects a reader or any file syscall. Directory entries are
synced before accepting data; active files are synced once per second, on rotation,
and before handing descriptors over. Initial directory creation is synced as one
batch before attaching receivers. Independent final syncs run concurrently.
The `startup_capture_operation_{seconds_total,calls_total,max_seconds}` metrics
separate encode, write, sync and rotation latency without taking lifecycle locks.

Storage and checkpoint validation run while receivers capture. `Start` attaches
background replay and returns without detaching receivers. A partition reader
replays to its applied frontier and submits a promotion through that partition's
FIFO. The append owner verifies the frontier and begins applying new frames
directly. Every point remains durably queued first. A denied cache reservation
returns that partition to capture, while its reader waits; sockets keep draining.
Same-metric records never bypass the partition, including across promotion,
capacity pressure, and subsequent epochs with a different partition count.

With a validated checkpoint, ordering is saved history, predecessor raw tail,
predecessor continuous journal, older capture epochs, and current capture. Older
epoch partitions replay concurrently, with a join between epochs and FIFO segment
order within each partition. An ambiguous extra generation uses ordinary ordered
recovery. Captured input continues while a single-generation fallback restores
old cache/WAL history. Tags, other receiver protocols and mirrored-store paths
retain their existing eligibility rules.

Capacity uses the shared `internal/hostcapacity` sampler: Linux MemAvailable,
cgroup remaining bytes, Go memory-limit remaining bytes and a physical reserve.
The shared cache admission implementation reserves up to one quarter of current
available memory per sample, prices observed bytes per point and distinct metric
headers, and publishes denied admission as automatic writeout pressure. Explicit
configured point/byte ceilings remain authoritative. Capture owns no second cache
budget. Shards/readers are the next power of two at least GOMAXPROCS, reduced
for memory and disk safety. Initial decompression and coalescing arenas scale with
available memory, within 64 KiB–4 MiB and 64 KiB–2 MiB bounds respectively.

The spool budget is one quarter of (free disk + retained source bytes), resampled
each second. Segment size scales with that budget. Catch-up duration is a measured
ETA from backlog divided by replay-minus-capture rate, without an absolute deadline.
A nonpositive margin logs ERROR and sets `startup_capture_continued=1`; intake
continues durably. Only actual disk safety pressure or an I/O error may stop it.
A disk safety stop logs ERROR, sets the fallback/paused metrics and reports expected
pause duration (-1 when no defensible estimate exists). Stop can release its own
disk quota fence instead of deadlocking behind an already-paused appender.

Cache batch identities retain the earliest source reference in each stream,
through pop, requeue, merging and confirmation. The finite checkpoint tracks its
own claimed history, rather than every future captured point. Independently
spooled batches do not enroll in the continuous journal or its relogging; batches
replayed from that journal retain its generation tracking. Mixed/requeued batches
preserve both owners. Older checkpoint
and journal sources gate current partition retirement. Files retire only as a
confirmed contiguous prefix, with a directory sync between removals. This prevents
an older correction reappearing after a newer source was removed. Shutdown during
capture preserves the existing sources instead of publishing a newer checkpoint
before an unreplayed suffix; input continues through store teardown and socket
handover.

Epoch files have names ending in `.capture-epoch-<epoch>.<partition>.<sequence>.tail.bin`
and contain independent GCS2 frames. Ordinary recovery recognizes this format and
recovers complete frames before a torn final write. **Recovery/rollback must use a
binary containing the GCS2 reader while these files remain; e3c4fa4d cannot read
them.** The socket handoff protocol itself remains version 2. Process-crash recovery
protects completed appends; unread kernel/decoder buffers retain TCP's usual
limitations. TCP ACK is not an application durability acknowledgement. Power-loss
exposure is the periodic sync window and any outstanding filesystem sync.

`input capture active before storage and checkpoint` marks intake readiness.
`input tail loaded` reports each finite predecessor/older source. Capture totals,
raw bytes, retained bytes, live partitions, admission pressure, spool budget,
convergence ETA and measured capture/replay rates remain available as metrics.
`/metrics` skips busy lifecycle/control snapshots and increments the single
`migration_metrics_skipped_total` counter from mutex-watch; `/debug/pprof` remains
independent. There is no final capture-to-live socket pause.

Every inherited socket uses RAM read-ahead while its protocol prefix is replayed,
including successors writing to startup capture. Capture handlers drain and
release this buffer at their first read beyond the inherited prefix. Ordinary
non-capture recovery also buffers newly accepted connections until ordered
input-tail replay finishes. Read-ahead retains at most 8 MiB per connection plus
a 64 KiB read block. A full buffer applies TCP backpressure; it never drops bytes. Memory grows with
actual queued bytes, independently of packet sizes. At 280 connections the
maximum ring allocation is 2.19 GiB. Once the tail is applied, each handler drains
its buffered bytes, frees the ring and returns to direct socket reads. A slow
recovery or input beyond this byte budget can still backpressure senders.

For busy connections, opt in to a larger buffer on the **successor**:

```toml
[tcp]
handoff-read-ahead-bytes = 268435456 # 256 MiB per connection
```

Zero retains the 8 MiB default. Explicit limits must be between 64 KiB and
1 GiB, and require a receiver restart. Inherited sockets and connections
accepted during recovery use the same limit, for all three wire protocols.
This changes no handoff protocol or on-disk format. SIGHUP validates the value
but does not resize running receivers.

Size from **wire bytes per second per connection**, including dictionary/name
churn, multiplied by the measured ordered-recovery interval, with margin.
The relay queue must also cover detach/snapshot/descriptor transfer before the
successor can pump sockets. An 8 MiB buffer covers only 0.26 seconds at
32 MB/s; a seven-second tail can exhaust a five-second sender queue even with
successful socket inheritance. A larger byte buffer covers that finite pause;
it does not compensate for sustained intake below the offered rate or an
unavailable host.

Allocation is lazy and capped separately on **each** connection. Budget the
limit times peak concurrent connections (including new connections during
recovery), plus 64 KiB read blocks, decoder snapshots and temporary ring-growth
copies. There is no aggregate reservation limit. With 80 connections at
256 MiB, the final ring capacities alone can reach 20 GiB. Watch:

- `tcp_handoff_read_ahead_limit_bytes`: effective limit on the running receiver.
- `tcp_handoff_read_ahead_buffered_bytes`: queued wire bytes across connections.
- `tcp_handoff_read_ahead_max_connection_buffered_bytes`: largest connection queue.

These byte queues are transient; a crash still needs sender-side durable
retention/acknowledgements. Repeated planned handoff preserves queued bytes in
the existing connection snapshots, and a full buffer still applies backpressure.

Input handoff protocol version 2 sends a small JSON header containing 64-bit
connection-state lengths, followed by raw state bytes and batched descriptors.
Dictionary snapshots reserve their raw output capacity and transfer their owned
storage without another full copy. The input-tail window remains 20 seconds;
after Detach, transfer gets a separate budget of one minute plus one second per
16 MiB of state. The successor waits for the predecessor's commit before reading
any handed-over socket.

The old worker probes the successor binary before offering sockets and checks
the registering executable again where the platform supports it. If support is
missing, the receiver configuration is incompatible, or the successor fails to
request input within the tail window, input stops and the successor uses the
ordinary recovery path. A disabled eligible handoff logs
`store input handoff disabled; stopping input` with the setting's name. Successful
handoff logs `input handed off to successor` and
`took over input from stopping instance`.

After Detach, a failed store-input transfer retains the original sockets and
snapshots for **two minutes from the first failed transfer**. The successor
retries the complete transfer without reading partial results. The predecessor
has released its database and sealed the immutable tail, so resuming receivers
there would race recovery. Repeated failures never renew the retention window;
registration, capability probes, control writes and descriptor acknowledgements
are bounded by its remaining time too. No new configuration option is needed.

If no successor commits before that deadline, the predecessor closes retained
sockets and exits through the ordinary shutdown path. It logs ERROR
`input retention expired; predecessor exiting`. Each discarded accepted socket
logs ERROR `input handoff socket discarded` with `reason`,
`kernel_unread_bytes`, and `leftover_bytes`. Linux samples the exact unread
receive-buffer count with SIOCINQ immediately before close, without draining the
socket. Other platforms, or an ioctl failure, report `kernel_unread_bytes = -1`.
This is a close-time observation; bytes still in transit are outside that count.
Leftovers include decoder snapshots and buffered wire input, so their byte
count cannot be translated directly into lost points. An ERROR
`input handoff sockets discarded` sums `connections_closed`, known
`kernel_unread_bytes`, `kernel_unread_unknown`, and `leftover_bytes`.

The process exports these counters with a bounded `reason` label
(`retention_timeout` for expiry):

- `input_handoff_closed_connections_total`
- `input_handoff_closed_kernel_unread_bytes_total`
- `input_handoff_closed_kernel_unread_unknown_total`
- `input_handoff_closed_leftover_bytes_total`

Other discarded detached input, failed snapshots/duplication, and inherited
startup failures use the same accounting. Descriptor copies released after a
commit or an uncommitted receiving attempt log the same byte fields with
`discarded = false` and do not increase loss counters. The final ERROR logs are
the durable observation because the predecessor has already stopped its metrics
endpoint. Process crashes and forced termination retain ordinary TCP delivery
limitations.

All input capability tokens (`input`, `shared-store`, `store-input`) advance to
version 2; the read-listener protocol stays at version 1. Version 1 and version 2
binaries decline input inheritance in both upgrade and rollback directions,
before detaching input, and use ordinary stop/rebind with ordered recovery. This
fallback preserves points already accepted by the receiver; it retains the
existing limitations for unread TCP bytes described below. A mixed-version
restart does not claim the connection-preservation guarantee of a version 2 to
version 2 handoff.

Without handoff, a successful TCP write is not a delivery acknowledgement.
Bytes accepted by TCP but not parsed before the receiver closes can be lost;
neither the dump nor the input log protects bytes the receiver has not emitted.
A sender can replay a write that fails, but a later failure does not identify
previous successful writes that were unread. Stream framing and zstd do not add
application acknowledgements. Handoff prevents this planned-restart loss by
preserving the same sockets, unread kernel bytes, partial lines/frames, and
stream state. Unexpected read and decode errors remain errors; locally closed
connections during planned shutdown log at INFO without decode-error counts.

## Boot-time management credentials

The embedded Buckyd management API normally fails startup if its configured
JWT secret is missing. Because that configuration runs after input recovery,
a late boot-time secret can repeatedly close freshly recovered input sockets.
For hosts whose secret delivery can lag store startup, opt in:

```toml
[buckyd]
auth-jwt-secret-file = "/path/to/managed/secret"
wait-for-auth-secret = true
```

Only an initially missing file may defer startup; invalid configuration,
permissions, an initially empty secret, and bind errors still fail. Input and
reads start normally. All management requests return HTTP 503, with
`Retry-After: 1`, until a request successfully loads a nonempty secret. Requests
retry file delivery; no background worker or unauthenticated management mode is
introduced. After publication the secret remains immutable and ordinary JWT
authorization applies, including the existing public discovery endpoint.
`buckyd_auth_secret_ready` is zero during the wait and one after loading. It
changes on a management request, so probe a protected endpoint when checking
delivery. The startup warning names the deferred setting. Default is false;
changing it requires restart, as with other Buckyd settings.

Restore the managed secret delivery/order in configuration management too.
Neither this option nor socket handoff can preserve input through a period
where the store host itself is absent.

## Local reproduction

The checked-in sender runs two real go-carbon instances in succession through
the built-in supervisor, with shared Pebble storage, compact checkpoints and an
input log. It tests all three protocols with handoff off and on:

- A line/frame split across the restart verifies preservation of partial input.
- Continuous input counts attempted and successfully written points, reconnects,
  and unique stored points, validating every value and timestamp.
- A queued burst stops the local process group, writes complete batches while
  the worker cannot read, resumes it, then sends USR2. This demonstrates the
  distinction between kernel acceptance and application delivery.

Build binaries into an explicit scratch directory, then run:

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o "$SCRATCH/go-carbon" .
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o "$SCRATCH/wire" ./test/input-handoff/wire
python3 test/input-handoff/run.py --carbon "$SCRATCH/go-carbon" \
  --wire "$SCRATCH/wire" --scratch "$SCRATCH" --output "$SCRATCH/results.json"
```

The harness removes its stores, fixtures, logs and child processes. Delete the
two build outputs separately after retaining the JSON results. Timings describe
the local host and workload; use `restart_input_unavailable_seconds` or the
successor's `previous process handed over` log for the full read pause.
`input_closed_for` in the predecessor's checkpoint log covers only the early
part of shutdown, before database close, exec and successor startup.
