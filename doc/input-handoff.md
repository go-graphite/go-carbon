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

On USR2, the worker keeps receiving while it checkpoints the cache and finishes
store flushes. It then diverts input to a tail log, closes reads and the shared
database, and releases its supervisor. The successor takes the listening and
accepted sockets immediately, before opening storage or validating the pending
checkpoint. Its handlers restore decoder state and append complete legacy
records to the **same tail file**, encoding in private 64 KiB batches rather
than serializing an allocation and writer lock per point. New connections use
the same capture path. The tail is synced every second and when sealed.

Storage opening, checkpoint verification, and file-list/index loading run while
capture drains input. Reads start only after the ordinary checkpoint/index
validation, and continue to merge saved history with recovered cache points.
Recovery follows complete-record file frontiers while capture continues. FIFO
workers receive batches of up to 128 points; each metric keeps its recorded
order and claims its checkpoint history before tail corrections. Once the
unreplayed suffix is at most 1 MiB, capture handlers detach and flush. The same
decoder and worker pool finish that bounded suffix, and the synced tail stays
available for crash recovery. The ordinary inherited receiver resumes the same
sockets. Decoder leftovers and partial frames retain their existing handoff
semantics. Reads still restart; input connections survive.

The source tail remains owned by pending recovery until all recovered points
are persisted, including points captured before storage opened. With a valid
checkpoint, the order is saved history, tail, newer continuous input-log replay,
then live input. This preserves corrections if the successor crashes after
accepting newer logged values. After a crash tears the final binary record,
recovery keeps its complete point prefix and warns about the unfinished end;
other structural decoding errors still fail startup. A failed startup retains the ordinary tail for
the next recovery; no new source format or handoff protocol is needed. This
works when the predecessor supports the existing version 2 input-only offer.

Capture uses local disk space proportional to input received while storage and
recovery catch up, rather than a RAM buffer or relay queue proportional to the
cache. Its sustained write and replay rates must exceed the offered rate. A
disk write/sync error stops capture and fails startup. Process crashes retain
complete appended records; bytes still in socket/decoder/encoding buffers have
the usual TCP delivery limitations below, and power loss has the one-second
sync window. No application-level acknowledgement is introduced.

`input capture active before storage and checkpoint` marks intake readiness.
`startup input capture sealed` reports captured points/bytes, replayed prefix
points/bytes, and capture duration. The following `input tail loaded` reports
only the remaining suffix. Add prefix and suffix when checking recovered counts;
add predecessor tail points and successor captured points for the input total.
`restart_input_unavailable_seconds` includes both the initial gap to capture
readiness and the final capture-to-live pause. The restart log also reports
`input_capture_switch_seconds`, so the second pause is not hidden by early
intake readiness. Snapshotting decoder state at that switch still scales with
connection state; it does not wait for unrelated cache history.

The brief final switch, and the existing mirrored-store handoff path, use RAM
read-ahead. It retains at most 8 MiB per connection plus a 64 KiB read block. A
full buffer applies TCP backpressure; it never drops bytes. Memory grows with
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
