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
database, and releases its supervisor. The successor opens the database,
receives the listening and accepted sockets plus their decoder state/buffered
bytes, and starts the inherited handlers before replaying the tail. Each handler
restores its own dictionary in parallel. A lazy raw-byte buffer drains each
inherited socket (and connections accepted during recovery) while restoration
and tail replay run. Every live point sink waits until tail replay completes:
tail corrections for a metric always precede newer socket points for that metric.
Reads still restart; input connections survive.

Read-ahead retains at most 8 MiB per connection plus a 64 KiB read block. A
full buffer applies TCP backpressure; it never drops bytes. Memory grows with
actual queued bytes, independently of packet sizes. At 280 connections the
maximum ring allocation is 2.19 GiB. Once the tail is applied, each handler drains
its buffered bytes, frees the ring and returns to direct socket reads. A slow
recovery or input beyond this byte budget can still backpressure senders.

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
snapshots. The successor retries the complete transfer without reading partial
results. The predecessor has released its database and sealed the immutable
tail, so resuming receivers there would race recovery. It therefore stays alive
until a capable successor commits the transfer. A persistent failure requires a
compatible successor; it can stall senders, but the predecessor does not discard
unread sockets just because the control connection failed. Process crashes and
forced termination retain ordinary TCP delivery limitations.

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
