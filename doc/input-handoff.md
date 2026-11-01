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
bytes, replays the tail, and resumes reading those sockets. Reads still restart;
input connections survive. A short read pause can backpressure a sender without
closing its connection.

The old worker probes the successor binary before offering sockets and checks
the registering executable again where the platform supports it. If support is
missing, the receiver configuration is incompatible, or the successor fails to
take input within the existing timeout, input stops and the successor uses the
ordinary recovery path. A disabled eligible handoff logs
`store input handoff disabled; stopping input` with the setting's name. Successful
handoff logs `input handed off to successor` and
`took over input from stopping instance`.

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
