# Experimental input-log-only restart

`dump.input-log-only` defaults to `false`. It prototypes a planned restart with
one recovery format, the continuous input journal. It requires background
input-log replay and supervised TCP socket handoff on a shared Pebble store.
The experiment must remain disabled until background replay has been integrated
and qualified. It is a separate opt-in from `dump.enabled`.

```toml
[dump]
enabled = false
input-log-only = true
handoff-supervisor = true
store-input-handoff = true
path = "/var/lib/graphite/dump"

[input-log]
enabled = true
background-replay = true
path = "/var/lib/graphite/input-log"
```

At stop, persistence finishes its current writes and the store is synced. Each
cache shard streams its remaining queued and in-flight points into journal
frames. This bypasses the bounded live staging buffer, so even a single metric
larger than that buffer is fully logged. Input continues on other shards. A
zero-point journal frame publishes the new replay boundary only after all
snapshot frames are synced; interruption earlier retains the old boundary.
Later live input follows the snapshot of its shard.

The predecessor closes the store and releases its supervisor while its receivers
keep writing to the cache and journal. Detaching those receivers preserves
accepted sockets, partial lines, compressed frame prefixes and stream decoder
state. The predecessor then syncs and closes the journal, and transfers the
sockets. The successor opens the journal and ordinary cache sinks before opening
Pebble. Background replay must preserve old history ahead of live writeout.
No dump, input tail, pending checkpoint or capture spool is written in this mode.

Reads can omit history until background replay finishes. Existing dump/capture
recovery must finish before enabling the mode; unsupported receiver layouts or
successors are rejected. Changes to this setting require a restart. A successor
must advertise support for this configuration: changing to an older binary while
stopping in this mode is refused. To return to dump mode, complete recovery, keep background replay enabled,
and restart a capable binary with `dump.input-log-only = false` and
`dump.enabled = true`. The predecessor marker selects journal-only recovery
for that start; subsequent stops use dumps. Older binaries require that
intermediate dump-mode stop before they can take over.

The local harness checks every rendered value for plaintext, zstd and stream
input, including partial frames, continuous input and unread queued connections:

```sh
go build -o /tmp/carbon-inputlog .
go build -o /tmp/carbon-wire ./test/input-handoff/wire
python3 test/input-handoff/run.py --carbon /tmp/carbon-inputlog \
  --wire /tmp/carbon-wire --scratch /tmp --output /tmp/inputlog-results.json \
  --enabled true --input-log-only --iterations 20
```

It reports lost points, input gap, replay/cache ratio and dump files written.
The acceptance limits are zero loss or value mismatches, an input gap of at most
1s, replay no larger than 1.1 times the unpersisted cache, and zero dump/tail/
capture files. Fixtures, process groups and stores are cleaned up automatically;
remove the two executables and the result file after recording the results.
