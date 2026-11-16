# Core-first TCP receiver experiment

`v2daemon.Store.ReceiveTCP(listener, config)` connects the existing relay stream
codec and plaintext parser directly to serial daemon producers. It accepts the
same binary preamble, optional per-frame zstd compression, plaintext, and zstd
plaintext on one endpoint. This is an offline L5 experiment; the ordinary daemon
startup and production receiver configuration are unchanged.

Each connection owns its decoder/dictionary and caches canonical name strings in
the decoder's opaque slots. Stable name hashing routes every occurrence of a name,
including on different connections, to the same producer. Each producer has one
bounded channel and one goroutine calling Append/Seal/Drain; producer catalogs,
input predictors and pending buffers have no shared writer lock. Full queues block
socket readers. No drop queue exists. Connection-lifetime locking does not protect
point data. Connection count, frame/dictionary sizes, batch size and queue depth
are bounded. Defaults are 128 connections, 1024 points per batch and two queued
batches per producer. Decoder dictionaries default to 64MiB per connection; include
that multiplied by MaxConnections when budgeting host memory.

Every second, including when the socket is idle, each producer seals and drains
its nonempty pending input. The existing daemon syncs catalog definitions before
journal fsync and releases journaled rows through the existing bounded core queues.
Final receiver Wait drains owners; Store.Close fences durable core completion.
An explicit MaxPendingRows in the daemon may force earlier groups. The raw adapter
accepts named L1 and journaled L2 stores. It rejects L0 and L3 at startup: exact
original application policies/groups require the separate L3 adapter. Raw-layer
throughput is not complete-rollup daemon throughput.

Append failures are sticky, logged and counted, stop new intake and close active
sockets. Rejected decoded points and malformed plaintext lines are counted;
malformed frames/partial transports increment TransportErrors and log the peer.
A rejected binary frame reaches no owner. The protocol has no durable wire ACK:
successful socket Write does not establish core admission. For finite experiments,
wait for Stats.Accepted to equal the offered count before Wait stops the listener;
this also ensures all connections in the kernel accept backlog were handled.
Wait cancellation closes connections, logs failure and returns an error while
still draining already decoded input. Call Store.Close only after Wait returns.

## Validation and finite measurements

`go test ./internal/v2recv` checks all four protocol modes over 32 simultaneous
fragmented TCP streams, closes/reopens the real journaled core, and checks every
name/timestamp/value bit exactly once. Names shared across connections exercise
stable producer routing. Separate tests cover idle synchronization, a blocked
owner, terminal append failure, malformed plaintext and corrupted binary frames.
The codecs' existing tests remain the reference for dictionary resets and special
IEEE values.

The prior receiver integration used the same production codecs but a shared
backend sink. That sink cannot own the core-first serial producers. This adapter
keeps the codecs and finite sender/oracle pattern, replacing only admission and
lifecycle ownership. It does not import the older backend or duplicate its storage.

Build the finite test binary with the release toolchain and cgo:

```sh
GOTOOLCHAIN=go1.27.0 CGO_ENABLED=1 go test -c ./internal/v2recv -o receiver.test
V2_TCP_ROOT=/fresh/root V2_TCP_MODE=binary ./receiver.test -test.run '^TestMeasureTCP$' -test.v
```

Modes are `l0`, `binary`, `plaintext`, and `decode` (TCP with a no-op owner). Each
uses 48 senders, 48 producers, 12 core shards and 512 names per sender, with 256
updates per name by default (`V2_TCP_ROUNDS` overrides). Input generation/encoding
is outside timing; sender socket-copy CPU remains included. Completion includes
receiver drain and core Close. L0 bypasses names, log and TCP; decode bypasses the
core. A reopened full-core oracle detects missing/duplicate tuples after timing.
`V2_TCP_PROFILE=/path/cpu.pprof` captures the timed arm for exclusive stack-based
CPU attribution; process CPU alone is not decoder CPU. Encoded socket bytes per
point exclude TCP/IP headers and are a synthetic fixture ratio, not measured NIC
bytes or production compression. Report co-tenant load and CPU placement with
every host measurement; shared-host numbers remain provisional.
