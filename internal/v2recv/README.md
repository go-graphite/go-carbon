# Core-first TCP receiver experiment

`v2daemon.Store.ReceiveTCP(listener, config)` connects the existing relay stream
codec and plaintext parser directly to serial daemon producers. It accepts the
same binary preamble, optional per-frame zstd compression, plaintext, and zstd
plaintext on one endpoint. This is an offline L5 experiment; the ordinary daemon
startup and production receiver configuration are unchanged.

Plaintext names travel in reusable byte batches through the L3 application's
`ByteOwner.AppendBytes` handoff. The owner hashes these bytes directly and creates
a string on a fingerprint-table miss. Quota checks for known names reuse the
catalog string. Partial socket batches copy name bytes into the serial worker's
pending batch before recycling the source; offsets are rebased with the copy.
Raw L1/L2 owners retain the string interface, and codec names retain their existing
dictionary strings. Both interfaces borrow their arguments only until return.

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
CPU attribution; process CPU alone is not decoder CPU. JSON results include
receiver admission/error counters and timed interval boundaries. The finite
fixture also checks that counted socket bytes equal the complete encoded payload. Encoded socket bytes per
point exclude TCP/IP headers and are a synthetic fixture ratio, not measured NIC
bytes or production compression. Report co-tenant load and CPU placement with
every host measurement; shared-host numbers remain provisional.

## Epoch-tagged acknowledged placement input

`Store.ReceivePlacedTCP` is the opt-in L2/L3 receiver for the shared `V2A1`
protocol. It uses the existing placement authority, catalog, original-application
journal and restart machinery. Plaintext and the unversioned stream codec remain
on the existing unplaced receiver. A placed endpoint accepts only authenticated
placement frames; it never falls back to unversioned input.

```go
server, err := store.ReceivePlacedTCP(listener, v2recv.Config{
    PlacementKeys: map[string][]byte{"relay-a/inc-1": producerSecret},
}, policyResolver) // nil resolver for raw journaled L2
```

Keys contain 32..4096 bytes and are unique per persistent producer incarnation.
The singleton `PlacementProducer`/`PlacementKey` form is also accepted. The
receiver detaches configured keys. HMAC-SHA256 covers the header and canonical
bounded frame, including group, replica, store incarnation, map digest/revision,
epoch, vshard, producer/lane, sequence, name, timestamp and IEEE value bits. The
store authenticates the producer, checks its registered lane and the complete
placement selection, then appends with the original stamp. Each stream is pinned
to one serial storage producer; store geometry must stay fixed across reopen.

A positive ACK is emitted only after catalog and stamped data journal sync.
The same journal packet retains the original relay position and payload digest;
restart rebuilds duplicate frontiers from that packet. There is no separate
receipt file whose commit could race data durability. A retry of the latest
accepted position with the exact payload returns its ACK without another append,
even after a fence. Conflicting retries, gaps, older positions and novel stale
writes fail. Older-than-latest retransmission is conservatively rejected; the
paired relay persists each ACK before dispatching a stream's next record.

Multiple authenticated producers have independent frontiers and credentials.
Producer calls serialize per owner, and one receiver owns a Store's producer
lifecycle. Stop and `Wait` for it before reading `DurableRelayEnds`, fencing,
closing or directly invoking producer methods. `FenceRelayPlacement` compares
every supplied final position with those recovered durable ends, including zero
lanes from the map census, before invoking the existing durable authority/core
fence. Its coverage callback must still certify coherent pinned roots. A drain,
connection close, journal sync or this protocol's ACK cannot attest target copy
coverage. One daemon root currently owns one PG; several PG roots on one endpoint
require a host dispatcher implementing `v2recv.PlacedSink`.

Authority adoption now retains validated predecessor documents and their exact
successor activation receipts for original journal replay. This permits restart
of acknowledged data after metadata revisions and owner changes. Live admission
continues to require the current map and open epoch. Old runtimes cannot silently
ignore the extra persisted authority/journal fields.

The wire source and independent golden fixture are byte-identical to the relay's
`internal/v2wire`. Tests cover blocked/failed durability before ACK, authentication,
independent producers, lost response replay, store SIGKILL/reopen, conflicting
payloads, stale/vshard rejection, checked real-core fencing and historical-map
replay. The loopback relay/store probe uses the test-only
`V2_STORE_ACK_CRASH_ROOT` helper together with the relay's
`TestExternalStoreACK`, preserving both roots across SIGKILL and reconnect.

This is a correctness-first experimental path. Frames drain individually; there
is no group-commit throughput qualification or complete cutover controller.
HMAC does not encrypt payloads; use a trusted network or TLS listener as needed.
No production receiver configuration or automatic startup path is changed.

Placed connections collect up to 256 authenticated frames or about 256KiB of frame memory, with a 2ms quiet
flush bound, and use `PlacedBatchSink` when available. The daemon serializes a
whole batch on its storage producer, drains its stamped journal once, and then
permits ACKs. Recovery retains the latest 256 exact retry digests per stream,
matching the relay's maximum outstanding window. Novel stale writes and
conflicting or expired retries still fail. The wire format and golden frame are
unchanged; roll out the recovered retry window before enabling batched relays.
