# Stream input

The TCP receiver accepts the compact binary stream protocol on the same port as
Graphite plaintext, normally `:2003`. Each connection is detected once from the
first bytes on its existing buffered reader. Plaintext parsing is unchanged.
Relays such as nanotube can send stream frames to avoid repeating long metric
names and parsing decimal timestamps and values for every point.

```toml
[tcp]
listen = ":2003"
enabled = true
buffer-size = 0
compression = ""
stream-max-frame-size = 8388608
stream-max-names = 5000000
```

A nonempty receiver `compression` setting disables stream detection and retains
legacy compressed plaintext behavior. Stream's optional zstd is selected in its
own connection preamble. Both stream and plaintext use the receiver's existing
sink and buffering policy.

## Wire format

The public Go package is
`github.com/go-graphite/go-carbon/receiver/stream/codec`. Its
[package specification](../receiver/stream/codec/doc.go) defines the wire format,
including a [portable golden vector](../receiver/stream/codec/testdata/README.md).
It imports only the Go standard library and `klauspost/compress/zstd`.

The 10-byte connection preamble is `00 47 43 53 0d 0a 1a 0a`, version `01`, then
flags (`00` for raw frames, `01` for zstd). Unknown versions, flags and malformed
frames close the connection with an ERROR log and a decode-error counter.

Each frame has a 20-byte header containing its kind, wire payload length,
uncompressed length, row count and CRC-32C of the wire payload. Both payload
lengths must fit `stream-max-frame-size`, before allocating or decompressing.
The receiver also bounds rows at 65,536 per frame, individual names at 65,536
bytes, and distinct names at `stream-max-names`. Encoder limits must fit the
receiver's limits; zero codec options select defaults.

Data frames define new names once and assign dense connection-local IDs. They
stably sort rows by ID, then encode ID differences, timestamp differences against
each name's previous point, and XOR differences of float64 value bits. The first
point of a name predicts the previous row's timestamp and zero value bits.
Ordering within a name is preserved, including repeated timestamps; order between
names can change. Float bit patterns, including negative zero and NaNs, survive
unchanged. Compression is independent per frame.

Reset frames clear the entire dictionary and prediction state. The encoder
flushes pending rows and automatically queues a reset before adding a new name
that would exceed its configured `MaxNames`. Long-lived stable connections
amortize the initial dictionary cost; frequent reconnects or resets repeat it.

## Sender API

Create one encoder for each connection, send `AppendPreamble` once, then retain
the encoder across batches:

```go
enc, err := codec.NewEncoder(codec.Options{Flags: codec.FlagZstd})
if err != nil {
    return err
}
defer enc.Close()
wire, err := codec.AppendPreamble(nil, codec.FlagZstd)
if err != nil {
    return err
}
if _, err = conn.Write(wire); err != nil {
    return err
}
wire = wire[:0]

// Repeat for each batch, retaining enc and wire for this connection.
for _, p := range batch {
    if err = enc.Add(p.Name, p.Value, p.Timestamp); err != nil {
        return err
    }
    if enc.Ready() {
        wire, err = enc.Flush(wire[:0])
        if err != nil {
            return err
        }
        if _, err = conn.Write(wire); err != nil {
            return err
        }
    }
}
wire, err = enc.Flush(wire[:0])
if err != nil {
    return err
}
if len(wire) != 0 {
    _, err = conn.Write(wire)
}
return err
```

Flush at batch boundaries or on a timer to bound latency for quiet connections.
`Add` copies a name only when new. Known-name `Add` and raw `Flush` reuse memory
without per-point allocations after warmup. Automatic resets or size limits may
queue complete frames internally; call `Flush` when `Ready` is true to keep that
queue bounded. After a write failure, reconnect with a new encoder and preamble.
The protocol has no acknowledgements or delivery replay.

## Restart handoff and sink references

Unbuffered connections without receiver compression support the existing restart
handoff. The old process finishes any validated frame before detaching. It
prepends a preamble and snapshot to unparsed bytes, including consumed fragments
of the next frame. The snapshot is a reset followed by bounded state frames
containing names and their last timestamps/value bits. The successor processes
these through the ordinary decoder without emitting points, then resumes input.
Splitting snapshots into bounded frames lets large dictionaries obey the same
frame limit. The process-to-process handoff format is unchanged.

The decoder callback receives `(id, name, value, timestamp, slot *any)`.
`TCP.SetStreamPointOut` optionally exposes the slot to a sink that caches its own
reference per ID. Slots survive data frames and are cleared by resets. Restart
snapshots restore protocol state and leave slots empty for the successor's sink.
The hook takes precedence over `SetPointOut` for unbuffered stream input; buffered
receivers use their configured store. This protocol does not depend on a cache
implementation.

## Metrics

Rows contribute to the existing `metricsReceived` and
`metrics_received_tcp_total` counters. Stream counters follow the receiver's
existing interval `Stat` / cumulative Prometheus publication behavior:

| Stat | Prometheus | Meaning |
| --- | --- | --- |
| `streamConnections` | `stream_connections_tcp_total` | Connections detected as stream, including rejected preambles |
| `streamFrames` | `stream_frames_tcp_total` | Validated data, reset and state frames |
| `streamPayloadBytes` | `stream_payload_bytes_tcp_total` | Validated wire payload bytes, excluding headers |
| `streamNamesDefined` | `stream_names_defined_tcp_total` | Names defined by data or state frames |
| `streamResets` | `stream_resets_tcp_total` | Validated reset frames |
| `streamDecodeErrors` | `stream_decode_errors_tcp_total` | Protocol errors, truncated frames and transport failures |

Stream failures also increment the receiver's existing `errors` counter. Clean
EOF between frames does not count as a failure. No rows from a rejected frame
reach the sink.

## Reproducing codec benchmarks

```sh
go test ./receiver/stream/codec -run '^$' \
  -bench 'Benchmark(Stream|PlaintextZstd)' -benchtime=3x -benchmem
STREAM_BENCH_NAMES=5000000 go test ./receiver/stream/codec -run '^$' \
  -bench BenchmarkStream -benchtime=3x -benchmem
```

The default dataset uses one million 150–300-byte dotted names, one point per
name per second, and a mix of integer counters, rounded gauges, stable gauges
and random float mantissas. Stream measurements exclude the initial dictionary
and report `ns/point` and `wire-B/point`; one benchmark operation is one epoch.
The plaintext comparison compresses the same epoch with zstd levels 1 and 3,
including names on every point. It excludes decimal formatting time. First-use
names and resets cost more than the warm stream cadence.

`cold-B/point` reports the first synthetic epoch, including the dictionary. To
measure a private plaintext capture (optionally gzip compressed), without adding
it to the repository:

```sh
STREAM_SAMPLE_FILE=/path/to/capture.txt.gz go test ./receiver/stream/codec \
  -run '^TestSampleRoundTrip$' -bench BenchmarkSample -benchtime=1x -benchmem
```

The capture test compares every decoded name, timestamp and float bit pattern
with the original points in stable per-ID order. The capture benchmarks measure
a fresh connection, including dictionary setup and allocations; they are not
warm replay measurements. Like the TCP plaintext reader, they ignore an incomplete
trailing line. Benchmark output contains counts and timings, without metric names.

### Independent zstd input

The first-bytes dispatcher selects plaintext, independent zstd frames (magic
`28 b5 2f fd`), or a stream preamble (leading NUL) once per connection. Stream
is a sibling of zstd input. Outer zstd frames carry plaintext only: a leading
NUL in their decoded input closes the connection, increments `zstdDecodeErrors`
and logs an ERROR explaining that nested stream is unsupported. Use stream's
`FlagZstd` for compressed stream frames instead. Explicit receiver compression
selects plaintext in that compression format and disables stream detection.

Unbuffered autodetected plaintext, zstd and stream connections all support
restart handoff. Plaintext transfers its partial line, zstd re-frames decoded
plaintext plus any partial wire frame, and stream transfers a preamble,
dictionary/delta snapshot and pending frame bytes. No per-id sink references
cross a restart; the new process resolves them against its own cache.

### Cache insertion

The application binds unbuffered stream slots through `Cache.AddStreamPoint` to
`Cache.AddPointRef`. Once an ID resolves, appends validate its current cache slot
under the shard lock without hashing or probing its name. Cache removal or table
growth invalidates the hint and resolves it through ordinary insertion. Tags and
graceful input diversion also use ordinary insertion. Dictionary resets and
restart snapshots clear references before an ID can refer to another name.

An opt-in receiver/cache benchmark reads a private capture prefix, preserving
its names and values in memory, and repeats it for eight one-minute epochs:

```sh
STREAM_SAMPLE_FILE=/path/to/capture.gz go test ./receiver/tcp -run '^$' \
  -bench BenchmarkIngestCapture -benchtime=1x -benchmem -count=3
```

All four modes use the same resident cache and points. Timing includes connection
setup and dictionary/reference initialization. Divide `allocs/op` and `B/op` by
the reported row count to obtain per-point allocation costs. The untimed oracle
checks every cached value bit pattern and timestamp; no captured names are saved.
