# Stream protocol codec

A standalone encoder and decoder for connection-local metric dictionaries,
timestamp deltas and lossless float64 value deltas. The codec does not select
network protocols or depend on a receiver or cache implementation.

## Wire format

The public Go package is
`github.com/go-graphite/go-carbon/receiver/stream/codec`. Its
[package specification](../receiver/stream/codec/doc.go) defines the wire format,
including a [portable golden vector](../receiver/stream/codec/testdata/README.md).
It imports only the Go standard library and `klauspost/compress/zstd`.

The 10-byte connection preamble is `00 47 43 53 0d 0a 1a 0a`, version `01`, then
flags (`00` for raw frames, `01` for zstd). Unknown versions, flags and malformed
frames return terminal errors; callers must discard the decoder.

Each frame has a 20-byte header containing its kind, wire payload length,
uncompressed length, row count and CRC-32C of the wire payload. Both payload
lengths must fit `Options.MaxFrameSize` (8 MiB by default) before allocating or
decompressing.
The decoder defaults to 65,536 rows per frame, 65,536 bytes per name, and
5,000,000 distinct names per connection. These limits are configurable through
`codec.Options`. Encoder limits must fit the
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
