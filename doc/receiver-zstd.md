# Zstd plaintext TCP input

The default TCP receiver accepts ordinary Graphite plaintext and concatenated,
independent zstd frames on the same port. No receiver configuration change is
needed. The first four bytes `28 b5 2f fd` select zstd; other input follows the
existing plaintext path. Dispatch runs once per connection, outside the line
parser. A future protocol can extend `dispatchFirstBytes` at that boundary.

```toml
[tcp]
listen = ":2003"
compression = ""
zstd-max-frame-size = 16777216
```

Explicit `compression = "gzip"`, `"snappy"`, or `"zstd"` selects that format
without detection. Legacy gzip/snappy behavior is unchanged. With automatic
selection, a leading skippable frame does not select zstd: send a regular zstd
frame first. Skippable frames between zstd frames are accepted and skipped.

Each frame is read according to the [zstd frame format](https://www.rfc-editor.org/rfc/rfc8878.html#section-3.1),
then decoded with `DecodeAll` into a reused buffer. Lines may span frames. The
limit bounds the advertised window, advertised content size, and actual decoded
size, including frames that omit their content size. The configured range is
1 KiB through 1 GiB; zero uses 16 MiB. The
compressed-frame bound is `limit + limit/128 + 1024`, including headers,
checksums and skippable payloads. Dictionaries are unsupported. Malformed,
truncated, oversized, or checksum-invalid input closes the connection, logs an
ERROR, and increments `zstdDecodeErrors`. EOF between complete frames is normal.

An unbuffered receiver using automatic detection retains graceful restart
handoff. It re-frames the decoded but unparsed plaintext tail as independent raw
zstd frames, followed by the unread compressed data. If a socket read was
interrupted inside a frame, its consumed prefix is retained with that data; the
successor starts decoding that frame from its beginning. No decoder state moves
between processes. Explicit compression and buffered receivers retain their
existing stop/reconnect behavior. Both restart participants must support zstd.

Receiver stats add `zstdConnections`, `zstdFrames`, `zstdCompressedBytes`,
`zstdDecompressedBytes`, and `zstdDecodeErrors`. Their Prometheus counterparts
are `tcp_zstd_connections_total`, `tcp_zstd_frames_total`,
`tcp_zstd_compressed_bytes_total`, `tcp_zstd_decompressed_bytes_total`, and
`tcp_zstd_decode_errors_total`. As with received-point counters, Prometheus
updates when receiver stats are collected. Frame and byte counts describe this
process's decoding work; a re-framed handoff tail is additional decoding work in
the successor. Skippable bytes count as compressed input but not data frames.

## Local validation

Use the repository's vendored modules. Keep a private gzip sample outside the
repository; neither benchmarks nor tests print its metric names.

```sh
go test -race ./receiver/tcp ./receiver/parse -timeout 150s
ZSTD_SAMPLE=/path/to/private/sample.gz go test ./receiver/tcp \
  -run '^$' -bench BenchmarkZstdSampleDecode -benchmem
```

The opt-in integration builds against this receiver's production code and uses
a real nanotube executable. It checks exact parser outputs before storage can
coalesce duplicates. It exercises timed flushes, idle connection refresh, mixed
plaintext/zstd targets, single TCP and mTCP, and descriptor handoff to a separate
successor process. Unit tests additionally interrupt frames and split lines
across frames.

```sh
# Build nanotube in its own checkout with its Makefile conventions.
NANOTUBE_BIN=/path/to/nanotube go test -race -tags zstdintegration \
  ./receiver/tcp -run '^TestZstdNanotubeIntegration$' -v -timeout 90s
```

On Linux, the full executable smoke test also starts the go-carbon application with its
supervisor, restarts via USR2, checks handoff logs, and queries exact stored values:

```sh
go build -o /path/to/scratch/go-carbon .
python3 test/zstd/smoke.py --carbon /path/to/scratch/go-carbon \
  --nanotube /path/to/nanotube --scratch /path/to/scratch
```

The Linux scratch directory must support Unix sockets and permissions; for
a container on macOS use a container filesystem or tmpfs, rather than a bind
mount backed by the host filesystem.

Set `TMPDIR` and `GOTMPDIR` to an appropriate scratch directory if system temporary
storage is unsuitable. Both integration tests bind only localhost.

Deploy zstd-capable receivers everywhere first, then deploy the sender, then
enable compression by pool or globally. Disable the sender setting before
rolling receivers back. TCP still has no application acknowledgement: a sender
replaying an ambiguous failed write can duplicate points, with or without zstd.
