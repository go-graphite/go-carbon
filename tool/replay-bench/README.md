# replay-bench

A small, system-agnostic driver for replaying an export of real series into a Graphite
TCP listener at full rate and measuring reads against the same data. It is used to
compare storage engines on identical input (go-carbon store engines,
VictoriaMetrics, ClickHouse with graphite-clickhouse).

Input is Graphite plaintext, one `path value timestamp` line per point, optionally
gzipped. Sort it by timestamp first (`sort -s -n -k3`) so every system sees a
time-ordered stream; lines of one path always reach the server over one connection, in
file order.

| Command | What it does |
| --- | --- |
| `send -data d.gz -addr host:2003 [-conns 16]` | Streams the file at full rate over `-conns` connections sharded by path and prints lines, bytes, seconds and lines/s as JSON. |
| `read -data d.gz -api carbonserver\|prom -base URL -shape NxSPAN [-n 100] [-warmup 5] [-pids p1,p2]` | Times `-n` serial requests, each on a fresh connection, after `-warmup` untimed ones; prints p50/p99 ms, response bytes and server CPU ms per query when `-pids` is given (Linux `/proc`). |
| `check -data d.gz -api ... -base URL [-sample 400]` | Fetches `-sample` evenly spaced series over the whole export and compares every point with the input bit for bit; prints sampled/changed/missing counts and the largest relative error. |

Shapes are `<names>x<span>`: `1x24h`, `100x24h`, `1000x1h`. A `24h` window is the whole
export (at most the last 24 h); `1h` is its final hour, and its names are drawn only from
series with a point in that hour. Names are picked at even strides over the sorted
unique paths, so every system reads the same names.

APIs: `carbonserver` posts `/render/?format=protobuf` (carbonapi v2 protobuf; also what
graphite-clickhouse serves); `prom` posts a `{__name__=~"a|b|c"}` selector to
`/api/v1/query_range`.

The first scan of an export caches its path list and bounds in `<data>.sum`.

CPU, RSS and bytes at rest are measured by the caller around these commands; the
driver only reads `/proc/<pid>/stat` for the per-query CPU of `read`.
