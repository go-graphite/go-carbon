# Carbonserver read replay

`read-replay` adds a bounded GET render workload from a host's own JSON logs.
It uses only the Go standard library. It never changes server configuration or
data. The target defaults to `http://127.0.0.1:8080`; proxies, redirects, and
automatic response decompression are disabled. Request URLs must use the local
`/render` or `/render/` path, so logged URLs cannot select another origin.

When a frontend sends every render to every store in a cluster, shrinking three
stores to two keeps request counts per store roughly constant, while each store
serves 1.5 times as many data-bearing renders. Replaying another 50% of one
store's successful renders approximates that additional work. This tool does
not decide whether a capacity gate passes; use server and client observations
together.

## Build

From the repository root:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor \
  go build -trimpath -o read-replay ./tool/read-replay
```

## Dry run

Flags precede the input paths. Plain files and `.gz` files can be mixed; shell
globs work. Each summary path must be new, to preserve previous evidence and
prevent accidentally overwriting input logs.

```sh
./read-replay --dry-run --rate-multiplier 0.5 --max-rps 100 \
  --start 2026-11-01T18:00:00Z --end 2026-11-01T19:00:00Z \
  --summary yesterday-plan.json carbon.log.2026-11-01-18.gz
```

The dry run sends nothing. It prints matched/skipped/malformed counts, arrivals
for every source minute (including zero-count minutes), the top ten decoded
`target` query values, original and requested RPS, and the capped RPS upper
bound. Multiple targets in one render count separately in the top-ten table.

Only records with `logger=carbonserver`, `message=fetch served`,
`handler=render`, and `http_code=200` are replayed. The timestamp, URL, and a
nonnegative `runtime_seconds` must be valid. Other records are skipped without
parsing render details. Malformed records, including lines larger than 8 MiB,
are counted and skipped; file read/decompression errors stop loading before
any sends. Input is sorted by timestamp and retained in memory, so memory usage
grows with the selected request count. Separate overlapping input files are not
deduplicated.

## Sixty-minute extra 50% load

For a replay at 18:00 UTC on November 2, select the same hour on November 1.
Choose caps from the dry run and available headroom; the example caps below are
illustrative, and can reduce the achieved rate.

```sh
./read-replay --target http://127.0.0.1:8080 \
  --start 2026-11-01T18:00:00Z --end 2026-11-01T19:00:00Z \
  --rate-multiplier 0.5 --duration 60m \
  --max-rps 100 --max-concurrency 8 --timeout 10s \
  --max-error-ratio 0.05 --abort-p99 2s \
  --summary canary-extra-50pct.json carbon.log.2026-11-01-18.gz
```

The multiplier stretches interarrival gaps: `0.5` runs the historical arrival
process at half its speed. A full source hour therefore takes two hours to
replay; a sixty-minute run consumes approximately its first thirty minutes.
For a constant additional rate, use a fixed `--rps` equal to half the dry run's
`original_rps`. Fixed-rate mode also consumes requests in timestamp order and
loops when it reaches the end. `--rps` and an explicit
`--rate-multiplier` are mutually exclusive.

Every loop shifts each request independently against its original log timestamp
and its actual send time. Only integer epoch seconds in `from` and `until`
(including repeated or encoded keys) are replaced. Relative values such as
`-1h` and `now` are left alone. All other URL bytes, including parameter order,
target escapes, and format, are preserved. Whole seconds introduce at most one
second of rounding. Timestamps accept RFC3339 and the log's `+0100` form;
the window is inclusive at the start and exclusive at the end.

With explicit `--start` and `--end`, that entire interval defines the source
cycle, including quiet edges. Otherwise the cycle begins at the first selected
record and ends one mean interarrival gap after the last selected record (one
second for a single timestamp). Supply both bounds for sparse hourly logs.

The maximum rate enforces spacing between launches, with no burst allowance.
The concurrency cap includes body reads. Missed arrival slots, rate-limited
slots, and slots encountering full concurrency are dropped and counted; they
are never queued for catch-up. The planned capped rate is an upper bound,
especially for bursty or sparse input. Check `started`, drops, and actual
elapsed sending time to confirm the intended extra load was achieved.

## Measurements and stopping

The tool prints a JSON line each minute, a final partial minute, and a total
summary line. The summary file contains configuration, input statistics, source
rate plan, per-minute/total measurements, stop times, stop reason, and the
60-second safety window at the stop. Minutes are measured from replay start;
completion counts belong to the minute in which results are collected. Drain
completions can appear after the sending interval.

Measurements include launches, completions, HTTP status counts, transport
errors, timeouts, HTTP errors (non-2xx), unique failed completions, fully read
body bytes, and client p50/p90/p99/p99.9/max latency in milliseconds. Bodies are
read to EOF and discarded; latency and timeout include the body read. A body
read error also fails a request. HTTP status counts can overlap transport
errors, so use `failed/count` for a unique failure ratio. Logged
`runtime_seconds` quantiles and `logged_from_cache` counts describe the
completed replay requests' original log records, not the new server runtime
or new cache hits.

Histograms have bounded memory and 128 buckets per power of two. Quantiles use
bucket upper bounds with less than 0.8% relative error; maximum is exact.
Safety p99 uses the same conservative quantiles.

The tool stops sending and exits **3** when the completed requests in the last
60 seconds have a failure ratio above `--max-error-ratio` (default `0.05`) or
client p99 above `--abort-p99` (default `2s`). The check applies from the first
completion, without a warmup or minimum sample count, and runs on completions
and once per second. It uses unique failures (HTTP non-2xx, transport error,
or timeout). Outstanding requests are bounded by the per-request timeout but
do not enter safety statistics until completion.

Duration expiry, SIGINT, SIGTERM, and safety stops allow in-flight requests up
to five seconds to finish, then cancel the remainder and write the summary.
`undrained` reports requests still outstanding at that deadline; their
incomplete bytes/latencies are excluded. Duration expiry and signals exit
**0**; invalid input/configuration or a summary write failure exits **1**.

## Read the capacity result

Compare the canary store's **server-side read p99 and CPU** with its untouched
peers during exactly the same minutes. Compare with a baseline before the
replay and inspect the client p99 by minute as well. A successful process exit
only means the safety limits did not stop it; it does not establish that read
p99 avoided regression. Confirm that the achieved extra load was close to
50%, caps did not drop material traffic, errors stayed low, and server read
p99 remained comparable to baseline and peers with sufficient CPU headroom.

Use yesterday's same-hour input to bring a different historical series mix,
reducing artificial cache warmth from replaying the just-completed live hour.
That does not guarantee cold blocks: live reads, repeated targets, looped
inputs, and shifted time windows still affect cache warmth. The tool only
replays successful data-bearing renders; existing live 404 fan-out stays in
place. It does not simulate redistribution of files, a larger working set,
changes to disk layout, or rebalancing. `from_cache=true` records are included;
they may represent query-cache hits and less storage work. Their logged
runtime distribution is context, not a server-side baseline for a new run.

## Tests

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor \
  go test -race -timeout 150s ./tool/read-replay
```

Pacing, looping, stalls, safety thresholds, and drain deadlines use
`testing/synctest`. HTTP and signal integration tests start their own localhost
servers. No external services or fixtures are required.
