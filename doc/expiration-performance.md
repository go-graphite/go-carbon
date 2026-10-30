# Measuring shared-store expiration

`BenchmarkExpirationContention` runs the expiration driver against a generated
Pebble store while eight writers offer 5400 one-point updates/second and eight
readers offer 600 five-minute fetches/second. Arrivals use absolute schedules,
so the latency samples include queueing and delayed scheduler ticks.

The fixture uses the actual Whisper continuation codec: ten hours of secondly
samples, three populated archives (`1s:2d,60s:28d,3600s:2y`), and time-bucketed
blocks. Three quarters of the metrics are expired. Names and physical metric
IDs have different orders. The fixture generator copies one encoded metric's
physical keys; foreground updates and fetches use the real codec. This measures
store operations rather than the complete receiver and persister pipeline.

Choose a persistent artifact directory with enough free disk for the fixture
and a disposable copy. A 131072-name fixture currently occupies about 2 GiB;
compaction and temporary copies need additional space. Check free space before
each run. Keep other heavy jobs off the machine when comparing wall time or p99.

```sh
export GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor
export EXPIRATION_BENCH_ARTIFACTS="$HOME/expiration-benchmark"
mkdir -p "$EXPIRATION_BENCH_ARTIFACTS/work.noindex"
export TMPDIR="$EXPIRATION_BENCH_ARTIFACTS/work.noindex"
export EXPIRATION_CONTENTION_SEED="$EXPIRATION_BENCH_ARTIFACTS/seed.json"
export EXPIRATION_CONTENTION_DIR="$EXPIRATION_BENCH_ARTIFACTS/store"

go test ./internal/whisperio -run '^TestPrepareExpirationContentionSeed$' -count=1 -timeout=175s
# Start with an empty fixture directory. Each invocation adds names.
for increment in 1 2; do
    EXPIRATION_CONTENTION_ADD=65536 go test ./internal/chunkstore -run '^TestPrepareExpirationContentionStore$' -count=1 -timeout=175s
done

go test -c ./carbon -o "$EXPIRATION_BENCH_ARTIFACTS/carbon.test"
for workers in 0 1 8 16 32; do
    "$EXPIRATION_BENCH_ARTIFACTS/carbon.test" -test.run '^$' \
        -test.bench "^BenchmarkExpirationContention/workers=${workers}$" \
        -test.benchtime=1x -test.timeout=175s
done
```

`workers=0` disables expiration and measures the foreground reference. Every
case makes and removes its own fixture copy. Use `-benchtime=1x`: each sweep
consumes expired metrics. Default cases stop at a complete sweep or 12 seconds,
then join the foreground workers. Run smaller fixture increments if preparation
approaches the timeout on slower storage.

Optional environment settings:

| Variable | Benchmark default | Meaning |
| --- | ---: | --- |
| `EXPIRATION_CONTENTION_SECONDS` | 12 | Maximum sweep duration |
| `EXPIRATION_CONTENTION_CACHE_MB` | 256 | Pebble block cache size in MiB |
| `EXPIRATION_CONTENTION_MEMTABLE_MB` | 64 | Memtable size in MiB |
| `EXPIRATION_CONTENTION_COMPACTORS` | 2 | Compaction workers |
| `EXPIRATION_CONTENTION_L0_CONCURRENCY` | Store default | L0 compaction concurrency |
| `EXPIRATION_CONTENTION_WARM` | Unset | `1` reads SST pages and primes active metrics before timing |

The benchmark sets scan and delete budgets to 1000000 names/second, removes the
per-sweep delete cap, and disables the soft L0/debt limits to measure throughput.
The write-stall guard remains enabled; a nonzero `guarded` count means pressure
stopped deletion. Do not compare such a partial sweep as an unrestricted run.
Application configuration defaults and the guard correctness tests are separate.

Report deletes/second, completed updates/second, read p99, queued writes and
`guarded`, alongside the worker count and every setting. CPU microseconds/delete
is total process user and system CPU through foreground draining divided by
successful deletes. It includes scanning, foreground operations and background
compaction; setup, warming and store close are excluded. CPU profiles include
setup too, so use call-stack filters to inspect the deletion path specifically.

Use identical fixture contents, cache state, storage settings, offered traffic
and timing for before/after runs. Interleave repeated foreground-only and
expiration cases to establish p99 noise. A single successful warm run does not
establish cold-cache behavior or sustained compaction capacity. Partitioned
workers avoid overlapping mutation stripes, but still share CPU, storage,
Pebble commits and compaction with foreground work.
