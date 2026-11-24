# replica-backfill

Compare local carbonserver data with a source and send only missing slots to the
local plaintext receiver. Use `-help` for pacing, cache guard, checkpoint, OTLP
and HTTP replica options.

## Offline dump source

`--source-dump FILE` replaces `--replica URL`. Repeat it for ordered donor
fallback, just like repeated HTTP replicas. Plain JSONL and gzip are detected by
content, with no filename requirement. The source file can be unsorted. Continue
to use `--names-file` to select which local metrics to compare; dump names do not
replace that selection. `--lookback 0` includes names absent from the local host.

The dump format comes from the read-only stopped-store reader in
[PR #1734](https://github.com/go-graphite/go-carbon/pull/1734). Request explicit
point ranges from that reader: its summary-only output cannot fill gaps. Each
line is one metric, for example:

```json
{"name":"example.metric","archives":[{"step":60,"count":1440,"points":[{"t":1700000040,"bits":"3ff0000000000000"}]}]}
```

`step` is seconds per point, `count` is the archive's retention slot count and
`bits` is the exact float64 bit pattern as 16 hexadecimal digits. Additional
reader fields (`last`, `last_update`, `zero`, `nonzero`) are ignored. Missing
names, `{"name":"example.absent","error":"not found"}`, and archives with no
requested points count as no source data. Other reader errors and malformed or
duplicate rows fail validation before ingestion starts.

The adapter selects the archive matching the local render step. When there is
no local series, it selects the finest archive covering the requested start
relative to run time, or the coarsest archive for older ranges. It never
upsamples or synthesizes points from a different archive. The existing engine
checks matching steps, detects gaps, keeps local values authoritative, and
handles ingestion, cache pause/resume, retries and checkpoints unchanged.

Each input is fully validated and copied to a private temporary file before
writing begins. The in-memory index stores names and byte offsets, not points;
concurrent batches read individual rows with `ReadAt`. Allow temporary disk
space for the uncompressed inputs; `TMPDIR` chooses its location. Temporary
files are removed on normal exit and handled errors. A row is limited to 64MiB
and a rendered metric to 8M slots; use narrower dump ranges/windows if needed.
Memory scales with indexed names, the largest row and concurrent batch data,
rather than all dumped points. Dump requests/bytes/timing appear in the existing
per-source report under `dump:FILE`; `--replica-rps` applies only to HTTP sources.

```sh
replica-backfill \
  --source-dump donor.offline.jsonl.gz --names-file names.txt \
  --from 1700000040 --until 1700003640 --lookback 0 \
  --local http://127.0.0.1:8080 --ingest 127.0.0.1:2003 \
  --heartbeat-file /path/to/go-carbon-heartbeat.json \
  --cache-pause 0.75 --cache-resume 0.60 \
  --checkpoint /path/to/unique-donor-window.checkpoint.json --dry-run
```

Review the dry run, then omit `--dry-run` to ingest. The guard needs the running
local go-carbon's configured `common.heartbeat-file`. Use a new checkpoint when
changing the window, names or source dumps: checkpoints retain the existing
window/name semantics and do not fingerprint source contents. Multiple donors
retain the existing ordered fallback semantics: a donor with some usable data
for a name answers that name; later donors are tried only when it has no usable
data, rather than unioning partial series.

## Local performance checks

```sh
nice -n 19 go test ./tool/replica-backfill -run '^$' \
  -bench BenchmarkSourceBackfill100K -benchtime=3x
```

The benchmark compares 100k-name dry runs from a synthetic dump and a local
`httptest` HTTP donor, with the same local HTTP server and two gaps per name.
It includes indexing/validation, local render, gap detection and reporting,
excludes fixture creation and TCP ingestion, and disables pacing. These numbers
measure local processing, not production ingest capacity.

To measure peak RSS without including Go compilation or fixture generation,
build the test binary and run `TestDumpMillionNameRSS` with `BACKFILL_RSS_DUMP`
pointing at a synthetic 1M-name dump. The probe indexes every name and compares it with a local HTTP render in
500-name batches; `/usr/bin/time -l` on macOS or `-v` on Linux reports peak RSS.
