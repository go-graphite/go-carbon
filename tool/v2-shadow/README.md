# Raw live shadow

`go run ./tool/v2-shadow -source /path/to/classic-input-log -out /fresh/private/root -classic http://127.0.0.1:8080 -duration 20m`

Opens the classic journal O_RDONLY, verifies segment identity and frame/payload CRC before advancing the stateful decoder, and follows only immediate successor segments. The tailer and its adversarial fixture are ported from the earlier immutable-parts shadow diagnostic. Never point `out` at a production store or an existing experiment.

Normalized input goes through `v2daemon` layer 2: durable names, one-second grouped journal sync and bounded pipelined core admission. No shared core implementation is modified. A diagnostic first-seen name map mirrors the serial producer catalog IDs (one-based); samples rotate through every 8192nd observed distinct name, up to 4096 candidates. The serial decoder/catalog and broad snapshot scans limit throughput. This is an experimental harness, not daemon capacity evidence.

Each comparison pins immutable owner snapshots, scans their raw records once for the selected names, and fetches classic protobuf responses via HTTP GET. It compares slot presence and float64 bits, including signed zero and NaN payloads, choosing the greatest original input sequence for repeated exact timestamps. Ranges begin 60 seconds after shadow start and end at wall time minus `-lag` (default 120s). This fixed exclusion is reported, not treated as measured ingest lag or permission to ignore mismatches. A source cursor that has not caught up, or classic correction arriving after the snapshot, leaves mismatches unclassified; rerun identical frozen ranges after convergence before blaming either engine.

Before pinning, the harness flushes and drains the producer and fences its core publication. A differing or failed comparison is retried up to twice at later comparison ticks with the same names and numeric window, using fresh snapshots. JSONL includes candidate IDs/names, attempt number and elapsed time since the first snapshot. Initial differences remain in totals even when a retry agrees. Convergence bounds the observed divergence duration; it does not by itself identify which side lagged or recover original persister application boundaries.

The compared slots are the intersection of the frozen window and the classic response's actual `StartTime`/value coverage. Whisper excludes the requested `from` boundary; a raw point exactly at that excluded boundary must not be counted as missing from classic.

After the writer has stopped, save one complete `comparison` JSON object as `case.json` and run `v2-shadow -out /existing/private/shadow -recheck case.json -shards 4 -classic http://127.0.0.1:8080`. This opens only existing owner databases in read-only mode and reruns that exact frozen query against their sealed contents. It requires the serial producer catalog, refuses missing databases, and emits separate recheck totals. The source cursor is unobserved in this mode; it does not replay missing input or qualify a lag allowance.

The source journal contains receiver groups, not original persister application boundaries or admission clocks. L3 rollup, retention, physical-slot wrap, LastUpdate, historical data and L4 serving parity remain unqualified. Unaligned raw input is reported as an unsupported comparison. Missing API metrics/errors are failures, not successful comparisons. Inspect `compared_names`, `compared_points`, `mismatches`, exact examples, failures, source cursor/caught-up state, frontiers, and process/load context on every JSONL row. Empty comparison coverage proves nothing.

Use external `taskset`, a hard timeout and a process/load observer on shared hosts. All rates are provisional under co-tenant load. Stop only the recorded harness process group and preserve results/checksums before removing its private output. The harness never signals or configures the classic daemon. A fresh output is required for every run; crash replay is owned by the daemon integration.

For bounded real-traffic correctness sampling, use `-sample-every 8192`. Selection
uses stable FNV64a of the journal name and retains every point for selected names;
counts distinguish decoded traffic from admitted traffic. This does not measure
full-stream throughput. At startup the newest segment is decoded to EOF to warm
its dictionary, discarding historical groups before the timed run. Compared
windows begin 60s later. Comparison end is the last observed live-edge time minus
`-lag`; the harness refuses snapshots over 10s since reaching that edge or with
any newer segment outstanding. JSONL exposes both bounds. The external supervisor
must still fence classic PID/start-time changes for the whole run.
