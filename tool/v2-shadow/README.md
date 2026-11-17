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

Use `-capacity-only -sample-every 1 -compare-interval 1s` for a full-stream
admission run without broad parity scans. Each sample reports backlog bytes and
successor segments, elapsed time since the last observed live EOF, and self CPU
counters. EOF age measures source freshness, not per-point end-to-end latency.
Measure the classic receiver counter over the same window externally. Capacity
mode can succeed without comparisons; it never implies read parity.

Use `-layer 3 -sample-every 256 -sample 192` for a separate sampled L3 run. Policies
come from the classic read-only `/info` API, including tags, retention ladders,
aggregation and XFF. Receiver groups are reconstructed applications using the
shadow admission clock; the source journal does not carry classic persister apply
boundaries/clocks. This mode cannot certify original application or metadata
parity. It emits each policy for a schema/method census and per-name archive
coverage. The sampled L3 run must not be presented as full-stream L3 throughput.

L3 comparisons pin immutable core snapshots and use `RollupSnapshot`. A classic
lookback beyond each finer retention selects its next archive; returned step is
checked explicitly. Only completed intervals strictly inside the fresh run count.
Archives with no such interval contribute no coverage. Values, presence and
float64 bits are compared with the same endpoint rules as raw checks. Every
initial difference/failure stays in totals; reconstruction ambiguity is never a
successful parity result. Compare the policy census and tagged/sparse/dense
coverage against the intended population before making a coverage claim.

To remove synchronous policy discovery from a measured L3 window, first collect
`policy` receipts in a discovery run, save their exact canonical objects as a JSON
name-to-policy map, then start a fresh run with `-policies policies.json
-sample-every 1`. Only that fixed population is admitted. Report discovery and
measurement separately, including the actual observed policy population and
missing strata. File-size backlog counters in older receipts include preallocated
space; the current field explicitly labels that upper bound. Successor-segment
counts and live-EOF freshness must also be inspected.

Frozen rechecks also accept saved `l3_comparison` objects from a stopped L3 root.
The read-only recheck preserves names/windows and emits each completed slot's
presence and value bits, so convergence can be checked against the initial
examples. It does not reconstruct missing classic application metadata.

Partition-parallel admission uses `-workers 16 -capacity-only -layer 2`.
Each source partition stays on one decoder and one producer-local catalog. The
bounded frame queues retain original bytes; the existing `SegmentReplay` checks
payloads and dictionary continuity through a decoder-local `ReaderAt` view.
Metric groups are submitted together rather than expanded into per-point
applications. The source is opened read-only, including during warmup.

For a fixed policy cohort, use `-workers 16 -layer 3 -policies policies.json`.
Single-archive policies compare their finest archive too; multi-archive policies
compare completed coarse intervals. Receiver groups and current clocks remain a
reconstruction, so every difference requires original classic application
metadata or measured classic convergence to the exact frozen v2 bits.
`-classic-receipts differences.json` reobserves an array of saved differences
through the classic read API without opening a v2 store. Each entry may specify `step`; omitted steps retain the legacy 60-second
archive choice. The policy read API determines the lookback needed to select it. The receipt records both the original expectation
and new observed value/presence, and never waives an unresolved difference.

`offered_points` counts validated source frame headers dispatched by this
reader; `admitted_points` counts points accepted by the private daemon. Live
classic intake must also be measured independently from its bracketing counters:
a lagging reader's own offered count is not the full offered stream. Source lag
is time since the scanner last reached logical EOF, not a per-point append clock.
Report pending admission and actual source backlog beside this proxy. All
shared-host measurements require CPU affinity and co-tenant context.

Use `-recheck-quiet` for large frozen cohorts: all differences remain, while
equal-slot observations are omitted from the receipt to bound output size.

Full-stream L3 admission is explicit: `-workers 16 -layer 3 -full-l3
-capacity-only`. Optional `-policies` seeds the exact-name cache; every other
name requires its stored policy from batched read-only `/info` requests. Missing
or invalid metadata fails admission; there is no inferred or default policy.
Policy discovery is part of the measured cost. This mode qualifies admission
only; rotating parity comparisons remain a separate deliberate cohort.

Parallel runs independently observe complete source frames, validating header
and payload CRCs without decoding or writing the source. The journal contains
no per-frame append clock. Each frame's append-completion time is conservatively
bounded by the last unsuccessful read (while no successor exists) and the first
CRC-valid read. Across rollover the earlier lower bound is preserved. A bounded
stamp table fails rather than silently dropping observations. The point-weighted
`append_to_visible` histogram reports lower and upper p50/p99 bounds in 10ms
buckets, through a synchronous core publication fence after producer sync.
Only an upper p99 below the target with zero unbounded points can qualify the
latency gate. The overflow bucket means greater than 120s; it is not an exact
latency. The source observer has its own point census and EOF frontier in every
sample. `source_lag_seconds` remains the older scanner-EOF proxy.

`skipped_points` is separate from `admission_pending_points`; fixed-policy
filtering no longer appears as producer debt. All reported shared-host results
remain provisional and include co-tenant load.


Full-stream L3 can use read-only copies of the classic daemon's creation
policies. Read their paths from its running configuration, preserve both files,
and pass them together:

```sh
v2-shadow -source /path/to/input-log -out /scratch/fresh-l3 \
  -workers 16 -shards 4 -layer 3 -full-l3 -duration 20m \
  -classic-schemas /scratch/storage-schemas.conf \
  -classic-aggregation /scratch/storage-aggregation.conf \
  -policies /scratch/comparison-cohort.json
```

The persister's schema priority and aggregation matching select every name's
creation policy, including names that classic has not created yet. This mode
does not claim to recover historical policies from existing files. With
`-full-l3`, `-policies` supplies the rotating comparison population; every input
name is admitted and the paired files override policies in that population.
Without the paired files, full L3 retains strict classic `/info` lookup.

Parallel samples report `stage_timings`: input-tail, admission (including
worker queue/decode/policy/producer work), admission-buffer wait until a fence,
and owner seal phases. The first three are point-weighted; `seal_total`,
`rollup_plan`, and `publication` are operation-weighted owner seals.
`publication` includes part encoding, sync, ingest, and commit after planning.
Seal total contains rollup planning and publication. Concurrent stages overlap;
percentiles cannot be added or subtracted. Append-to-visible remains the
conservative point-weighted source-append to synchronous publication bound.

`offered_points` (also `source_dispatched_points`) counts the consumed source
header prefix. It can understate live arrivals when admission falls behind.
Use `source_observed_points_delta` for the independent observer's arrival
window and report its cursor/window alongside the admitted rate.
