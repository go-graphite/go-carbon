# Receive and persistence accounting

A point is one input timestamp/value pair, not a unique archive slot. TCP's
`metrics_received_tcp_total` counts parsed network input before cache admission.
It excludes local self-metrics, other receiver protocols, restored history and
input replay. During startup capture, network receipt can precede cache intake.
Malformed protocol input rejected before receipt is outside this balance.

The cache counts live intake in `cache_received_points_total` and previously
accepted input in `cache_recovered_points_total`. Recovery includes dumps,
input journals, startup capture and immutable pending history claimed by a live
write. Neither counter counts a failed persister batch being requeued.

## Point fates

All counters below are cumulative and use points. Prometheus exports the names
shown. Graphite exports the same suffix below `carbon.<instance>.cache` or
`carbon.<instance>.persister`, following the configured Graphite prefix.
`cache_points` is Graphite `cache.size`; it counts queued points, excluding
popped batches. `cache_in_flight_points` is Graphite `cache.in_flight_points`.

| Fate or source | Prometheus metric | Balance treatment |
| --- | --- | --- |
| Live input | `cache_received_points_total` | Left-hand input, all cache entry APIs including local metrics |
| Recovered input | `cache_recovered_points_total` | Additional input; subtract from fates when balancing only live receipt |
| Successful persistence | `persister_committed_points_total` | Entire input batch, once across single, batch, transfer flush and mirrored writes |
| Queued | `cache_points` | Signed change in gauge |
| In flight | `cache_in_flight_points` | Signed change in gauge; retry moves points back to queued |
| Quota/throttle rejection | `cache_throttled_points_total` | Actual enforcing rejection, including throughput, usage and creation quotas |
| Capacity rejection | `cache_overflow_dropped_points_total` | Point or byte capacity exceeded |
| Invalid tag/name normalization | `cache_invalid_tag_rejected_points_total` | Every point in the refused batch |
| Intake name too long | `receiver_name_too_long_rejected_points_total` | Graphite `cache.name_too_long_rejected_points_total` |
| Backend name/path too long | `persister_name_too_long_dropped_points_total` | Disjoint from intake refusal |
| Graceful diversion | `cache_xlog_diverted_points_total` | Successfully handed to shutdown buffering, outside the live cache |
| Failed diversion | `cache_xlog_write_failed_points_total` | Entire affected batch; a partial write can have reached buffering |
| Permanent file-error discard | `persister_write_dropped_points_total` | Failure to remove a broken empty Whisper file |
| Failed backend attempts | `persister_write_failed_points_total` | Diagnostic only; repeated attempts count again and remain unfinished |

Name-length Graphite metrics use `name_too_long_...` while their Prometheus
names retain the established receiver/persister prefixes. Legacy Graphite
`cache.overflow` and `persister.committedPoints` are interval counts;
**do not differentiate them**. The new `_total` metrics never reset on `Stat`.
Their Prometheus counters reset on component recreation, so rate calculations
must handle resets and balance windows should exclude process/config epochs.

No-schema or no-aggregation input remains queued or is requeued; it is not a
terminal filter. There is no blacklist/short-retention filter in normal cache
intake: copy-tool selection happens outside this boundary. Protocol validation
before successful receipt is likewise not a fate of a received point. Cache
compression retains repeated timestamps. Archive alignment, overwrites,
coalescing, too-old/future/invalid timestamps and out-of-order handling are
included in successful input-batch commitment. They must not be subtracted
again. `pebble_continuation_invalid_points_total`, Whisper out-of-order metrics
and storage expiration metrics describe storage disposition after that boundary,
not additional fates. Later metric expiration/deletion does not undo commitment.

## Balance

For one component lifetime and a common interval:

```text
live cache receipt + recovered input
  = committed input + change(queued + in flight)
    + throttle + capacity + invalid tags + intake name refusal
    + backend name refusal + successful diversion + failed diversion
    + permanent file-error discard
```

Thus, when showing live receipt alone, recovered input is a **negative**
source adjustment. Successful writes with identical timestamps still count
all submitted points. Failed writes that are requeued are counted in queued or
in-flight gauges until a later successful write; adding their attempt counter
would double-count them.

Input-log writes, re-logging and failed durability attempts do not remove points
from cache. `input_log_unlogged_points_total` describes missing crash protection,
not a live loss. Startup capture's captured/replayed totals describe movement
before/into the cache: use the recovered-input counter for this balance, rather
than adding those totals a second time. Direct raw store imports bypass cache
receipt and are outside this identity.

The legacy backend counters count backend operations, which can include writes
before a later mirror-repair retry; summing mirrored backend counters also
counts both copies. Prefer the unified committed-input counter for a balance.
Without the new counters, TCP receipt, existing backend totals, cache size and
name-refusal counters can show only a partial balance. An unexplained remainder
is evidence of missing measurements, not evidence of archive coalescence.

Scrapes sample independently, so brief residuals around large batches are
expected. Compare common windows long enough to include several reporting
intervals, exclude counter resets, and retain signed cache changes. Partial
failed diversion writes and crashes can cross process boundaries; this is an
input-accounting identity, not a proof that every committed point was fsynced
or remains a distinct stored value indefinitely.
