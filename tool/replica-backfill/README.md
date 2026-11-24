# replica-backfill

Fill donor-present holes on a local go-carbon store. The tool compares exact
metric names at the returned archive resolution, retains existing local values,
and sends missing points to the local plaintext receiver.

```sh
go build -o replica-backfill ./tool/replica-backfill
./replica-backfill \
  -local http://127.0.0.1:8080 -ingest 127.0.0.1:2003 \
  -replica http://replica.example:8080 \
  -from 2026-11-10T12:00:00Z -until 2026-11-10T13:00:00Z \
  -names-file names.txt -lookback 0 \
  -write-rate 100000 -checkpoint replay.json
```

Use `-help` for all options. `-dry-run` compares without writing and ignores
checkpoints. Without `-names-file`, names come from the local `/metrics/list/`.
Replicas can be repeated or comma-separated and are tried in order. A positive
`-lookback` skips metrics with no local activity in the lookback or gap window.

## Receiver confirmation and quota drops

**TCP success is not persistence.** The receiver can accept bytes and then drop
points to namespace throughput quotas, including existing names under a
`dropping-policy=new` quota. Cache occupancy checks alone cannot detect this.

Every write batch now waits for a local `/render/` read to return each submitted
timestamp and its exact value before completing its checkpoint. Confirmation
uses unique v3 `PathExpression` values, which form part of both go-carbon render
response cache keys; metric `Name` and time range stay unchanged. This avoids
reusing a cached pre-write response. Reads use the existing `-local-rps` limiter
and HTTP retry/timeout settings.

If points remain missing or different, the tool logs `ERROR`, retains the batch
outside the checkpoint, halves the shared write rate (minimum 1 point/s; an
unlimited rate first becomes 50000 points/s), backs off and rewrites only those
points. The socket burst remains bounded by `-flush-points`; choose a small flush
size as well as a low rate for small namespace quotas. Retries consume
`-max-write-points` just like initial sends and reconnect retries.

| Flag | Default | Meaning |
| --- | --- | --- |
| `-confirm-delay` | `1s` | Wait before the first read; double on missing points up to `-max-backoff` (default `30s`). Also wait this backed-off delay before resending. |
| `-confirm-retries` | `20` | Maximum retries after the initial send/read. Zero still requires confirmation. |
| `-confirm-timeout` | `10m` | Deadline per batch, including writes, rate limiting, readback and backoff. |

Exhausted retries, deadline, or failed confirmation reads stop the replay with
exit code **1**, an `Error` in the final JSON report and a failed batch count.
Configuration errors exit **2**. Successful recovery can exit **0**, with the
retry counts preserved. Signals leave unconfirmed batches available on resume.
Use the same window, names, selection flags and checkpoint to resume; confirmed
out-of-order batches may be repeated behind an unfinished checkpoint prefix.

Version 1 checkpoints recorded only TCP success and are rejected. Start a new
checkpoint and rescan the full original window when upgrading; a completed old
checkpoint may already hide quota losses. Receiver quota defaults and all
metric-count/new-name quota policies remain unchanged. Confirmation is mandatory
on write runs; there is no unsafe opt-out.

This guard deliberately does not infer replay losses from the aggregate
`cache_throttled_points_total` counter. Unrelated live quota drops cannot trigger
retries when replay values are readable. Neither that counter nor `/admin/quota`
provides a per-batch acknowledgement: counter polling has a race with queued TCP
input, and quota headroom can be consumed by concurrent traffic. Readback tests
the actual replay data instead. Missing data may also mean admission delay,
another quota, retention, an incompatible archive resolution, or a stale storage
read cache. These can cause conservative retries or a bounded failure; they are
not reported as an exact number of quota-dropped points. Align `-local` and
`-ingest` to the same store and size the confirmation budget for its visibility
delay. Lower the replay rate/flush size if retries persist.

## Report and metrics

The final JSON `Write` object includes:

| Field | Meaning |
| --- | --- |
| `points_sent` | Submitted points whose exact values have been read back locally. This replaces the old socket-only meaning. |
| `points_written_socket` | Points successfully written to TCP, including resends; does not imply admission. |
| `points_attempted` | All socket attempts, including ambiguous reconnect retries and confirmation resends. |
| `points_unconfirmed` | Submitted points not yet confirmed; includes points left by failed or interrupted batches. |
| `unconfirmed_point_observations` | Missing/different values observed during confirmation. Repeated observations count again; this is not an attributed quota-drop counter. |
| `resent_batches` / `points_resent` | Confirmation retry rounds / points queued for resend. |
| `confirmation_failures` | Batches that failed receiver confirmation, including interruption. |

`Metrics.failed_batches` and `Error` describe unsuccessful runs. OTLP exports
`replica_backfill_points_sent` with the confirmed meaning and adds
`replica_backfill_points_unconfirmed_observations`,
`replica_backfill_resent_batches`, `replica_backfill_points_resent`,
`replica_backfill_confirmation_failures`, and the
`replica_backfill_points_unconfirmed` gauge.

## Required persistence verification

Readback may include go-carbon's unpersisted cache. **A verification dry run
remains required** after the replay has drained to storage and the deployment's
read/query caches have expired. Use the same window and selection with
`-dry-run` and no checkpoint, and require `fillable_points=0`,
`failed_batches=0` and no unexpected step mismatches before declaring repair
complete. The example below uses the same explicit bounds as the replay:

```sh
./replica-backfill \
  -local http://127.0.0.1:8080 -replica http://replica.example:8080 \
  -from 2026-11-10T12:00:00Z -until 2026-11-10T13:00:00Z \
  -names-file names.txt -lookback 0 -dry-run
```
