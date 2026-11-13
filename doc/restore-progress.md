# Legacy restore progress

With `[prometheus] enabled = true`, the diagnostic HTTP endpoint (by default
`:7007/metrics`) reports compressed legacy restore progress before the receiver
gate opens. It uses direct gauges, without starting the ordinary statistics
collector or adding its generated points to the cache.

| Gauge | Meaning |
| --- | --- |
| `restore_loaded_points` | Points admitted from legacy dump files in this startup. |
| `restore_backlog_points` | Queued plus in-flight cache points; popping a write batch does not reduce this to zero. Shards are sampled separately. |
| `restore_persisted_points_per_second` | Average successful point writes per second since loading finished, under the configured store sync policy. Persistence also runs during loading; those writes are excluded from this drain average. |
| `restore_load_seconds` | Time loading dump files, including saved index warmup. |
| `restore_drain_seconds` | Time waiting for persistence after loading. |
| `restore_blocked{reason="load"}` | One while dump loading blocks the receiver gate. |
| `restore_blocked{reason="drain"}` | One while post-load persistence blocks the receiver gate. |

Values are zero when this legacy restore path has not run. At completion,
backlog and blocked gauges become zero; loaded points, durations and average
rate retain the completed restore values until the next startup. These gauges
do not describe the separate pending-checkpoint recovery path. The input gate
and its ordering guarantees are unchanged.

A valid checkpoint manifest has no fixed size limit. Startup capture reads the
whole JSON manifest to find its input generation; ordinary checkpoint opening
still validates the data root, source checksums and saved index before replay.
Invalid checkpoints retain the existing ordered legacy restore fallback.

To reproduce a local executable restart with a legacy dump of 2,097,152 points,
a checkpoint rejected by root validation, and scrapes of `127.0.0.1:7007/metrics`:

```sh
go build -o /tmp/go-carbon-restore .
RESTORE_PROGRESS_BINARY=/tmp/go-carbon-restore go test ./carbon \
  -run '^TestRestoreProgressLegacyProcess$' -count=1 -v
```

The test checks load and drain progress with the receiver gate closed, then
reopens the store and checks every restored timestamp and value. Port 7007 must
be free. `TestStartupInputLargeManifestLegacyRestart` separately exercises a
valid manifest over 1 MiB, legacy dump restoration, pending reads, an inherited
listener and an accepted TCP connection. It expands the manifest with JSON
whitespace so the test does not require hundreds of gigabytes of dump data to
produce that many checksum entries.
