# Restart load test

This Docker Compose test runs a real go-carbon process with compressed Whisper,
flock, realtime indexing, and graceful SIGUSR2 shutdown. It sends deterministic
TCP samples while issuing HTTP reads, fills timestamp gaps with late samples,
restarts the process, and checks **every sent value** before restart, after
readiness, and after the restored cache drains. Internal interval statistics go
to a separate observer, avoiding feedback into the test database.

Build baseline and candidate binaries from separate checkouts with the same Go
toolchain. Set GOARCH to the Docker engine's architecture (`arm64` or `amd64`):

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -mod=vendor -o /tmp/carbon-candidate .
python3 test/restart/run.py --binary /tmp/carbon-candidate \
  --output /tmp/carbon-default-1 --label default \
  --metrics 10000 --seconds 45 --drain-before-restart
python3 test/restart/run.py --binary /tmp/carbon-candidate \
  --output /tmp/carbon-batched-1 --label batched \
  --metrics 10000 --seconds 45 --drain-before-restart \
  --min-points 8 --max-delay 2s
```

Repeat in alternating order with unique output directories. Run one variant at
a time, without competing load on the Docker VM. The store has two CPUs and
1536 MiB memory; `GOGC=100` and `GOMEMLIMIT=1200MiB` by default. `--gogc` allows
an independent GC experiment. Ports bind only to localhost (18080, 12003,
17007 and 18090). Each run removes only this Compose project's containers and
data volume, preserving its result JSON, effective configuration, and logs.
Use `--project NAME --port-offset N` to isolate concurrent runs; N offsets
all four ports. Baseline and candidate comparisons should still run sequentially.

For recovery with a disk-write backlog, omit `--drain-before-restart` and set
`--max-updates 1000`. Verify the saved store log reports a nonempty dump/restore;
an empty-cache restart does not validate backlog recovery. TCP has no application
acknowledgement: the runner observes receiver counters after quiescing the sender
before initiating the restart. It rejects cache overflow and realtime-index
enqueue failures rather than silently treating those as accepted data.

`--names-file PATH` accepts one unique metric name per line, allowing a private
production name-shape fixture. Keep such fixtures, logs, configurations and raw
profiles outside public source control. Values and arrival cadence are synthetic;
matching name shapes alone does not reproduce production traffic or disk behaviour.

Use `pre_restart_resources.cpu_seconds - initial_resources.cpu_seconds` to compare
CPU through full drain when that option is enabled. `load_resources` alone can
exclude deferred writes and overstate a batching gain. Counters and read latency
summaries are also retained, split between load, restart and recovery. Process
RSS high-water and cgroup peak memory are separate measurements: the latter
includes filesystem page cache. The read workload continues during restart;
connection failures from a single unavailable process are expected and reported.

The ordinary run validates graceful recovery. Use the SIGKILL mode below for
crash recovery. Neither mode validates multi-replica availability. Batching deliberately keeps points in memory longer; a successful
SIGUSR2 test does not prove those points survive SIGKILL or a machine failure.
Likewise a faster local restart does not prove a production deployment can restart
all replicas together. Keep readiness and replica-availability gates in the rollout.

Pass `--require-snapshot` when validating the saved-index path. The harness waits
for a complete accelerator before restarting and verifies that the new process
actually used it. Retained values are checked again after readiness and after
persistence drains, just as in the ordinary restart run.

Add `--crash-recovery` with a nonempty backlog and `--require-snapshot` to
kill the new process while it is serving saved points but intake is still closed.
The test verifies the recovery gate was still active, starts another process on
the same volume, and checks every value again before and after persistence.

Use `--sigkill` for a real crash under continuous input, with input logging,
a 30-minute writeout delay and out-of-order writes disabled. Run the same command
without `--background-replay` for the blocking baseline, then with it for the
candidate. `--points-per-second` is a target rate; the result records achieved
throughput as well as the longest sender gap and the application's input-gap
metric. For example:

```sh
python3 test/restart/run.py --binary /tmp/carbon-candidate \
  --output /tmp/carbon-crash-background --label crash-background \
  --sigkill --background-replay --metrics 10000 --seconds 4 \
  --points-per-second 2000000
```

The sender keeps retrying failed batches with identical values through SIGKILL.
Every completed-send sample remains in the oracle: TCP has no durable
acknowledgement, so unread socket bytes and unsynced journal data can be lost.
The result reports `lost_points` separately from incorrect retained values
(`changed_points`, which fails the run), plus replay count/time and resource
peaks. Zero changed values does not mean zero crash loss. Compare baseline and
candidate loss counts as well as the input gap. The run requires a nonempty
journal replay and drains both history and live input before checking all values.
The deterministic ordering and double-crash Go tests additionally cover writes
at the same timestamp, including out-of-order writes disabled.
