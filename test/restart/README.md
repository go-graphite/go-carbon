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

The test validates graceful recovery, not crash durability or multi-replica
availability. Batching deliberately keeps points in memory longer; a successful
SIGUSR2 test does not prove those points survive SIGKILL or a machine failure.
Likewise a faster local restart does not prove a production deployment can restart
all replicas together. Keep readiness and replica-availability gates in the rollout.
