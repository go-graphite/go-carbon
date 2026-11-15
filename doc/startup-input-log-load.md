# Startup input-log load qualification

Run this test alone on an isolated **non-production Linux** machine with CPU,
RAM and sustained disk throughput comparable to the target store. Do not use a
production store or a shared, oversubscribed laptop. Unit tests and smoke mode
cannot qualify deployment.

Provide a private, stable copy of a representative input-log directory (at least
1.4 GB of journal segments and 100 million replayed points) and its TOML config.
The test copies the segments, never opens the original for writing, uses private
store/dump directories, and binds its receiver to loopback on an ephemeral port.
All other listeners are disabled. Reserve enough free disk for both journal
copies and stores; the normal application disk safety reserve remains enabled.
Use the release Go toolchain and cgo environment. Record `uname -a`, `lscpu`,
`free -h`, filesystem/free-space information and competing processes with the run.

```sh
STARTUP_LOAD_GATE=1 STARTUP_LOAD_ISOLATED=1 \
STARTUP_LOAD_JOURNAL=/private/fixture/input-log \
STARTUP_LOAD_CONFIG=/private/fixture/go-carbon.conf \
STARTUP_LOAD_OUTPUT=/private/evidence/startup-load.jsonl \
GOTOOLCHAIN=go1.27.0 CGO_ENABLED=1 \
go test ./carbon -run '^TestStartupJournalLoadGate$' -count=1 -v -timeout=40m
```

Each path receives 25 million points per second for 600 seconds over 280 real
TCP connections while a representative journal replays. The first trial uses the
existing capture path, the second uses startup append. Prebuilt native stream
frames include canonical tagged names, repeated corrections and special IEEE
values. Tag parsing is disabled on both trials because legacy early capture
requires that setting; a separate bit-exact regression covers alias normalization. Both trials use identical timestamps and retentions (1s:1d, 1m:7d) so
expiry cannot make the archive comparison empty. Metric names are disjoint per
connection, preserving same-name arrival order across independent trials.

The producer follows a fixed clock in 100 ms windows, independent of write
completion. Every write has a deadline at the end of that window. Missing a
write deadline fails; reducing load when the consumer blocks is forbidden.
Every second the evidence records offered, sent, received and committed points,
direct state, relog count and write errors. Every sample requires intake and
sent >= offered, zero relog/errors/pause, and, after direct, commits keeping up
with intake. Completion requires direct, an empty cache, commits >= total
intake, and actual startup capture. Every stored name, archive retention,
timestamp and IEEE value bit is hashed and compared across the two paths; an
empty live result fails. A result is qualified only when **both trials pass**
and the final evidence verdict is `PASS`.

A small driver check is available on a development machine:

```sh
STARTUP_LOAD_GATE=1 STARTUP_LOAD_SMOKE=1 \
STARTUP_LOAD_OUTPUT=/private/evidence/smoke.jsonl \
GOTOOLCHAIN=go1.27.0 go test ./carbon \
-run '^TestStartupJournalLoadGate$' -count=1 -v -timeout=5m
```

Smoke uses 10,000 points/s, 32 connections and three seconds per path. Its
verdict is always `SMOKE_ONLY_NOT_QUALIFIED`. It allows ten-second socket
writes and checks intake after drain instead of enforcing each one-second
performance threshold; it validates the driver and archive differential only. Retain failure evidence, too. A
passing smoke or local correctness suite does not authorize a canary; report
qualification as pending if the Linux host or representative fixture is absent.
