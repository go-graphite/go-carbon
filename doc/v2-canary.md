# Preparing a shadow-first v2 canary

This is a preparation runbook, not permission to start a production canary.
The coordinator must confirm every entry gate for one integrated source commit,
binary and configuration. A library benchmark or sampled shadow does not qualify
the daemon. Record every observation with a CET timestamp and retain failed runs.

The first stage keeps classic authoritative for writes and reads while v2 receives
the same live stream in isolation. Rollback changes the selection back to classic;
it does not convert, replay into, or migrate data between engines. Promotion to
v2-only writes is a separate decision with stronger rollback requirements.

## Entry gates

The numbers below are proposed acceptance limits. They are not measured passes.
The coordinator must ratify them before execution; a missing measurement fails
the gate. Use the same configuration, CPU allowance and policy corpus throughout.

| Gate | Required evidence |
| --- | --- |
| Integrated artifact | Go 1.27.0, CGO enabled, vendored dependencies; build, vet and tests pass for the runtime and tools. Record source SHA, tree, binary SHA256, tool hashes and configuration hashes. The running release source must be an ancestor of the deployable candidate. |
| Rollups versus raw | At least five interleaved equal-corpus L0/L3 pairs, including journal, seal, drain, Close and compaction debt. Complete L3 paired median must be at least 25% of L0, matching the current rollup milestone; 90% remains the optimization goal. The separate sustained live-peak capacity gate must also pass. Report written bytes/point (target at most 10), allocated bytes/point (target at most 300), live SSTs and debt. Every tuple and archive oracle must pass; no deferred compaction presented as throughput. |
| Sustained capacity | At least 30 minutes at 1.25 times the measured 15-minute live ingress peak, with reads and rollups enabled. Pending bytes and compaction debt have nonpositive slopes over the final 10 minutes. Publish ingest, drain and complete rates separately. |
| Reads | Three interleaved zero/four-reader pairs, at least 30 seconds per arm, same four-name windows and load context. Paired median ingest loss at most 5%; each pair at most 10%. Every read arm has p99 at most 5ms and at least 50,000 four-name queries/s, matching the current reader milestone. Also report the matched classic baseline. Production request p99 must stay below 500ms and twice its matched baseline. Report every arm; fixed-file classic is only a read-cost reference. |
| Full-stream shadow | At least 20 minutes, sample-every=1, source age at most 10s and no successor-segment backlog after warm-up. Report EOF and durable/visible frontier lag separately. At least 10,000 distinct names and nonzero compared points; zero unexplained differences or comparison failures. Source age is not the comparison exclusion horizon. |
| L3 parity | Raw and every configured archive step, metadata, policy, physical slot, presence and float bits match the independent classic oracle. Include late corrections, wrap, NaNs, sparse/XFF and original application boundaries/clocks. Sampled L3 correctness and full-stream raw capacity are separate evidence, neither implies full-stream L3 capacity. |
| Fallback modes | The actual daemon supports qualified classic, dual-write/shadow and read selection on the same binary. Classic remains authoritative initially. Synthetic failure, restart and flip-back tests preserve classic reads/writes. Switching modes with retained v2 data must be explicitly supported. Buckyd and every already-enabled endpoint must keep working. |
| Operator metrics | Positive ingest/committed counters; pending bytes, log sync age/frontiers, seal latency/errors, read latency/q/s, part counts and compaction debt; explicit write/read authority and dual-write state. config_unknown_keys must be zero. Missing, stale, NaN or reset signals cannot count as green. Test positive and negative guard cases. |
| Reopen and copies | TCP and known-name find/render/info reopen within 120s. Keep any existing copies running across restart; each previously active copy shows names/s greater than zero within 120s of reads reopening. No pause or drain. |
| Host envelope | Pending budget 64GiB, warning at 80%; MemAvailable at least 256GiB; free disk at least max(1TiB, twice qualified transient working set). No new write/drop/overflow errors after reopen. Baseline and proposed limits must fit the actual host. |

The independent full-stream and L3 oracles must identify their common input and
frontiers. Never reopen an old spool to manufacture traffic. A host with classic
input-log disabled needs a separately authorized same-input source or a qualified
in-process dual writer. A diagnostic journal follower is not daemon shadow mode.

## Artifact recipe

Build on an approved machine in an isolated build directory, using a native
Linux cgo toolchain for the deployable artifact. Follow the coordinator's current
build-location instructions; do not compile on a serving production host. The
following payload also works inside an approved remote-build wrapper:

```sh
set -eu
export GOTOOLCHAIN=go1.27.0 CGO_ENABLED=1
test "$(go env CGO_ENABLED)" = 1
test "$(go env GOVERSION)" = go1.27.0
mkdir -p .rbuild/artifacts
go build -p 4 -o .rbuild/artifacts/go-carbon .
go build -p 4 -o .rbuild/artifacts/v2-shadow ./tool/v2-shadow
go build -p 4 -o .rbuild/artifacts/storectl ./tool/storectl
go vet -p 4 ./carbon ./carbonserver ./internal/v2... ./tool/v2-shadow ./tool/storectl
go test -p 4 ./carbon ./carbonserver ./internal/v2... ./tool/v2-shadow ./tool/storectl
go test -p 4 -race ./carbon -run '^TestV2' -count=1
go version -m .rbuild/artifacts/go-carbon
sha256sum .rbuild/artifacts/go-carbon .rbuild/artifacts/v2-shadow .rbuild/artifacts/storectl
```

Use `-ldflags "-X main.BuildVersion=$CANDIDATE_SHA"` for the deployable binary.
The wrapper may set buildvcs=false, so retain the source/tree receipt separately.
All tools come from the same source tree, never a previously installed binary.
Do not edit vendor. Export and checksum receipts before removing a build directory.

Before producing the deployable artifact in the running release's source lineage:

```sh
git merge-base --is-ancestor "$RUNNING_RELEASE_SHA" "$CANDIDATE_SHA"
git diff --exit-code "$CANDIDATE_SHA" -- vendor/
git rev-parse "$CANDIDATE_SHA^{tree}"
```

An integration-branch binary that fails the ancestry check is a test artifact.
Have the release owner join the qualified changes onto the running release tag
with a merge commit, rerun gates and rebuild; never relabel a test artifact.

## Operator inputs and preflight

Freeze an operator manifest before the window: host, service name, original
ExecStart and cgroup, existing configuration, release/source/tree hashes, classic
roots, new v2 root, empty legacy dump path, artifact directory, shadow and classic
configuration files (cutback means shadow mode), HTTP/metrics addresses, fixed query corpus and main's gate
confirmation. Verify physical path isolation, ownership, available disk and the
existing configuration-management hold. Preserve every existing hold reason.

The fallback owner specifies the following restart-only contract. Verify it in
the landed integrated binary before use; the preparation base predates it.

```toml
[v2]
enabled = true
path = "/absolute/isolated/v2-root"
mode = "shadow"
```

| Mode | Public reads | Writes / rollback |
| --- | --- | --- |
| `shadow` | Classic | Both engines receive the common durable stream; classic stays authoritative |
| `v2-serving-with-classic-dual-write` | v2 plus historical classic | Both engines stay written; restart in `shadow` to flip back |
| `v2-only` | v2 plus historical classic | Classic stops receiving input; a durable finalization receipt closes the flip-back window |

Keep enabled=true and the same root in shadow and serving-dual modes. An empty
mode retains the earlier v2-only behavior; enabled=false is not the flip-back.
The first shadow start needs an empty v2 root. Preserve `classic-fallback.json`
and the common journal throughout the hold. Never edit a finalization receipt to
reopen a stale classic store. The initial implementation rejects buckyd, tags,
classic migration, metadata prewarm and expiry during dual write. Any such live
setting is a launch blocker, not permission to disable an existing capability.
Serving-dual also rejects unseeded historical multiarchive overlap before opening
its read listener; a passing single-archive rehearsal does not qualify that case.

Preflight commands, with manifest values supplied explicitly:

```sh
date '+%F %T %Z'
systemctl show "$SERVICE" -p MainPID -p ExecStart -p ControlGroup -p KillMode
readlink -f "/proc/$(systemctl show "$SERVICE" -p MainPID --value)/exe"
sha256sum "$ORIGINAL_CONFIG" "$SHADOW_CONFIG" "$SERVING_DUAL_CONFIG" "$CANDIDATE_BINARY"
"$CANDIDATE_BINARY" -config "$SHADOW_CONFIG" -check-config
"$CANDIDATE_BINARY" -config "$SERVING_DUAL_CONFIG" -check-config
df -B1 "$CLASSIC_ROOT"
grep -E 'MemTotal|MemAvailable' /proc/meminfo
curl --fail --silent --show-error --max-time 10 "$METRICS_URL"
```

`-check-config` validates parsing; it is not proof that Start accepts a mode or
that a root can reopen. Unknown TOML keys can be warned about without rejecting
startup, so inspect warnings and require config_unknown_keys=0. The isolated
rehearsal must start both configurations and verify their active-mode metrics.
Sample metrics every 5s for a 15-minute baseline. Capture counter epochs, actual
TCP ingress, read q/s/p99, classic commits and cache, available memory/disk and
copy progress. Require a stable final ten-minute pending/debt baseline. Preserve
baseline failure/drop counters; a pre-existing nonzero total is not a new loss.

## Shadow start and manual flip-back

The following is a future operator sequence. It is not run by preparation.
The same candidate binary and existing service supervision are used for both
shadow and classic. Install the qualified candidate/configuration through the
release owner's reviewed single-host mechanism, preserving the original command
arguments and permissions. Do not overwrite a shared release tree in place.

1. Main records that all entry gates pass for the exact artifact manifest.
   Verify the host and process identity again, hash the hold, and arm signal-only
   observations. Record any already-active copies and their individual counters.
2. Stage an empty, physically disjoint v2 root. Preserve the classic store. Check
   the full diff between original and shadow configuration: same ingress/read
   addresses, classic authority and working maintenance/copy endpoints. No relay,
   placement-map or upstream membership change belongs to this step.
3. Cold-stop the supervised service. Wait for the old parent and every child to
   exit and for its captured cgroup to become empty. If they remain alive, stop
   here; do not start another writable owner.
4. Verify the legacy dump is the exact manifest path and not a symlink or any
   classic/v2 data root. Delete only that stopped service's legacy dump. Start
   cold, without legacy dump/spool replay or backfill. Record the restart gap and pending
   classic points abandoned by this policy; never call them persisted.
5. Select the staged shadow configuration and candidate, then start the same
   service. Record PID/start epoch, actual binary hash and mode/authority metrics.
   Classic remains the public read and write authority; v2 receives the identical
   fresh stream through the qualified fallback interface.
6. Check immediate readiness, then minute 3, 20 and 30 gates below. Report to main
   by minute 3. Do not enable v2 authority or widen the fleet from this runbook.

After a green 30-minute shadow window, main may separately confirm a read-only
selection change to v2 while **both engines keep receiving writes**. First prove
that historical multiarchive boundaries, discovery and quotas remain correct on
the integrated binary. Observe another complete 30-minute window under the same
guards and compare both read paths behind their measured common frontier. A
failed v2 read selection flips back to classic on the same dual-writing binary.
Keep classic writes for at least 24 hours, including an observed busy period,
before considering a separately approved v2-only change. The hold is a proposed
minimum, not evidence that every retention/history case has been qualified.

Use only the transition mechanism qualified by the fallback owner. Until reload
is proven, mode changes require the cold-stop/start sequence above. If a future
qualified transition uses SIGHUP, check the service log immediately for
`config reload failed` and verify the active-mode metric; a successful signal
command does not prove the configuration changed.

To flip back, first capture the alert, frontiers, error deltas and configuration
hashes. Manually stop this candidate, verify all its processes are gone, apply the
tested `mode="shadow"` configuration on the **same binary**, remove only the verified
legacy dump while stopped, and cold-start. Check classic reads, fresh TCP writes
and copy resume. Preserve v2 log/parts and all evidence. There is no data migration,
no ad-hoc data re-import, no backfill and no deletion of either store. Normal
restart recovery through the candidate's own mandatory common journal is the
qualified storage path; it is distinct from feeding an old classic spool.

If a later phase stops classic writes and makes v2 the sole write authority, this simple
rollback no longer proves preservation of the v2-only interval. That phase needs
its own verified recovery/catch-up and read-union plan before starting. A classic-only old
binary cannot be used to hide retained v2 history.

## Observation windows and guards

Every guard only prints/persists `ALERT` with a timestamp, artifact/config identity
and observed value. It never restarts, changes selection, signals the daemon,
changes configuration or holds, pauses copies, deletes data or routes traffic.
Missing samples signal `UNKNOWN` and block promotion. A guard's own timeout may
stop only its own isolated rehearsal process, never the production service.

| Window | Signal / limit | Operator action |
| --- | --- | --- |
| Every 5s | Unexpected process epoch; unavailable scrape; missing authority/frontier metric; negative counter delta | Signal immediately; distinguish deliberate restart/reset from an unknown interval. Never treat missing as zero. |
| First 120s | Known-name find, info and render must return HTTP 200, parseable and expected nonempty data; TCP fresh writes become readable | If absent, signal and investigate/manual flip-back. A 200 empty response fails. |
| 120s after reads reopen | Previously active copies each resume names/s greater than zero | Signal each stalled copy; keep copies running. |
| After reopen | New classic or v2 write/sync/rollup errors, overflow, drops, parity mismatches: zero | Signal immediately and freeze expansion. Preserve restart-window deltas separately. |
| After minute 3 | Eligible live sync/seal frontier age at most 2s; source age at most 10s and zero segment backlog | Signal; record admission clocks, source EOF and commit frontiers separately. Event timestamps do not measure ingest lag. |
| Every 5s | Pending at least 51.2GiB: warning; 64GiB: alert; MemAvailable below 256GiB; free disk below max(1TiB, 2x working set) | Signal the breached limit. Any shadow failure must remain visible while classic stays authoritative. |
| Two consecutive 5s samples | Production read p99 greater than 500ms, or greater than twice the matched baseline; q/s below 90% at matched offered load | Signal. Report low request counts and offered-load changes; low traffic cannot prove p99. |
| Minute 3 | Correct authority/mode, no new loss, readiness and copy-resume receipts | Report exact status to main; no automatic next stage. |
| Minute 20 | Full-stream and raw/L3 parity gates; zero unexplained differences, failures or missing coverage | Keep sampled and full-stream results separate. Do not widen horizons or waive classic quirks. |
| Minute 30 | Every required gate passes; final 10-minute pending/debt slopes nonpositive | Report complete evidence to main. Only main decides whether to proceed. |

Initial operator metric names proposed by the metrics owner include
`v2_ingest_points_total`, `v2_committed_points_total`, `v2_parts{level=...}` and
`v2_compaction_debt_bytes`. These names and the remaining seal/read/authority
series must be verified against the final binary. An always-zero dual-write gauge
from the sole-writer path is not evidence of implemented shadow mode.

## Isolated rehearsal and receipts

Run on the currently approved rehearsal host, only within its own scratch
directory (or `build-<unique-name>` when using the remote-build wrapper).
Use temporary synthetic classic and v2 roots,
loopback ephemeral listeners, copied synthetic policies, and the same artifact
and mode configurations intended for the canary with paths/ports substituted.
Never open or change the running daemon's roots, log, config or service.

Execute and record: configuration checks; classic start; fresh synthetic writes
and classic reads; shadow start; identical live input to both engines; raw and
archive comparisons; a signal-only negative guard case; manual flip-back;
continued classic writes/reads; shadow reopen with retained v2 history; final
guard output. Include NaN/presence and correction cases in the existing parity
tests. No spool is a rehearsal input. A sole-writer test must be labelled as such.

Each receipt records CET start/end, wall time, exact command, source/tree and
binary/config hashes, exit status, expected/observed mode, guard state and output.
Verify the test runner actually ran the named tests (no empty-regex success).
Retain health observations before/after and verify no owned process remains before
scratch cleanup. Copy/checksum the transcript first. DNS failure, unavailable
tooling or missing shadow mode is `BLOCKED`, never a successful dry run.

## Preparation rehearsal status

A native Linux Go 1.27.0 cgo diagnostic rehearsal of the fallback candidate
completed classic -> shadow -> serving-dual -> shadow -> shadow restart using
one synthetic single-archive metric. It checked historical and fresh reads,
corrections, retained history, discovery/info, one-hot mode, mirrored points and
unknown configuration keys. A deliberately wrong expected mode raised ALERT
without changing the process. Signed-zero verification used the binary response;
the JSON response emitted integer zero and could not establish its sign bit.

This exercises transition mechanics only. An earlier classic-shadow override test
failed because its fixture reused the query cache. With fixture query caches
disabled, independent native build/vet, fallback/journal tests and focused races
passed, followed by a second successful transition rehearsal. This test-only
correction does not change production cache settings.
The rehearsal does not establish full-stream/L3 parity, multiarchive historical
cutover, production endpoint compatibility, sustained capacity or full operator
guards. Repeat on the final integrated release artifact after every blocking
component has passed. Private receipts retain exact source/tree, hashes, commands,
CET timing, earlier failed attempts and guard output.

## Complexity check

[VictoriaMetrics mergeset](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/lib/mergeset/table.go)
pins part references for reads and snapshots, flushes in-memory data and hard-links
immutable files for snapshots. [ClickHouse MergeTree](https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/MergeTree/MergeTreeData.cpp)
publishes part state and retires outdated parts; replicated part checking belongs
to its replication layer. Follow those boundaries: existing immutable parts,
snapshot pins, whole-part checksums and the existing daemon mode selector. This
runbook adds no replication coordinator, per-name digest, auto-revert daemon or
second storage path. Dual writing is a bounded qualification stage needed to keep
classic authoritative while testing the candidate; remove it only after a separate
authoritative-write rollout and rollback decision.
