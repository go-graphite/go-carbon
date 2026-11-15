# Startup capture group commit

Startup append receivers release borrowed batches as soon as the input-log encoder
has consumed their columns and copied their names. `Log.StageGroup` returns an
`AppendCompletion`; returning from Stage is an ownership boundary. Only
`AppendCompletion.Wait` returns a durable generation/tail receipt. This follows
the Stage/Sync separation explored in #1386 while retaining the existing journal
format and replay machinery.

The writer syncs staged appends every second or after 64MiB of written bytes,
whichever occurs first. Synchronous `AppendGroup` still forces durability before
returning. `Sync`, `Close`, segment rotation and the capture-to-direct fence also
force a commit. Replay observes `segment.synced`, never the merely written tail.
Original-source and replay retention pins remain independent of cache generations.
An encoding, write or sync failure poisons subsequent appends and explicit syncs.
No successful receipt is published before fdatasync succeeds.

Receivers reserve bytes before entering the direct-write fence. The conservative
reservation includes input bytes, native columns, copied names, sorting and
compression work, instead of counting connected handlers. Active reservations are
capped at 128MiB (less on small machines); idle pooled native columns are capped
at 32MiB. Existing per-segment encoder dictionaries, normal cache memory and
socket buffers remain separate. This is a staging admission budget, not an RSS
limit. Each encode group is also bounded by its estimated bytes and request count.

The one-second timer is not a hard crash-loss deadline: scheduling, encoding,
writing and fdatasync latency can extend exposure. TCP has no application-level
durable acknowledgement. Process crashes can recover extra complete frames still
in the kernel page cache; a power-loss model restores exactly the last successful
sync image. Tests compare IEEE bits and correction order, actual archive contents,
and injected encode/write/sync/publication failures against that storage image.

An isolated Linux capture test is available with
`GROUP_COMMIT_CAPTURE_GATE=1 STARTUP_LOAD_ISOLATED=1 GROUP_COMMIT_OUTPUT=/private/result.jsonl`.
Run only `TestStartupGroupCommitCaptureGate` using a release-style cgo test binary.
It offers 26M points/s over 280 TCP connections for 600 seconds to measure whether
actual intake remains above 25M/s, including rotation and a final forced sync.
Its result is explicitly capture-only; startup replay capacity requires the
separate full startup-load gate. `GROUP_COMMIT_SECONDS` shortens diagnostics and
cannot qualify the 600-second requirement.
