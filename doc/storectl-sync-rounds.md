# Bounded sync passes

By default, `storectl sync` verifies the discovered catalog once, fills holes,
then verifies only the previous round's hole, skipped and unindexed names. It
stops after that second verification and reports unresolved names in
`residual.json` and `sync_done`; it does not keep retrying live mutations.
Existing target points continue to win during imports. A completed pass is an
observation of its selected names and time horizon, not a live readiness proof.
READY remains a separate, explicit attestation.

```sh
storectl sync --source http://donor:2004 --to http://target:2004 --state ./sync-state
storectl sync --resume ./sync-state --streams 64
```

A completed full-history pass stays complete on resume. Resuming a terminal
residual report schedules only its recorded residual names. Window-only resume
retains its existing closed-window mutation validation. To inspect changes
outside a finished pass, start a fresh state or explicitly request convergence:

```sh
storectl sync --resume ./sync-state --rescan-all --max-rounds=0
```

`--rescan-all` enables full-catalog rounds when all holders support mutation
proofs; without proof support the existing repair-only fallback applies.
`--max-rounds=0` allows convergence while progress continues only with this
explicit opt-in. Otherwise zero (including old saved defaults) means two rounds.
A positive limit can permit more repair rounds without enabling union rescans.

Round input scope is journaled and retained by checkpoints. In the default
finish-first mode, resuming an entered legacy union verification round N >= 2
switches that round to the preceding round's repair set: its hole, skipped and
unindexed names, plus names recorded by its fill receipts. Reconstruction uses
adopted receipts without rereading the earlier catalog or repeating imports.
`sync_legacy_round_repair` logs the union and repair counts once. The transition
and exact selected names are appended to the journal; original records remain
unchanged. Restarting after the transition retains the repair selection.

Observations already made in round N can satisfy repair names, with the newest
eligible round-N observation winning. Sparse receipts record exact membership,
so their bounds cannot overwrite an adopted name between their endpoints.
Names outside the repair selection are not verified. At or beyond the effective
round limit, the round reports residuals without another fill or union scan.
A positive larger limit can allow further repair rounds.

Legacy round 1 keeps its original union membership, including target-only names.
Completed receipt ranges are adopted without new data or metadata reads,
including their holes, skips and unindexed results. Earlier original observations
win overlapping recovery rows; gaps still need verification.
`sync_legacy_round_adopted` reports how many names were retained. The `cursors`
map in progress.json belongs to discovery and never proves verification
completion. Adoption retains historical observations, not a live readiness proof.

`--rescan-all` retains union verification and its existing proof validation.
READY and closed-window campaigns retain their existing validation. If a legacy
fill had already begun, it finishes that fill and verifies its repair set.
No state reset is needed.

Legacy receipt reconstruction uses the same `--streams` verification budget as
an active round. Ambiguous receipt memberships are planned durably and verified
in bounded parallel batches; an interruption retains completed recovery receipts.
The saved current-round counter can remain at its floor during reconstruction.

Each verification backend read (data, schema, mutation proof or expiration
evidence) has a ten-minute budget including restart and busy retries. Healthy
requests retain the previous ten-minute HTTP allowance. An exhausted data read
uses the existing split-and-skip path; timed-out proof/metadata batches remain
skipped residuals without a clean proof. Parent cancellation still stops the pass.
Copy/import retries retain their existing receipt and restart handling.

## Automatic ingest fill

`--fill-path=auto` selects the repair transport per name during fill. Names
already present on the target can replay just their missing finite values to
`--fill-ingest` (default `127.0.0.1:2003`). Run the controller on the target or
set that address to the target's plaintext receiver. Missing names continue
through transfer. `--fill-path=transfer` retains the previous transport for all
names; these operational flags can also override `--resume`.

Eligibility is deliberately conservative. Normal `UpdateMany` routes a past
point to the first archive whose retention covers its age, then propagates to
coarser archives. Replay therefore requires every observed hole for a name to
belong to its last archive, and to be older than the preceding archive's
retention. Single-archive names satisfy the latter condition automatically.
There is a two-minute margin at age boundaries; ineligible names, unknown grids,
and nonliteral plaintext names use the existing transfer/window engine. This
protects independently stored coarse values from propagation. It also means a
multi-archive campaign can remain entirely on transfer.

Replay compares fresh donor and target series over the saved hole ranges,
keeps existing finite target values, and formats floats with round-trip exact
precision. It requires target health monitoring: pause at 75% cache occupancy,
resume below 60%, wait on missing health data, and abort the invocation on an
observed overflow increase. Disabling target health selects transfer.

A socket write alone is insufficient for completion. Before fsyncing a done
range, the controller reads every sent value back through carbonserver and
compares IEEE-754 bits. Coarser archives currently require persistence before
read-back because carbonserver only overlays the cache at the finest step.
Round two retains the normal verification scope. Window repair counts include
confirmed replay batches; an interrupted write keeps an uncertain count.

Full-history replay uses the existing copy journal identity and done ranges, so
an interrupted fill from an older binary skips completed work and can replay
unfinished eligible names. An already-entered native window transaction keeps
its original recovery protocol; a new window fill uses separate ingest receipts
and keeps its fallback inside the selected windows. Neither option changes the
data identity or requires a new state directory.

To roll back the transport, keep this binary and resume with
`--fill-path=transfer`. An older binary may not recognize the new flags saved in
`config.json` after the first upgraded invocation.

Plaintext ingest does not provide an atomic missing-only condition. Comparing
before sending and confirming afterwards cannot fence a concurrent historical
writer. Qualification of that assumption is required before enabling replay on
a live target; transfer remains the path with an atomic target-wins merge.
