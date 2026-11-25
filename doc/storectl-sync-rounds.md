# Bounded sync passes

By default, `storectl sync` verifies the discovered catalog once, fills holes,
then verifies only the previous round's hole, skipped and unindexed names. It
stops after that second verification and reports unresolved names in
`residual.json` and `sync_done`; it does not keep retrying live mutations.
Finite target points continue to win during imports. A finite donor repairs an
infinite target at the same timestamp, matching verification's finite-point
definition of a hole; existing NaN normalization is unchanged. A completed pass is an
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
