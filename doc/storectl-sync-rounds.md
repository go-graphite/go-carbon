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

Round input scope is journaled and retained by checkpoints. An interrupted
legacy union round continues that union round after upgrading. If the saved
phase is verification and that union round reaches or exceeds the effective
round limit, completed earlier rounds are not replayed: the current union
supersedes their observations and no further fill can use their history.
One `sync_replay_skipped` event explains this decision. Existing journal
records remain unchanged.

For an already entered legacy full-history union, the original union membership
also stays fixed: a newer source-only default cannot remove target-only names
or shift the saved batch cursors. Completed current-round receipt ranges are
adopted without new data or metadata reads, including their recorded holes,
skips and unindexed results. Earlier observations win overlapping recovery rows;
newer recovery/planning rows cannot reduce the adopted progress. Gaps without a
completed receipt still need verification. `sync_legacy_round_adopted` reports
how many names were retained. This is scheduling compatibility with the old
round's observations, not a claim that they prove the current live stores clean.
The `cursors` map in progress.json belongs to discovery and never by itself
proves verification completion.

Otherwise earlier rounds are replayed to reconstruct input and fills. New
rounds, explicit rescans, READY and closed-window proofs keep their mutation
validation. No state reset is needed.
A legacy round beyond the new limit finishes its current verification and
reports the remaining holes without another fill. If a fill had already begun,
it finishes that fill and verifies its repair set before reporting.

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
