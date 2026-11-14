# Model archive-anchor changes from captured native history

`cohort-anchor-plan` evaluates one checksummed native continuation record in
memory. It compares physical point multisets before/after the anchor plan,
checks selected fetch results, and requires the resulting model to be
idempotent. It has no listener, store path or production mutation operation.

```sh
go build -o /tmp/cohort-anchor-plan ./tool/cohort-anchor-plan
/tmp/cohort-anchor-plan -at 1700003600 < captured-native.wire > anchor-model.json
```

The first usage is offline review of a captured archive-anchor repair candidate.
Use the capture's fixed clock for `-at`. Input is bounded to 16 MiB. Corrupt,
absent and non-continuation records fail. The result deliberately reports
`applied: false` and `runtime_qualified: false`: MODEL_VERIFIED proves only the
in-memory candidate, and does not authorize or establish a live repair.
