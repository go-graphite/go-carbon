# Native snapshot evidence decoder

`cohort-decode` reads native transfer snapshots and emits exact float-bit evidence
for independent read comparisons. It can decode a single record with explicit
half-open windows, process framed point/digest streams, and join sorted digest
inputs with their recorded identities. It never imports points or opens a live
store for writing.

Build the helper from the same reviewed source revision as the codec:

```sh
go build -o /tmp/cohort-decode ./tool/cohort-decode
/tmp/cohort-decode -identity -windows '[{"from":1700000000,"until":1700003600}]' < captured-native.wire > evidence.json
```

The first consumer is an independent native-read audit: retain the original
checksummed wire, decode physical values without numeric reformatting, and
compare exact timestamp/IEEE-754 bits across sources and windows. A checksummed
framed stream can use `-mode points-framed` or `-mode digest`; `-mode stream`
encodes the corresponding exact input stream. `-mode digest-join` and
`-mode windows-join` require `-join-spec` and `-join-summary` paths, and only publish
summary counts after validating complete input identities and sortedness.

`-mode offline-native -offline-directory /absolute/stopped-store` exposes a
bounded JSON-lines pipe with `list` and `export` operations. It requires an
existing regular LOCK and format marker, opens an existing database read-only,
and retains the exclusive database lock for the run. Stop the daemon before
using it; read-only access does not make simultaneous database access safe.
Requests cannot mutate the database. Existing disk-space admission still
applies. Preserve a copy and do not interpret an offline snapshot as proof that
all later queued or live points have been copied.

See the package tests for complete framed and digest-join fixtures, exact
signed-zero/float-bit cases, corrupt/oversized input refusal, missing-name
handling and unchanged offline database file hashes.
