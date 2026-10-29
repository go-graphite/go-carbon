# Live migration to shared exact blocks

> Design overview, data layout, write and read paths, tuning and production
> lessons: [Pebble store design notes](pebble-storage/README.md).

Use live migration to complete a verified CWhisper copy without pausing intake
for a full-tree final scan. The application opens the shared database once,
writes the original files and shared store, and initially reads original files
plus its ordinary cache. Bounded workers repair each metric under the same
persister mutex as normal writes, then audit the catalog for vanished/new files.
After verification, the existing persister pool briefly drains and restarts on
the same object while the HTTP/gRPC storage reader switches to shared blocks.
The original index, intake sockets, counters and rollback files remain current.

A migration configuration extends the existing mirrored exact-store config:

```toml
[whisper]
storage-backend = "pebble-chunk"
store-encoding = "exact"
data-dir = "/var/lib/carbon/whisper"
store-dir = "/var/lib/carbon/shared-exact"
store-mirror-files = true
store-live-migration = true
store-migration-workers = 4
```

For live resource adjustments, set `store-migration-workers` to a ceiling (for
example 24) and `store-migration-control-file` to an absolute local JSON path.
Atomically replace that file with `version: 1`, an increasing `generation`,
`workers` between zero and the ceiling, and `expires_at` as Unix seconds within
the next five minutes. Refresh the lease while controlling resources. Zero
pauses new imports; existing operations finish. Invalid, missing or expired
leases use one worker. Changes preserve the traversal, completed imports and
audit state; they require no HUP. Effective workers, in-flight work and generation
are visible in `/admin/info?scopes=storage` and the migration receipt. A controller
can increase workers while request latency and queue depth are healthy, and
reduce them as production pressure rises.

Complete the verified baseline first. Never run an external converter against the
application's database handle. An active conversion marker prevents startup;
catch-up/session markers prevent ordinary shared serving but can remain during
migration because the API still uses originals. Tags, administrative shared
expiration and buckyd must be disabled, as required by the rollback mirror.
Source paths must be canonical. Failure or cancellation leaves guards in place.
Retain the stock executable and original files for an explicit rollback.

`LIVE-STORE-MIGRATION.json` records the actual process identity, phase, verified
archive-point count, catalog audit and baseline skips. Phase `shared` is published
only after the reader changes. Retain `MIRROR-RECONCILE-REQUIRED` and originals
for rollback; a restart requires reconciliation again. Initial binary takeover
uses ordinary handoff and must be qualified separately from the online scan.

An optional acceleration setting reuses an operator-trusted **completed full
verification**:

```toml
store-migration-baseline-copy = "/var/lib/carbon/retained-cwhisper-copy"
```

The matching `CONVERSION.json`, `CAPTURE.json` and `VERIFIED-CAPTURES.json` must
bind this copy, source and database. Operators adopting an older verified copy
must audit its actual completed run and verification settings before issuing
that receipt. Partial or previously mirrored stores cannot enable the shortcut.
Only initial catalog IDs at generation/revision one are eligible, and initialized
shared-extent equality must prove the original main/OOO bytes are unchanged.
Changed, sparse, missing or unsupported cases receive full snapshot verification.
The catalog still checks current source existence. There is no mtime shortcut.

Both storage backends use the same CWhisper OOO/retention correction behavior.
The mirror adds write work; measure CPU, RSS, read/find/write latency and disk I/O
on equal steady windows after migration, including background compaction. The
format switch itself does not establish a fleet compression or performance claim.
