# store-prune-empty-dirs

Remove leftover Whisper directories after shared-store conversion. The Go tool
uses **32 depth-3 subtree workers and no rate limit by default**. Each worker walks
its subtree depth-first with fd-relative getdents/d_type and
`unlinkat(..., AT_REMOVEDIR)`. There is no per-directory timer, global counter
update or HTTP request in the unpaced removal loop. The daemon stays online and
retains its store lock.

## Build and run

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o store-prune-empty-dirs ./tool/store-prune-empty-dirs
/usr/local/sbin/store-prune-empty-dirs -root /var/lib/carbon/whisper
```

The local `http://127.0.0.1:8080/admin/info?scopes=storage` response must report
`storage.migration.phase = "shared"`, `in_flight = 0`, no skipped sources and no
error. Missing status, redirects and failed HTTP checks refuse or stop the run.
A request that times out during the walk (`-endpoint-timeout`, default 3s) pauses
directory operations instead: the walk resumes after the next passing check and
stops after `-max-endpoint-timeouts` consecutive timeouts (default 6; 0 stops on
the first). go-carbon's metrics endpoint can take several seconds under load.
There is no override. Both status and metrics URLs must be local HTTP endpoints.
The tool does not open the Pebble database.

The first three directory levels distribute independent subtrees to the workers;
shallow parents are removed after their workers finish. `-workers N` changes the
worker count. Empty leaves are tried with rmdir directly from their parent's
listing, avoiding an open/read/close for each empty inode. Occupied directories
are read and removed after their children. Buffers and counters belong to each
worker; progress merges snapshots between directory-read batches and jobs.

The root is retained. Descendants use pinned parent fds and `O_NOFOLLOW`.
Symlinks, special files, all `.ns-quota-trim` subtrees, and configured exclusions
are retained. Linux uses openat2 with `RESOLVE_NO_XDEV` to avoid entering mounts
without per-entry stat; older kernels/platforms check the device of opened
directories. Ordinary entries are classified by d_type; only candidate lock
files need stat. A filesystem returning `DT_UNKNOWN` is refused. Missing or
occupied directories are harmless. Applying repeats until a pass removes
nothing, including on filesystems whose directory cookies change after deletion.

Use `-exclude store-path,input-log-path` if other live storage directories are
inside the Whisper root. Exclusions are relative paths. Normally the shared store
and input log are disjoint from the original tree.

## Stale standalone bucky locks

The tool can also unlink a regular **zero-byte `*.lock`** when its target (the
name without `.lock`) is absent and it satisfies GNU find's `-mmin +60`
predicate: older than one hour. This includes `.wsp.lock` and `.wsp.ooo.lock`.
A nonblocking flock protects an active lock holder; device/inode, type, size and
mtime are checked again before unlinking. Quarantine contents are never inspected.
All other files are retained.


## Inventory, progress, stop and resume

```sh
/usr/local/sbin/store-prune-empty-dirs -root /var/lib/carbon/whisper -dry-run
```

Dry-run reads only: it counts directories, empty directories, regular files by
suffix, symlinks, special entries, exclusions and eligible stale locks. It never
unlinks a lock or directory. It is also unpaced and parallel by default; add
`-rate 200` when a slower inventory is wanted. Excluded subtrees contribute only
to the exclusion count.

Progress includes worker count, pass count, directories visited/removed,
files/lock counts, recent dirs/s and `budget/s=unlimited` unless a rate was given.
Applying may revisit retained entries over multiple passes; `removed` and
`locks_removed` count successful unlinks, while the other counts describe visits.
Dry-run uses one pass. Counts remain observational under concurrent changes.

```sh
sudo touch /var/lib/carbon/stop-prune-empty-dirs
```

A stop file or SIGTERM interrupts the run. Remove the stop file and rerun the
same command to resume: deleted directory entries are the checkpoint, so deleted
branches cost no work. No cursor file is needed. Repeating passes also handles
entries skipped by the tool's own directory mutations. Keep the stop file outside
the tree. Stop checks run every 200ms; a pending HTTP request (`-endpoint-timeout`), pacing
or filesystem I/O can add delay. Progress/status checks default to every 5s.
A crash may leave empty parents because pruning adds no fsync per rmdir.

## Detached execution and impact

Run the selected implementation with systemd-run. For the Go tool:

```sh
sudo systemd-run --unit=carbon-prune-empty-dirs --collect \
  --property=User=carbon \
  /usr/local/sbin/store-prune-empty-dirs \
    -root /var/lib/carbon/whisper -workers 32
journalctl -fu carbon-prune-empty-dirs
```

To yield scheduling priority to go-carbon, add `--property=Nice=19` and
`--property=IOSchedulingClass=idle`. These change scheduling priority rather than
imposing a directory rate. Use the account that owns the tree if it differs.
`systemctl stop carbon-prune-empty-dirs` also stops the transient unit.


Use a shared optional limiter when needed:

```sh
/usr/local/sbin/store-prune-empty-dirs -workers 32 -rate 2000
```

`-rate` limits directory reads and rmdir attempts across all workers. The old
`-dirs-per-second` flag is an alias. Linux auto pacing is opt-in and requires an
explicit positive initial rate and disk device names:

```sh
/usr/local/sbin/store-prune-empty-dirs -workers 32 -rate 200 -auto \
  -devices md0,dm-0,nvme0n1 -max-dirs-per-second 2000
```

Auto mode uses the worst selected device utilization/await and mean successful
Pebble write latency from deltas of `persister_pebble_write_seconds_total` and
`persister_pebble_updates_total`. The default ceilings are 60% util, 5ms await and
20ms write mean. Three healthy 5s windows raise the rate 25%; a breached ceiling
or increased write failures halves it. Idle writes do not authorize increases.
Missing metrics/devices, counter resets and failed health reads stop the run; a
timed-out health read pauses the walk and halves the rate as described above.
The default metrics URL is `http://127.0.0.1:7007/metrics`, served on
`[pprof].listen` when `[prometheus].enabled` is true. Override `-metrics-url` for
the actual endpoint. Continue watching tail latency independently of the means.

Expected residual inode use is **quarantine + shared-store files/directories +
input-log segments + remaining live files and their parents**, plus filesystem
and operational metadata. Slab may decline later as cached dentries are reclaimed.
Unexpected retained WSP/OOO/lock files need an ownership/recovery investigation.
Check updatedb, backup, security/EDR and inventory agents too: they may still walk
the historical tree even when go-carbon no longer does.

## Reclaim and walker audit

Live reclaim prunes each synced batch's empty parent chain **without pacing**;
its directory batches and existing I/O worker pool already bound the work.
Creators retry mkdir+create once on ENOENT. Main files/path locks prevent rmdir
while sidecars are created. Store scratch and original archives are separate
trees. Pruning remains rmdir-only, below the retained data root, with no symlink
traversal or quarantine pruning. The deferred reclaimer pins one root fd for its
lifetime, so batches avoid reopening the same root; Stop releases it after the
workers join. Directory
descriptors are closed before their names are rmdired, and ENOTEMPTY stops an
ancestor chain without another open/probe.

Plain shared mode indexes the store catalog, bypasses FLC and filesystem globs,
and derives quota usage from the virtual index; capacity checks use one statfs.
Clean completed incremental runs switch to catalog discovery. Durable source
completion survives interrupted restart cleanup receipts and avoids a source
re-walk. Pending cleanup still resumes from the catalog. Skipped sources retain
mixed discovery and are refused by the offline tool.

Buckyd lists/imports through the store and serializes only private temporary
files. Startup checks fixed markers/root identity and store scratch, not the
Whisper tree. `-check-policies` refuses shared storage. `store-reclaim-orphans`
visits staged catalog records and exact original paths. `store-convert` is an
explicit source-tree converter rather than a background daemon walker.

Conversion/reclaim verification used a real store, 512 OOO metrics per trial,
64 source directories, 16 conversion workers, exact snapshot comparisons and
catalog cleanup checks. ABBA order gave ten trials per variant. With overlapping
conversion/reclaim, median completed cleanup output was 502.24 metrics/s with
pruning versus 502.67 without (within 0.1%); its existing one-second dispatch
cadence limits this fixture. In isolated drain, pruning's median was 29,405
metrics/s versus 31,390 without, with overlapping ranges. Thus no pipeline
throughput loss was measured, but this does **not** establish zero added I/O or
identical saturated reclaim capacity. Every pruned trial removed its empty
parents, and both variants retained exact values and durable ownership.
