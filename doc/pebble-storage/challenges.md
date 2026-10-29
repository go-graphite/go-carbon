# Production challenges and solutions

Each entry: what was seen, how it was diagnosed, what changed, and the
result. Numbers are from a large production host (about 3M active metrics,
about 1 TB store, one NVMe device sustaining about 700 MB/s of mixed writes)
unless stated.

Most diagnoses used the same tools:

- **Goroutine dumps** (`/debug/pprof/goroutine?debug=1`), classified by top
  frames: where persister workers, flushes and compactions wait.
- **CPU profiles** (`/debug/pprof/profile`).
- **Engine and store metrics** (`pebble_*` for Pebble internals, `store_migration_*`/`store_relayout_*` for go-carbon's own processes) and the store section of `/admin/info`.
- **`iostat`, kernel wait channels and `/proc/<tid>/stack`** for the disk
  side.

## Readers queued behind range-tombstone fragmentation

- **Symptom.** The persister fell to a fraction of its normal rate (14k of
  40k planned updates/s; later 3–5k after restarts) with few write stalls.
- **Diagnosis.** Goroutine dumps showed 40–90 of 96 persister workers in
  `sync.(*Once).doSlow` under `pebble.(*keySpanFrags).get` ←
  `memTable.newRangeDelIter` ← `Batch.NewIter`. Pebble keeps one fragment
  cache of a memtable's range tombstones. Every new range tombstone replaces
  it, and the next reader re-fragments all of them while every other reader
  of that memtable waits. Updates read through indexed batches, so every
  update builds such an iterator.
- **Causes.**
  1. Block rotations deleted old groups with `DeleteRange`.
  2. Later, out-of-order merges deleted whole virtual files with more than
     the 1,024 keys the point-delete path allowed. These peak after restarts,
     when restored cache points arrive out of order.
- **Fix.** `continuationIO.DeletePrefix` point-deletes the visible keys of an
  indexed batch (limit 1<<20, beyond any virtual file). Fresh generations
  delete only their own pending keys. `pebble_continuation_range_deletes_total`
  counts any remaining fallback.
- **Result.** The first fix took the persister from about 14k to its planned
  rate. The second targets the post-restart collapse.

## Flushes waited on one table sync after another

- **Symptom.** Write stalls 15–40% ("memtable count limit reached") with the
  memtable queue pinned at its budget.
- **Diagnosis.** Sampling the flush goroutine 30 times put it in `fdatasync`
  at table `Finish` 50–60% of the time, on v6 and v7 hosts alike. Flushes
  write 2 MiB L0 tables and Pebble syncs each one as it finishes, so a 3.9 GB
  flush waited for about 1,900 syncs in turn, each behind the device queue.
  Pebble relies on new tables only after syncing their directory, just before
  the manifest commit (`objstorage` provider `Sync`).
- **Fix.** `store-parallel-table-syncs` wraps the store file system
  (`deferredSyncFS`):
  - a Pebble table's `Sync`/`SyncData` records the file instead of syncing;
  - a directory sync first syncs every recorded table, N at a time, then the
    directory.

  Durability at every manifest commit is unchanged. A failed table sync is
  sticky: it fails every later directory sync, because another job's tables
  may have been in the failed batch.
- **Second problem.** With one batch at a time, 4–11 of 16 compactions sat
  finished but waiting for their turn to commit, holding worker slots while
  L0 grew. Batches now overlap: a directory sync syncs the tables pending at
  its start in parallel with running batches, then waits only for batches
  that were already running.
- **Third problem.** Pebble's provider skips the directory sync when no
  object was created or removed since its previous one, and clears that flag
  before syncing. A job whose table was created before another job's
  directory sync and finished after it then skipped the barrier and could
  commit the table to the manifest before its data was durable, exposed to a
  power loss until the next barrier. `patches/pebble-provider-dir-sync.patch`
  makes the provider always sync the directory when the store defers table
  syncs.
- **Result.** Write stalls 25% → 0% on the canary; memtables from 15.5 GB
  (budget 16 GiB) to about 2 GB.
- **Consequence.** With flushes fast, the limit moved to compaction and L0
  (see [Bottlenecks move](#bottlenecks-move)).

## Large L0 tables piled up sublevels

- **Symptom.** With `store-target-file-size` = 64 MiB (to cut per-table syncs)
  L0 went from 1–4 to 14 sublevels and writes stalled on the L0 limit.
- **Why small L0 tables.** Every flush covers the whole key range written
  during its memtable, and writes here are spread over millions of metric
  IDs. Pebble organizes L0 into sublevels and splits flush output at the
  boundaries of existing L0 files, so compactions can take narrow,
  non-overlapping slices and run several at once. A larger L0 table spans a
  wider range, overlaps more files in other sublevels and in the base level,
  and drags more of them into each compaction. Compactions grow and conflict,
  L0 drains slower, and sublevels pile up.
- **Fix.** Keep Pebble's 2 MiB L0 tables and make their syncs cheap (above).
  `store-l0-target-file-size` exists for experiments at 4–8 MiB; deeper
  levels keep their own sizes.

## Incremental syncs waited behind write-back

- **Symptom.** Flushes took about 15 s per memtable; half the time was spent
  in memtable-count stalls.
- **Diagnosis.** Pebble's default bytes-per-sync (512 KiB) issues an
  incremental sync every 512 KiB of table data. Each waited behind a page
  cache with hundreds of GB dirty (unconverted Whisper files, long
  dirty-expire settings).
- **Fix.** `store-bytes-per-sync = 64 MiB`.

## The filesystem journal

- **Symptom.** Dozens of threads in uninterruptible sleep in
  `xlog_grant_head_wait` / `xlog_cil_force_seq`: XFS log-space waits.
- **Diagnosis.** Wait channels and kernel stacks. With `noatime` alone, every
  write still journalled an inode timestamp update, so millions of CWhisper
  file writes (plus conversion's opens and unlinks) kept the log full.
- **Fix.** Mount the data filesystem with `lazytime`. D-state threads dropped
  from about 90 to about 10 and conversion sped up about 5x. The remount can
  spin minutes in the kernel holding `s_umount`, so run it detached.
  Deferred reclaim (`store-migration-reclaim-delay`) batches unlinks for the
  same reason.

## v6 kept rewriting cold history

- **Symptom.** Compaction about 11x the flushed bytes (364 vs 33 MB/s), 64%
  of it into L5/L6. Hosts were compaction-bound near the device's ~700 MB/s
  write ceiling.
- **Diagnosis.** In v6 every key of a metric shares its prefix, so each
  metric's hot tail sits beside its whole history. Writes reach every
  metric, so every bottom-level table overlaps recent writes and each
  compaction rewrites cold data to merge a few hot keys.
- **Fix.** Format v7: point groups in hourly time buckets, and headers in
  128-byte pages of which only the changed ones are written
  ([data-layout.md](data-layout.md#time-buckets-v7)).
- **Result.** Compaction per persister update roughly halved: reads 24–25 KB
  vs 39–53 KB, writes 23–24 KB vs 36–49 KB, against v6 peers on the same
  hardware.

## The delete wave after a v7 upgrade

- **Symptom.** One to two hours after enabling v7 the host slowed sharply.
  The persister fell to about 3k updates/s for about 25 minutes and the cache
  climbed toward its limit.
- **Diagnosis.**
  - **Deletions dominate.** Compaction read 1.5 bytes for every byte written,
    against 1.09 on peers, and the device read 451 MB/s against 161.
  - **Old copies are going.** L6 shrank from 894 to 743 GB in about 30
    minutes. As v6 blocks rotate into v7 their old groups are deleted, and
    those tombstones were now reaching the bottom level.
  - **Concurrency fed it.** All 16 compaction workers were busy (peers ran 5),
    because Pebble raises concurrency with debt.
  - **Flushes starved.** They waited in the kernel's writeback throttling
    behind that traffic; memtables filled and writes stalled.
- **Mitigation.**
  - **Room to absorb it.** Raised cache `max-size` by `SIGHUP` to buy
    headroom; no points were dropped.
  - **Live compaction control.** Made `store-compaction-workers` reloadable,
    and paced the relayout per interval.
  - **A signal to watch.** "Compaction input dropped"
    (`1 − compaction write / compaction read`) rose from 6–8% to 31% and
    marks the end of the wave as it returns.
- **Lesson.** Upgrade one host at a time, with cache headroom, and expect a
  wave proportional to the v6 data the host holds.

## Compaction debt looked inflated

`pebble_compaction_debt_bytes` is Pebble's estimate (Pebble v1.1, roughly):

1. Level targets come from the store size: the bottom target is 90% of the
   total, and each level above is the one below divided by a smoothed
   multiplier.
2. If L0 and the base level are non-empty, debt = L0 + base level.
3. Walking down from the base level, carry L0's bytes as "added". Where a
   level plus the carried bytes exceeds its target, the excess carries down
   and debt += excess × (next level ÷ this level + 1).

Worked example during the delete wave (GB): L0 9.1, L1 0.2, L2 1.1, L3 5.0,
L4 25.6, L5 141, L6 791 (total 973).

| Level | Target | Size + carried | Excess carried | Debt added |
|---|---:|---:|---:|---:|
| L0 + L1 | | | | 9.3 |
| L1 | 0.06 | 9.3 | 9.2 | 10.3 |
| L2 | 0.42 | 10.3 | 9.9 | 14.7 |
| L3 | 2.85 | 14.9 | 12.1 | 32.8 |
| L4 | 19.2 | 37.7 | 18.4 | 87.5 |
| L5 | 129.7 | 159.4 | 29.7 | 177.1 |
| **Total** | | | | **≈ 332** (reported 314) |

Two things inflate it:

1. **L0 bytes cascade through every level.** 9 GB of L0 contributes most of
   the total; peers with 0.8 GB of L0 reported 5–18 GB of debt.
2. **A shrinking bottom level shrinks every target.** As deletions shrank L6,
   L5's target fell below its size even though L5 itself had not grown.

So high debt can mean "L0 is behind", or "the store is shrinking", not
necessarily "compaction has hundreds of GB to do". Look at L0 sublevels and
the per-level sizes before acting.

## Bottlenecks move

Removing one limit exposed the next:

1. **Fast flushes moved the backlog into L0.** L0 then grew from 1–2 to 15–19
   sublevels: compactions were waiting on their own output syncs on a busy
   disk.
2. **L0 depth raised read cost.** Every iterator seek visits each L0 sublevel,
   so persister reads (headers, groups) slowed as L0 grew. Write stalls had
   been limiting the input rate; without them, L0 depth did.
3. **Levers.** Lower competing I/O (pause relayout, lower conversion), raise
   compaction workers live when the disk has headroom, and keep L0 thresholds
   that still stop writes before reads collapse.

## Table cache churn

- **Symptom.** Reads of uncached data were 30–60% slower per metric than on
  CWhisper, and CPU showed `tableCacheShard.newIters`, `findNodeInternal`,
  syscalls and reader-count atomics.
- **Diagnosis.** Hosts held 23,000–37,000 tables, against a table-reader
  budget of 8,192. They reopened 35,000–48,000 tables per second.
- **Fix.** `store-max-open-files = 65536`.
- **Result.** Table opens 0/s and CPU about 8% lower. Read p99 barely moved,
  because it was dominated by very large renders on either backend.

## Conversion starved the persister's reads

- **Symptom.** With conversion at 512 workers the persister fell from 25–43k
  to 9–18k updates/s, and to about 5k right after a restart. The cache
  climbed.
- **Diagnosis.** Most persister workers were in disk reads (headers and
  groups), while block-cache misses rose to 190k/s. The conversion lease only
  backed off on stalls and cache size, so it kept adding read load.
- **Fix.** Cut the lease to 128 workers. The persister recovered to 28.6k
  updates/s within two minutes. Controllers should also watch persister
  throughput.

## Thread exhaustion

- **Symptom.** The process crashed at 12,288 conversion workers.
- **Diagnosis.** Each worker in a blocking file read or sync holds an OS
  thread.
- **Fix.** Keep workers below roughly 6,000 on a large host, bound file I/O
  with `store-migration-io-slots`, and have the lease controller back off on
  thread count.

## Relayout burst after a stall

- **Symptom.** After a stall the relayout ran unpaced for minutes.
- **Diagnosis.** Pacing compared total bytes with total elapsed time, so time
  spent stalled earned a budget that was then spent at full speed.
- **Fix.** Pace per interval from a live rate; the rate is reloadable.

## A failed open upgraded the marker

- **Symptom** (found in review). A second opener rejected by Pebble's lock,
  or invalid options, still left the v6 marker replaced by v7, which older
  binaries and volume guards refuse.
- **Fix.** Decide the format before opening, replace the marker only after
  `pebble.Open` succeeds and before any v7 key is written. Only one relayout
  runs at a time for the same reason of safety: two runs planning the same
  metric could delete copies the other had made live.

## Restarts are expensive

- **Graceful stop.** A `SIGUSR2` stop dumps the cache: about 40 s for 650M
  points, minutes at larger sizes.
- **Start.** The store replays unflushed WAL, block and table caches start
  cold, and restored points cause out-of-order rewrites. Throughput can stay
  low for many minutes.
- **Verify the new process.** The old process keeps answering on its ports
  during its graceful stop, so readiness checks can pass against the old
  process. Check the new main PID and its executable.
- **Prefer live changes.** Reloadable settings (compaction workers, relayout
  rate, cache `max-size`) avoid restarts.

## Untrimmed drives capped compaction

- **Symptom.** Compaction writes sat at about 550–600 MB/s per host with
  compaction debt growing to 1–1.6 TB, whatever the compaction worker count
  (24 workers changed nothing). Device write latency was about 50 ms.
- **Diagnosis.** The data volume (XFS) had no `discard` and was trimmed only
  weekly, while compactions delete tens of TB of tables a day. The drives
  therefore treated freed space as live, and their own garbage collection
  copied it: the drive, not Pebble, was the bottleneck. NVMe drives with the
  OCP cloud SMART log report physical NAND bytes written; physical ÷ host
  bytes written is the drive's write amplification, which rises in this state.
- **Fix.** `fstrim` on the data volume, then a daily trim timer.
- **Result.** On the first host, write latency fell from 53 to 14 ms over
  about three hours after the trim, device writes rose from 750 to 1,200 MB/s,
  compaction from 580 to about 1,000 MB/s, and debt from 1,067 GB to under
  20 GB, with the same 16 workers. Other hosts followed within 1–2 hours of
  their trims. A trim takes 10–60 minutes under load and dips compaction
  meanwhile.

## Metric names longer than an index block

- **Symptom.** Several hosts were killed by the kernel OOM killer about 1.5 TB
  RSS, minutes after each restart, while serving reads. With external reads
  blocked, memory stayed flat.
- **Diagnosis.** A heap profile showed 505 GB of live block-cache values
  allocated by `sstable.(*Reader).readIndex` under catalog lookups
  (`Fetch` → `metadataFrom` → `db.Get`). Recent tables had 12,602 entries,
  12,602 data blocks and 12,602 index partitions, with a 77 MB top-level
  index for a 36 MB table: a client had started sending metric names of about
  6,150 bytes. Every catalog key was larger than the 4 KB index block, and a
  point lookup loads a table's top-level index before its bloom filter can
  exclude it, so once such tables sat in L0 nearly every lookup loaded tens of
  MB of index, and concurrent reads pinned them. A cold block cache after a
  restart made it worse.
- **Fix.** Creating a metric now fails with `ErrNameTooLong` for names over
  4000 bytes or with a node over 255 bytes, the limits a Whisper file already
  has, and the persister drops such points instead of retrying them.
  `store-purge-invalid-names` deletes the names already stored.
- **Result.** Pending rollout.
