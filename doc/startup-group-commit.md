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

## Memory accounting

The active admission limit is `max(1MiB, min(128MiB, available_memory/32))`.
Each submission reserves `4*raw_bytes + 128*points + 64KiB` until Stage has
consumed it. At 128MiB this permits at most 2048 simultaneous minimum-size
reservations, fewer than 1,048,576 points, or fewer than 32MiB of raw input;
these are separate ceilings, not quantities that can all reach their maxima
simultaneously. Reservations count conservative work, not measured heap bytes.
Waiting connections retain their existing receiver buffers outside this charge.

Idle buffers use a separate 32-slot pool with at most 1MiB charged per entry:
actual column capacities plus 256 bytes per lifetime peak distinct name and
2KiB for fixed state. The map allowance is conservative accounting, not a Go
runtime allocation contract. References are cleared, multi-point decoder arenas
are discarded, and a large map's high-water charge survives reuse. Allocator
size classes, dead objects awaiting collection, goroutine stacks and runtime
metadata remain additional heap costs.

The 32MiB idle ceiling counts buffers only while they are in the pool. A small
request can borrow a previously larger buffer while reserving only its current
work charge. At the 128MiB admission limit, the deliberately loose ceiling of
2048 admitted requests times 1MiB per borrowed pool entry is 2GiB of retained
buffer accounting, before new work and other owners. Such entries cannot all be
idle simultaneously, and this is not a measured workload, but it makes clear why
adding 128MiB active plus 32MiB idle is not a worst-case heap calculation. An
additional capacity reservation would be needed to charge borrowed high water.

The ordinary input-log writer retains memory across Stage calls and is not
covered by the admission limit:

| Owner | Scaling / bound |
| --- | --- |
| Sort scratch | Per encoder worker: up to 262,144 rows, 32 bytes each, plus radix scratch that can reserve twice the row count (24MiB combined before slice growth slack). Default workers <=8. |
| Raw / columns / new-name scratch | Per worker, reusable capacities at the largest frame/batch seen; raw frame target 4MiB, with one record of overshoot. Separate id, timestamp, value and name arrays coexist. |
| Compression output and write buffer | Per-partition compressed cycle output plus a combined positioned-write buffer; capacities persist at the largest cycle, not the latest small submission. A cycle drains <=64 requests, targeting 8MiB estimated input and allowing the last <=4MiB request to overshoot. |
| Stream dictionary | 16 partition maps retain metric strings and 16-byte previous-point state for every distinct name in the current segment. Rotation clears entries but retains map/slice high-water capacities. The configured segment size is a compressed disk limit, not a heap limit. |
| Durable completions | Stage releases admission before sync. Pending receipts are independently limited: a commit triggers at 65,536, with at most 63 additional requests from the last drained group. Channels, receipts and slice slack remain additional heap, without borrowed point/name references. |
| Compression runtime | Shared zstd encoder with worker concurrency; internal workspaces are additional. |
| TCP receiver | Per-connection wire buffers and stream dictionary; the normal global stream dictionary reservation is 3GiB, independent of startup admission. |
| Replay / cache / storage | Independent decoder dictionaries, replay frames, cache entries, and storage buffers; concurrent replay adds these to capture memory. |

Consequently there is **no total-heap or RSS bound of 128MiB or 160MiB**.
At the admission ceiling, total memory still depends on cardinality, configured
segment size, encoder and receiver concurrency, replay, and allocator/GC state.
A high-cardinality segment can retain much more than the admission limit.
Capacity planning must leave headroom for that full inventory. A strict total
memory guarantee would require a separate encoder-dictionary and scratch policy;
the admission budget alone does not provide one.

`startup_capture_admission_{limit,used,peak}_bytes` expose reservation state.
`startup_capture_admission_waits_total` counts submissions that actually block;
`startup_capture_admission_rejections_total` counts oversized batches.
Exhaustion logs WARN with used/requested/limit bytes, at most once per 30 seconds;
an oversized batch logs ERROR and returns an error. The limit is logged at startup.
`startup_capture_idle_buffer_bytes` reports the separate idle-pool charge.

`startup_capture_admission_seconds_total` divided by
`startup_capture_admission_calls_total` gives mean successful admission latency;
`startup_capture_admission_max_seconds` records its lifetime maximum.
These include immediate admissions and exclude cancelled/rejected calls.
