# Restart CPU allowance and history

With metadata prewarming and a positive metadata cache size enabled,
`[whisper] store-metadata-cache-prewarm-cpu-cores` supplies a **fallback**
whole-process CPU allowance for restart work. It is read at startup.

The selection order is:

| Available history | Configured value | Allowance | Logged source |
| --- | --- | --- | --- |
| Fresh host-local steady sample | Any | 1.05 × saved steady CPU cores | `history` |
| None, invalid or stale | Positive | Configured cores | `fallback` |
| None, invalid or stale | `0` (default) | Host CPU count / 12, rounded down, at least 1 and at most GOMAXPROCS | `fallback` |

For example, a fallback of `48` provides headroom on a first start. Once the
process records a steady value of `16` cores, its next start uses `16.8` cores,
even while the configured fallback remains `48`. A sample expires after 24
hours. History from another host or the future is rejected. The older
30-sample median format remains readable within its original freshness window.

The allowance includes user and system CPU from the entire process. Receivers,
reads and input logging proceed independently; optional warming and post-gate
writeout share the remaining allowance. A configured fallback should leave
headroom above mandatory CPU consumption. Zero never means unlimited CPU.

CPU and confirmed update rates are saved in `restart-cpu.json` in the shared
store directory. Sampling requires two consecutive minute ticks after metadata
warming and restoration finish, without cache pressure, and no increase in
queued points across that minute. A steady process can therefore record its
first sample within two minutes of becoming eligible. Growing queues, warming,
dump loading, and pending restored writes prevent sampling. CPU history can
refresh during quiet batching phases. Saved throughput updates only when a
complete minute of confirmed writes meets at least 95% of the writeout floor;
quiet minutes preserve the last qualified rate. Until one is recorded, the
configured writeout ceiling applies. This avoids shrinking the next restart's
catch-up ceiling because of gaps between writeout phases.

Normal batching can keep more points than were present when receivers opened,
and max-delay writeout can have quiet phases. These do not require a return to
the initial point count before recording history. Restart admission retains
its separate drain policy: it continues for at least 30 minutes and until
warming and the startup backlog finish. History is saved each eligible minute
and at shutdown; shutdown joins the sampler before a successor can use its file.
