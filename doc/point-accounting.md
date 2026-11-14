# Receive and persistence accounting

TCP receipt counts parsed input points before cache admission. A successful
persister update counts the entire submitted batch, including timestamps that
the backend folds into one archive slot. It is not a count of unique stored
slots. Other receiver instances and locally collected metrics have their own
receipt paths.

Cache admission exports cumulative Prometheus counters:

| Metric | Meaning |
| --- | --- |
| `receiver_name_too_long_rejected_points_total` | Names over the intake byte limit |
| `cache_throttled_points_total` | Points actually refused by the throttle hook, including throughput, usage and creation limits |
| `cache_overflow_dropped_points_total` | Points refused by point or byte capacity limits |
| `cache_invalid_tag_rejected_points_total` | Points whose tag normalization failed |
| `cache_xlog_write_failed_points_total` | Points in failed diversion writes; a prefix may have reached the destination |
| `persister_name_too_long_dropped_points_total` | Names refused later by the persistence backend |

These counters are exported automatically with the existing cache collector.
They count points, including every point of a rejected batch, and survive
Graphite `Stat` collection. Throttle totals count only an actual rejection:
quota warnings configured to allow traffic do not increment them. Existing
per-namespace quota metrics and creation-attempt counts remain useful for
identifying the policy, but cannot substitute for a cumulative point counter.
Recovered input that bypasses live admission does not increase rejection totals.

Compare receipt, committed input points, and rejection counter changes over the
same interval and worker lifetime. Include the change in `cache_points`:

```text
received + other admitted input
  = committed + intake rejections + change in queued and in-flight points
```

`cache_points` describes queued points, so a strict endpoint comparison must also
allow for batches already popped by persisters. Receiver reporting, scraping and
cache sampling can happen at slightly different instants. Recovery, copy work,
local self-metrics, and graceful input diversion can cross the accounting
boundary independently of TCP receipt. Backend write failures are retried when
cache requeue is configured. Archive alignment, duplicate timestamp updates,
rollups, retention expiry and out-of-order handling affect stored values without
reducing the successful persister input-batch count.
