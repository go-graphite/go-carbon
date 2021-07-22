# Quotas

In large Graphite (go-carbon) storage installations, it's desirable to have control over how many resources a user can consume.

Go-Carbon addresses this issue by the implementation of pattern-matching based quota design.

Limitations/Caveats:

* The current implementation is based on concurrent/realtime trie-index, so to use quotas, also need to enable those features on go-carbon.
* Pattern matching rules aren't regexp, but like graphite query syntax, using globs.
* Quota and usage metrics are only produced if the the control has non-empty quota values.

## Available controls

* namespaces: how many sub-namespaces are allowed for the matched prefix/namespaces
* metrics: how many metrics are allowed for the matched prefix/namespaces
* logical-size: how many logical disk space are allowed for the matched prefix/namespaces
* physical-size: how many physical disk space are allowed for the matched prefix/namespaces
* data-points: how many data points (inferred using storage-schemas.conf) are allowed for the matched prefix/namespaces
* throughput: how many data points are allowed within the interval specified by `carbonserver.quota-usage-report-frequency` for the matched prefix/namespaces
* dropping-policy: available values includes `none` and `new`. `none` means doesn't not drop any values. `new` means dropping new metrics. `none` can be used to produce only the quota, usage, and throttle metrics without actually dropping the data.

### Why `logical-size` and `physical-size`

If `whisper.sparse-create` or `whisper.compressed` is enabled, logical size could be much larger than physical size.

Distinguishing them gives us control for that scenario.

### `data-points` and `logical-size`

Usually, `data_points` corresponds to `logical-size` as they are both determined by its matching retention policy.

### `throughput`

Throughput controls how many data points are allowed within the interval specified by `carbonserver.quota-usage-report-frequency`.

This is useful for us to monitor how many datapoints are sent to a specific patthen/namespace. It's useful for cases like some of those namespaces are generating too many datapoints and causing other namespaces to be overflown in the memory cache.

The value is reset every interval specified by `carbonserver.quota-usage-report-frequency`.

If `carbonserver.quota-usage-report-frequency` is 1 minute, then `throughput = 600,000`, then it means only 600k data points per minute are allowed to be pushed to the matched namespace/pattern.

For `throughput` control, both new and old metrics might be dropped, depending their arriving order.

## Special values

The control values can be set to `max`/`maximum`, which is set to max int64 (9,223,372,036,854,775,807), and it practically means no limit.

If the config is not set or set to empty ``, then no quota and usage metrics will be produced.

## Stat Metrics

When using quota, go-carbon will also generate usage, quota, and throttling metrics for each matched patterns.

```
carbon.agents.{host}.quota.metrics.sys
carbon.agents.{host}.usage.metrics.app
carbon.agents.{host}.throttle.app
...
```

The `carbon.agents.{host}` is from config `common.graph-prefix`.

We can also define a prefix for the generated path, using `stat-metric-prefix` for each matched patterns.

## How to enable Quotas

For go-carbon.conf:

```ini
[whisper]
quotas-file = "/etc/go-carbon/storage-quotas.conf"

[carbonserver]
scan-frequency = "2h"
trie-index = true
concurrent-index = true
realtime-index = 65536
quota-usage-report-frequency = "1m"
```

Quota config example:

```ini
# This control all the namespaces under root
[*]
metrics       =       3,000,000
logical-size  = 250,000,000,000
physical-size =  25,000,000,000
# max means practically no limit
data-points   =             max
throughput    =             max
stat-metric-prefix = "level1."

[sys.app.*]
metrics       =         3,000,000
logical-size  = 1,500,000,000,000
physical-size =   250,000,000,000
data-points   =   130,000,000,000
stat-metric-prefix = "level2."

# This controls the root/global limits
[/]
namespaces    =                20
metrics       =        10,000,000
logical-size  = 2,500,000,000,000
physical-size = 2,500,000,000,000
data-points   =   200,000,000,000
dropping-policy = new
```
