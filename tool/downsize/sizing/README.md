# Capacity estimates from explicit inventories

These standard-library Python commands plan capacity and do not provision hosts
or change routing. Every inventory member supplies its hostname, cluster, site,
backend, physical cores, logical CPUs and store devices explicitly. The bundled
fixtures and shapes are illustrative; replace them with verified measurements.

```sh
python3 collect.py --inventory fixtures/inventory.json --days 7 --step 300
python3 collect.py --inventory inventory.json --days 7 --step 300 --collect \
  --prometheus-url https://prometheus.example.net --graphite-url https://graphite.example.net \
  --state-dir sizing-state --output snapshot.json
python3 size.py fixtures/snapshot.json --output capacity.json
```

Collection prints its exact query plan by default. Supply both service URLs for
live collection and optional bearer authentication through `--token-env`.
No provider library or hostname convention is required. The Graphite
`system_prefix` inventory field defaults to `servers.{site}.*.{hosts}` and
`carbon_prefix` defaults to `carbon.agents.{hosts}`. The Prometheus `job` defaults
to `go-carbon`, and host labels use `host_name`. Set `query_overrides` keyed by
`cluster/site/field` for your exporters, retaining host/device aliases. Missing
series and partial-result warnings never become healthy zeroes.

The collector limits concurrency and request rate, checkpoints raw series and
pins its inventory/query/window manifest. A process lock prevents competing
controllers. Default limits are two requests in flight, one request per second
and a 60 second request timeout. Use a new state directory when inputs change.

Supply `--exclude START-END` for known maintenance/conversion intervals. Sustained
statistics exclude those windows while burst checks retain full-window peaks.
Offline replay requires the original raw-query cache via `--state-dir`; a
summary alone cannot recover excluded intervals or change consolidation methods.
The `quiet.py` tool explains whether a selected observation window is suitable.

RAM estimates distinguish live application heap, configured metadata/block
caches, memtables, non-heap RSS, Slab and active file pages. Metadata backed by
Go memory is removed before applying the GC factor and target cache budgets.
Missing components produce an explicit legacy fallback. CPU percentages are
normalized by observed logical CPU count into inventory physical cores; supply
`logical_cpus` to retain configured CPU slots. Shape capacities, sustained device
throughput, growth, spare hosts and price assumptions must be reviewed locally.

Capacity output shows each constraint, missing evidence, burst checks and
movement estimates. Prices and copy durations are planning inputs or optimistic
bounds, not measurements. Keep all current members until the resize executor's
fencing, durable verification and read-coverage requirements are met. Native
ring plans may be supplied with `--ds-ring SPEC SUMMARY SHAPE`.

Run offline tests with `python3 -m unittest discover -s . -p 'test_*.py' -v`.
