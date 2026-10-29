#!/usr/bin/env python3
"""Read-only Prometheus and Graphite collector. With no --collect, print the exact query plan."""
import argparse
import concurrent.futures
import fcntl
import gzip
import json
import math
import os
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

from common import FIELDS, GUARDS, OPTIONAL_FIELDS, MEMORY_FIELDS, combine, digest, has_go_carbon, positive, read_json, summarize, write_json
from windows import EVENT_FIELDS, activity, complement, describe, merge, parse_exclusions

MIMIR = "prometheus"
GRAPHITE = "graphite"



def load_inventory(path):
    inventory = read_json(path)
    seen = set()
    if not inventory.get("hosts"):
        raise ValueError("inventory.hosts must be nonempty")
    for host in inventory["hosts"]:
        name = host["host"]
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", name) or name.split(".")[0] in seen:
            raise ValueError(f"invalid or duplicate inventory host: {name}")
        seen.add(name.split(".")[0])
        if not host.get("cluster") or not host.get("site"):
            raise ValueError(f"explicit cluster and site required for {name}")
        if host["backend"] not in ("pebble-chunk", "whisper"):
            raise ValueError(f"unknown backend for {name}")
        if not host.get("store_devices") or len(set(host["store_devices"])) != len(host["store_devices"]):
            raise ValueError(f"explicit unique store_devices required for {name}")
        if any(not re.fullmatch(r"[a-zA-Z0-9_-]+", d) for d in host["store_devices"]):
            raise ValueError(f"invalid store device for {name}")
        positive(host["cores"], f"{name}.cores")
        if host["backend"] == "whisper" and not host.get("store_bytes_target"):
            raise ValueError(f"{name}: Whisper needs an explicit Graphite store_bytes_target")
    return inventory


def query_plan(inventory):
    """One query per field and cluster/site; never issue per-metric requests."""
    groups = defaultdict(list)
    for host in inventory["hosts"]:
        groups[(host["cluster"], host["site"])].append(host)
    jobs = []
    for (cluster, site), hosts in sorted(groups.items()):
        names = sorted(h["host"].split(".")[0] for h in hosts)
        glob = "{" + ",".join(names) + "}"
        prefix = inventory.get("system_prefix", "servers.{site}.*.{hosts}")
        system = prefix.format(site=site, cluster=cluster, hosts=glob)
        carbon = inventory.get("carbon_prefix", "carbon.agents.{hosts}").format(site=site, cluster=cluster, hosts=glob)
        selector = 'job=' + json.dumps(inventory.get("job", "go-carbon")) + ',host_name=~' + json.dumps(
            "(" + "|".join(re.escape(h["host"].split(".")[0]) for h in hosts) + r")([.].*)?")

        def prom(name, expr):
            jobs.append(dict(cluster=cluster, site=site, field=name, source="mimir",
                             datasource=MIMIR, expression=expr))

        def graph(name, expr, **extra):
            base = dict(cluster=cluster, site=site, field=name, source="graphite",
                        datasource=GRAPHITE, expression=expr, **extra)
            jobs.append(dict(base, role="sustained", consolidation="average"))
            jobs.append(dict(base, role="burst", consolidation="min" if name in
                             ("cpu_idle_percent", "cpu_iowait_percent") else "max"))

        for field in ("idle", "iowait"):
            graph(f"cpu_{field}_percent", f"aliasByNode({system}.cpu.total.{field},-4)")
        graph("cpu_count", f"aliasByNode({system}.cpu.cpu_count,-3)")
        for field in ("AnonPages", "Active_file", "Slab"):
            graph(field, f"aliasByNode({system}.memory.{field},-3)")
        for field, metric in (("device_write_bytes_s", "write_byte_per_second"),
                              ("device_util_percent", "util_percentage"),
                              ("device_read_iops", "reads_per_second"),
                              ("device_write_iops", "writes_per_second"),
                              ("device_read_await_ms", "read_await"),
                              ("device_write_await_ms", "write_await")):
            # Fetch one layer only; assembly filters to the inventory's devices.
            devices = "{" + ",".join(sorted({d for h in hosts for d in h["store_devices"]})) + "}"
            graph(field, f"aliasByNode({system}.iostat.{devices}.{metric},-4,-2)", device=True)
        graph("metric_count", f"aliasByNode({carbon}.carbonserver.metrics_known,-3)")
        prom("input_points_per_second", f"sum by(host_name)(rate(metrics_received_tcp_total{{{selector}}}[5m]))")
        prom("read_requests_per_second", f"sum by(host_name)(rate(http_requests_total{{{selector}}}[5m]))")
        prom("read_p99_seconds", 'histogram_quantile(0.99,sum by(host_name,le)'
             f'(rate(http_request_duration_seconds_exp_bucket{{{selector}}}[5m])))')
        prom("write_errors_per_second", f"sum by(host_name)(rate(persister_pebble_write_failures_total{{{selector}}}[5m]))")
        prom("stall_seconds_per_second", f"max by(host_name)(rate(pebble_write_stall_seconds_total{{{selector}}}[5m]))")
        prom("unlogged_points_per_second", f"sum by(host_name)(rate(input_log_unlogged_points_total{{{selector}}}[5m]))")
        prom("input_log_errors_per_second", f"sum by(host_name)(rate(input_log_write_errors_total{{{selector}}}[5m]))")
        prom("store_bytes", f"max by(host_name)(pebble_disk_bytes{{{selector}}}) or "
             f"max by(host_name)(store_disk_bytes{{{selector}}})")
        for field, metric in (
                ("heap_live_bytes", "go_gc_heap_live_bytes"), ("block_cache_bytes", "pebble_block_cache_bytes"),
                ("memtable_bytes", "pebble_memtable_bytes"), ("process_rss_bytes", "process_resident_memory_bytes"),
                ("go_heap_objects_bytes", "go_memory_classes_heap_objects_bytes"),
                ("go_heap_free_bytes", "go_memory_classes_heap_free_bytes"),
                ("go_heap_unused_bytes", "go_memory_classes_heap_unused_bytes"),
                ("go_heap_stacks_bytes", "go_memory_classes_heap_stacks_bytes"),
                ("process_start_time_seconds", "process_start_time_seconds"),
                ("migration_in_flight", "store_migration_in_flight")):
            prom(field, f"max by(host_name)({metric}{{{selector}}})")
        prom("metadata_cache_bytes", f"max by(host_name)(store_metadata_cache_arena_bytes{{{selector}}}) or "
             f"max by(host_name)(store_metadata_cache_bytes{{{selector}}})")
        prom("migration_converted_rate", f"sum by(host_name)(rate(store_migration_converted_total{{{selector}}}[5m]))")
        for name, metric in (("block_cache_hit_ratio", "pebble_block_cache"), ("metadata_cache_hit_ratio", "store_metadata_cache")):
            hit = f"sum by(host_name)(rate({metric}_hits_total{{{selector}}}[5m]))"
            miss = f"sum by(host_name)(rate({metric}_misses_total{{{selector}}}[5m]))"
            prom(name, f"({hit}) / (({hit}) + ({miss}))")
        for host in hosts:
            if host["backend"] == "whisper":
                short = host["host"].split(".")[0]
                graph("whisper_store_bytes", f'alias({host["store_bytes_target"]},"{short}")')
    # Site/cluster-specific overrides support older exporter names without
    # editing the collector. The expanded expressions are part of cache identity.
    for job in jobs:
        key = f'{job["cluster"]}/{job["site"]}/{job["field"]}'
        if key in inventory.get("query_overrides", {}):
            job["expression"] = inventory["query_overrides"][key]
    return jobs


def normalize(job, response):
    """Return host[.device] -> timestamp seconds -> value, refusing ambiguity."""
    output = {}
    if job["source"] == "mimir":
        if response.get("status") != "success" or response.get("warnings"):
            raise ValueError("Prometheus failed or returned partial-result warnings")
        rows = [(r["metric"]["host_name"].split(".")[0], r["values"])
                for r in response["data"]["result"]]
    else:
        # /render gives explicit timestamps and no Grafana frame alias guessing.
        if not isinstance(response, list):
            raise ValueError("invalid Graphite render response")
        rows = [(r["target"], [(t, v) for v, t in r["datapoints"]]) for r in response]
    for name, points in rows:
        if job.get("field") == "metric_count":
            # Carbon's graph prefix flattens the FQDN into this node.
            name = name.split("_")[0]
        if name in output:
            raise ValueError(f"ambiguous duplicate series {name}; pin query labels")
        values = {}
        for timestamp, value in points:
            if value is None:
                continue
            value = float(value)
            if math.isfinite(value) and value >= 0:
                values[int(float(timestamp))] = value
        output[name] = values
    return output


class MetricsHTTP:
    """Explicit service URLs; no provider SDK, discovery or automatic redirects."""
    def __init__(self, prometheus_url, graphite_url):
        for url in (prometheus_url, graphite_url):
            if url:
                parsed = urllib.parse.urlsplit(url)
                if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
                    raise ValueError("metrics URL must be an HTTP(S) origin or base path")
        self.prometheus_url = prometheus_url
        self.graphite_url = graphite_url
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None
        self.opener = urllib.request.build_opener(NoRedirect)

    def request(self, base, path, token, timeout):
        if not base:
            raise ValueError("explicit metrics service URL required")
        request = urllib.request.Request(base.rstrip("/") + path)
        if token:
            request.add_header("Authorization", "Bearer " + token)
        with self.opener.open(request, timeout=timeout) as response:
            data = response.read((64 << 20) + 1)
            if len(data) > 64 << 20:
                raise ValueError("metrics response exceeds 64 MiB")
            return json.loads(data)

    def range_query(self, datasource, expression, token, *, start, end, step, timeout, retries):
        query = urllib.parse.urlencode(dict(query=expression, start=start, end=end, step=step))
        return self.request(self.prometheus_url, "/api/v1/query_range?" + query, token, timeout)

    def get_json(self, path, token, *, timeout, retries):
        return self.request(self.graphite_url, path, token, timeout)


class RateLimit:
    def __init__(self, requests_per_second):
        self.period = 1 / requests_per_second
        self.lock = threading.Lock()
        self.next_at = 0

    def wait(self):
        with self.lock:
            wait = max(0, self.next_at - time.monotonic())
            self.next_at = max(self.next_at, time.monotonic()) + self.period
        if wait:
            time.sleep(wait)


def fetch(http, token, job, window, timeout):
    if job["source"] == "mimir":
        return http.range_query(job["datasource"], job["expression"], token,
                                start=window["start"], end=window["end"],
                                step=window["step_seconds"], timeout=timeout, retries=0)
    consolidation = job.get("consolidation", "average")
    query = urllib.parse.urlencode({"target": f'consolidateBy({job["expression"]},"{consolidation}")',
                                   "from": window["start"], "until": window["end"],
                                   "format": "json", "maxDataPoints":
                                   (window["end"] - window["start"]) // window["step_seconds"] + 1})
    return http.get_json(f'/render?{query}',
                         token, timeout=timeout, retries=0)


def curves(raw, host):
    short = host["host"].split(".")[0]
    def get(field):
        return raw.get(field, {}).get(short, {})
    fields = {f: get(f) for f in FIELDS + GUARDS + OPTIONAL_FIELDS + MEMORY_FIELDS}
    cpu = combine([get("cpu_idle_percent"), get("cpu_iowait_percent")])
    counts = get("cpu_count") or {t: host["logical_cpus"] for t in cpu if "logical_cpus" in host}
    fields["cpu_cores"] = {t: (1 - v / (100 * counts[t])) * host["cores"]
                           for t, v in cpu.items() if t in counts and counts[t] > 0 and 0 <= v <= 100 * counts[t]}
    for dest, source in (("anon_bytes", "AnonPages"), ("active_file_bytes", "Active_file"), ("slab_bytes", "Slab")):
        fields[dest] = get(source)
    fields["working_set_bytes"] = combine([get(f) for f in ("AnonPages", "Active_file", "Slab")])
    if host["backend"] == "whisper":
        fields["store_bytes"] = get("whisper_store_bytes")
    # Metadata arenas are Go-backed (state_slab.go). Remove their retained
    # bytes before applying a target GC factor, then budget the target cache.
    subtract = lambda values: max(0, next(values) - sum(values))
    fields["application_heap_live_bytes"] = combine([get("heap_live_bytes"), get("metadata_cache_bytes")], subtract)
    fields["application_heap_live_bytes"] = {t: value for t, value in fields["application_heap_live_bytes"].items()
                                             if get("heap_live_bytes")[t] >= get("metadata_cache_bytes")[t]}
    # Heap resident = objects + unscavenged free/unused spans + stacks. Released
    # pages are not RSS. Subtract external C-backed block cache/memtables too;
    # metadata was already removed with the Go heap and must not be subtracted twice.
    fields["non_heap_anon_bytes"] = combine([get(f) for f in ("process_rss_bytes", "go_heap_objects_bytes",
        "go_heap_free_bytes", "go_heap_unused_bytes", "go_heap_stacks_bytes", "block_cache_bytes", "memtable_bytes")], subtract)
    devices, device_curves = {}, defaultdict(list)
    for device in host["store_devices"]:
        def dget(field):
            return raw.get(field, {}).get(short + "." + device, {})
        parts = {"write_mbps": {t: v / 1e6 for t, v in dget("device_write_bytes_s").items()},
                 "device_util_percent": dget("device_util_percent"),
                 "device_await_ms": combine([dget("device_read_await_ms"), dget("device_write_await_ms")], max),
                 "iops": combine([dget("device_read_iops"), dget("device_write_iops")])}
        devices[device] = parts
        for field, values in parts.items():
            device_curves[field].append(values)
    for field, values in device_curves.items():
        fields[field] = combine(values, max if field in ("device_util_percent", "device_await_ms") else sum)
    return fields, devices


def collect(inventory, jobs, window, state_dir, http, token, concurrency=2, rps=1, timeout=60,
            offline=False, exclusions=()):
    state_dir = Path(state_dir)
    if not offline:
        state_dir.mkdir(parents=True, exist_ok=True)
    limiter = RateLimit(rps)

    def run(job):
        key = digest({"job": job, "window": window})
        path = state_dir / (key + ".json.gz")
        if not path.exists():
            if offline:
                raise ValueError(f"offline replay needs cached query {key}")
            limiter.wait()
            response = fetch(http, token, job, window, timeout)
            normalize(job, response)
            temporary = path.with_suffix(".tmp")
            with gzip.open(temporary, "wt") as out:
                json.dump(response, out, allow_nan=False)
            os.replace(temporary, path)
        return key, path

    groups = defaultdict(list)
    for job in jobs:
        groups[(job["cluster"], job["site"])].append(job)
    def stats(values, masked=True):
        return summarize(values, window["start"], window["end"], window["step_seconds"], exclusions if masked else ())
    for group, group_jobs in groups.items():
        hosts_out, queries_out, group_curves = [], [], []
        raw = {"sustained": {}, "burst": {}}
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            results = list(pool.map(run, group_jobs))
        for job, (key, path) in zip(group_jobs, results):
            with gzip.open(path, "rt") as source:
                values = normalize(job, json.load(source))
            role = job.get("role", "sustained")
            raw[role][job["field"]] = {**raw[role].get(job["field"], {}), **values}
            # Mimir samples serve both analyses. Legacy Graphite has no separate
            # burst query; preserve its existing upper bounds without relabeling.
            if job["source"] == "mimir" or "role" not in job:
                raw["burst"][job["field"]] = {**raw["burst"].get(job["field"], {}), **values}
            queries_out.append({**job, "cache_key": key})
        for host in inventory["hosts"]:
            if (host["cluster"], host["site"]) != group:
                continue
            fields, devices = curves(raw["sustained"], host)
            bursts, _ = curves(raw["burst"], host)
            summary = {field: stats(values) for field, values in fields.items()}
            short = host["host"].split(".")[0]
            events = {f: raw["sustained"].get(f, {}).get(short, {}) for f in EVENT_FIELDS}
            hosts_out.append({**host, "metrics": summary,
                "devices": {device: {f: stats(s) for f, s in parts.items()} for device, parts in devices.items()},
                "burst_metrics": {field: stats(values, False) for field, values in bursts.items()},
                "activity": activity(events, window["start"], window["end"], window["step_seconds"]),
                "memory_accounting": "cgo block cache/memtables outside Go heap; metadata arenas inside Go heap; RSS residual deduplicated",
                "cpu_normalization": "system logical-CPU sum / cpu_count, expressed in inventory physical cores",
                "excluded": not has_go_carbon({"metrics": summary})})
            if has_go_carbon({"metrics": summary}):
                group_curves.append(fields)
        cluster_fields = {f: stats(combine([c[f] for c in group_curves], min if f.endswith("_hit_ratio") else max if f in
                          ("device_util_percent", "device_await_ms", "read_p99_seconds") else sum))
                          for f in FIELDS + GUARDS + OPTIONAL_FIELDS + MEMORY_FIELDS}
        yield hosts_out, {"cluster": group[0], "site": group[1], "host_count": len(hosts_out),
                          "included_host_count": len(group_curves), "excluded_host_count": len(hosts_out) - len(group_curves),
                          "metrics": cluster_fields}, queries_out


def replay_snapshot(original, state_dir, exclusions=(), inventory=None):
    exclusions = merge(list(exclusions) + [(x["start"], x["end"]) for x in original.get("exclusions", [])])
    if not complement(original["window"]["start"], original["window"]["end"], exclusions):
        raise ValueError("exclusions cover the entire observation window")
    inventory = inventory or {"hosts": original["hosts"]}
    jobs = [{k: v for k, v in job.items() if k != "cache_key"} for job in original["queries"]]
    if not jobs:
        raise ValueError("snapshot has no raw-query provenance; exclusions require cached series")
    rebuilt = {**original, "hosts": [], "clusters": [], "queries": [], "inventory_sha256": digest(inventory),
               "replayed_from_sha256": digest(original)}
    for hosts, cluster, queries in collect(inventory, jobs, original["window"], state_dir, None, None,
                                           offline=True, exclusions=exclusions):
        rebuilt["hosts"].extend(hosts)
        rebuilt["clusters"].append(cluster)
        rebuilt["queries"].extend(queries)
    analysis_metadata(rebuilt, state_dir, exclusions)
    return rebuilt


def analysis_metadata(snapshot, state_dir, exclusions):
    average = all(j.get("consolidation") == "average" for j in snapshot["queries"]
                  if j["source"] == "graphite" and j.get("role") != "burst")
    snapshot["query_cache"] = str(Path(state_dir).resolve())
    snapshot["exclusions"] = describe(exclusions)
    snapshot["sustained_seconds"] = sum(b-a for a,b in complement(snapshot["window"]["start"], snapshot["window"]["end"], exclusions))
    snapshot["graphite_sustained_consolidation"] = "average" if average else "legacy-max-upper-bound"
    snapshot["semantics"] = "average-consolidated sustained percentiles with exclusions; separate full-window burst peaks" if average else \
                            "legacy max-consolidated upper bounds with exclusions; averages cannot be recovered"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--output", default="snapshot.json")
    parser.add_argument("--state-dir", default="sizing-state")
    parser.add_argument("--days", type=float, default=7)
    parser.add_argument("--step", type=int, default=60)
    parser.add_argument("--end", type=int, help="epoch seconds; defaults to the last complete step")
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--requests-per-second", type=float, default=1)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--prometheus-url", help="Prometheus API base URL")
    parser.add_argument("--graphite-url", help="Graphite base URL")
    parser.add_argument("--token-env", default="METRICS_TOKEN", help="optional bearer token environment variable")
    parser.add_argument("--collect", action="store_true", help="execute read-only queries (default: print plan)")
    parser.add_argument("--replay-snapshot", help="rebuild a snapshot from its query cache only; no token/network access")
    parser.add_argument("--exclude", action="append", default=[], metavar="START-END", help="exclude a UTC/epoch window from sustained statistics only; repeatable")
    args = parser.parse_args(argv)
    try:
        for field in ("days", "step", "concurrency", "requests_per_second", "timeout"):
            positive(getattr(args, field), field)
        if args.concurrency > 8 or args.requests_per_second > 10:
            raise ValueError("query budgets exceed concurrency=8 or requests/second=10")
        if args.days * 86400 / args.step > 10999:
            raise ValueError("window exceeds 11,000 points; increase --step")
        if args.days * 86400 < args.step:
            raise ValueError("window must span at least one step")
        inventory = load_inventory(args.inventory)
        exclusions = parse_exclusions(args.exclude)
        if exclusions:
            print("Excluded from sustained statistics: " + json.dumps(describe(exclusions)), file=sys.stderr)
        if args.replay_snapshot:
            if args.collect:
                raise ValueError("--replay-snapshot and --collect are mutually exclusive")
            original = read_json(args.replay_snapshot)
            print(f"Offline replay -> {args.output}", file=sys.stderr)
            rebuilt = replay_snapshot(original, args.state_dir, exclusions, inventory)
            write_json(args.output, rebuilt)
            return 0
        jobs = query_plan(inventory)
        end = args.end if args.end is not None else (int(time.time()) // args.step - 1) * args.step
        window = {"start": end - int(args.days * 86400), "end": end, "step_seconds": args.step}
        if not complement(window["start"], window["end"], exclusions):
            raise ValueError("exclusions cover the entire observation window")
        identity = {"inventory": digest(inventory), "jobs": digest(jobs), "window": window}
        manifest = Path(args.state_dir) / "manifest.json"
        if manifest.exists():
            previous = read_json(manifest)
            # A resume without --end retains the original pinned window.
            if args.end is None:
                identity["window"]["start"] = previous["window"]["end"] - int(args.days * 86400)
                identity["window"]["end"] = previous["window"]["end"]
            if identity != previous:
                raise ValueError("state directory belongs to another inventory/query/window; use a new directory")
        print(json.dumps({"mode": "collect" if args.collect else "dry-run", "window": window,
                          "queries": jobs, "exclusions": describe(exclusions), "output": args.output}, indent=2), file=sys.stderr)
        if not args.collect:
            return 0
        http = MetricsHTTP(args.prometheus_url, args.graphite_url)
        token = os.environ.get(args.token_env, "")
        Path(args.state_dir).mkdir(parents=True, exist_ok=True)
        with (Path(args.state_dir) / "collector.lock").open("w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError("another collector is using this state directory") from None
            # Recheck after acquiring the lock: another controller may have
            # completed between our first manifest check and this lock.
            if manifest.exists() and read_json(manifest) != identity:
                raise ValueError("state identity changed; use a new state directory")
            write_json(manifest, identity)
            snapshot = {"schema_version": 1, "window": window, "inventory_sha256": digest(inventory),
                        "hosts": [], "clusters": [], "queries": [],
                        "semantics": "sampled peaks; nearest-rank p95; cluster p99 = worst host p99"}
            for hosts, cluster, queries in collect(inventory, jobs, window, args.state_dir, http, token,
                                                   args.concurrency, args.requests_per_second, args.timeout, exclusions=exclusions):
                snapshot["hosts"].extend(hosts)
                snapshot["clusters"].append(cluster)
                snapshot["queries"].extend(queries)
            analysis_metadata(snapshot, args.state_dir, exclusions)
            write_json(args.output, snapshot)
        missing = [f'{h["host"]}:{f}' for h in snapshot["hosts"] for f in FIELDS
                   if not h.get("excluded") and h["metrics"][f]["coverage"] < .90]
        excluded = [h["host"] for h in snapshot["hosts"] if h.get("excluded")]
        if excluded:
            print("excluded (no go_carbon metrics): " + ", ".join(excluded), file=sys.stderr)
        if missing:
            print("incomplete telemetry (snapshot saved): " + ", ".join(missing), file=sys.stderr)
            return 2
        return 0
    except (ValueError, OSError, KeyError, TypeError, ImportError) as error:
        print(f"collector: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
