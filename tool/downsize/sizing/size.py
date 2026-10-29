#!/usr/bin/env python3
"""Offline, capacity-only node sizing. Never changes a ring or a host."""
import argparse
import math
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

from common import FIELDS, GUARDS, digest, has_go_carbon, positive, read_json, write_json
from memory import cache_budgets, ram_model
from windows import parse_exclusions, timestamp

GIB = 2 ** 30


@dataclass
class Policy:
    # User, 29 Oct 2026: use ~70% of CPU, memory, IOPS, write bandwidth and space at the sustained
    # percentile, with one node down (N+1). The target is the headroom, so the old 1.15/1.25
    # multipliers stacked on a 35% CPU target are gone; bursts are checked separately.
    sustained_percentile: str = "p95"
    ram_mode: str = "components"
    go_gc_factor: float = 1.5
    block_cache_fraction: float = .08
    metadata_cache_fraction: float = .04
    cpu_target: float = .70
    memory_target: float = .70
    disk_target: float = .70
    write_target: float = .70
    device_util_target: float = .70
    await_limit_ms: float = 20
    nic_target: float = .70
    iops_target: float = .70
    imbalance: float = 1.15
    cpu_headroom: float = 1.0
    memory_headroom: float = 1.0
    compaction_headroom: float = 1.0
    conversion_headroom: float = 1.10
    conversion_cpu_factor: float = .75
    conversion_memory_factor: float = .65
    conversion_disk_factor: float = 1.0
    # During conversion an unreclaimed original and its converted copy coexist.
    conversion_disk_headroom: float = 2.0
    growth_days: float = 90
    growth_min_days: float = 7
    reserved_ram_gib: float = 4
    reserved_disk_gb: float = 30
    input_bytes_per_point: float = 40
    read_bytes_per_request: float = 65536
    coverage_min: float = .90
    read_p99_limit_seconds: float = 1.0

    def validate(self):
        for name, value in asdict(self).items():
            if name in ("sustained_percentile", "ram_mode"):
                allowed = ("p95", "p99") if name == "sustained_percentile" else ("components", "legacy")
                if value not in allowed:
                    raise ValueError(f"{name} must be one of {allowed}")
                continue
            positive(value, name, zero=name in ("growth_days", "growth_min_days", "reserved_ram_gib", "reserved_disk_gb",
                                               "block_cache_fraction", "metadata_cache_fraction"))
            if name.endswith("_target") or name == "coverage_min":
                if value > 1:
                    raise ValueError(f"{name}: fraction must be <= 1")
            if name.endswith("headroom") or name == "imbalance":
                if value < 1:
                    raise ValueError(f"{name}: must be >= 1")
        if not .30 <= self.cpu_target <= .85:
            raise ValueError("cpu_target must be between .30 and .85")
        if self.go_gc_factor < 1:
            raise ValueError("go_gc_factor must be >= 1")


def validate_shapes(shapes):
    seen = set()
    for shape in shapes:
        name = shape["name"]
        if not isinstance(name, str) or not name or name in seen:
            raise ValueError("shape names must be unique and nonempty")
        seen.add(name)
        for key in ("cores", "ram_gib", "disk_tb", "nic_gbps", "disk_write_mbps"):
            positive(shape[key], f"{name}.{key}")
        if int(shape["cores"]) != shape["cores"]:
            raise ValueError(f"{name}.cores must be an integer")
        if "logical_cpus" in shape and (positive(shape["logical_cpus"], f"{name}.logical_cpus") < shape["cores"]
                                        or int(shape["logical_cpus"]) != shape["logical_cpus"]):
            raise ValueError(f"{name}.logical_cpus must be an integer >= physical cores")
        for key in ("cpu_per_core_factor", "disk_iops", "max_input_points_s", "max_read_qps",
                    "max_metrics", "monthly_cost"):
            if key in shape:
                positive(shape[key], f"{name}.{key}")
        for key in ("block_cache_gib", "metadata_cache_gib"):
            if key in shape:
                positive(shape[key], f"{name}.{key}", zero=True)
        if "go_gc_factor" in shape and positive(shape["go_gc_factor"], f"{name}.go_gc_factor") < 1:
            raise ValueError("shape go_gc_factor must be >= 1")
    if not shapes:
        raise ValueError("need at least one shape")


def validate_snapshot(snapshot, policy):
    if snapshot.get("schema_version") != 1 or not snapshot.get("hosts"):
        raise ValueError("need a schema_version=1 snapshot with hosts")
    window = snapshot["window"]
    if window["end"] <= window["start"]:
        raise ValueError("snapshot window must have positive duration")
    positive(window["step_seconds"], "step_seconds")
    seen = set()
    for host in snapshot["hosts"]:
        name = host["host"]
        if name in seen:
            raise ValueError(f"duplicate snapshot host: {name}")
        seen.add(name)
        if host["backend"] not in ("pebble-chunk", "whisper"):
            raise ValueError(f"{name}: unknown backend")
        for field in FIELDS:
            stats = host["metrics"].get(field, {})
            positive(stats.get("coverage", 0), f"{name}.{field}.coverage", zero=True)
            if stats.get("peak") is None or stats.get(policy.sustained_percentile) is None or stats.get("last") is None:
                raise ValueError(f"{name}: missing {field}; collect complete telemetry")
            for key in ("peak", policy.sustained_percentile, "last"):
                positive(stats[key], f"{name}.{field}.{key}", zero=True)
            if stats["coverage"] < policy.coverage_min or stats["coverage"] > 1:
                raise ValueError(f"{name}: insufficient {field} coverage ({stats['coverage']:.1%})")
            if (stats.get("last_at", 0) < stats.get("eligible_end", window["end"]) - 2 * window["step_seconds"] or
                    stats.get("first_at", window["end"]) > stats.get("eligible_start", window["start"]) + 2 * window["step_seconds"]):
                raise ValueError(f"{name}: stale or truncated {field} window")
            if stats[policy.sustained_percentile] > stats["peak"] or stats["last"] > stats["peak"]:
                raise ValueError(f"{name}.{field}: inconsistent summary")
        growth = host["metrics"]["store_bytes"].get("growth_per_day")
        if growth is None or not math.isfinite(growth):
            raise ValueError(f"{name}: missing store growth/day")


def go_config(shape, points_s, policy=None, memory=None, survivors=1):
    """Bounded starting budgets, using only settings in go-carbon.conf.example."""
    ram = shape["ram_gib"] * GIB
    policy = policy or Policy()
    block, metadata = cache_budgets(shape, policy)
    gc_factor = shape.get("go_gc_factor", policy.go_gc_factor)
    heap_limit = ram * .40
    if memory and memory["mode"] == "components":
        go_heap = sum(row["application_heap_live_bytes"] for row in memory["datapoints"]) * gc_factor * memory["skew"] / survivors
        non_go = memory["component_variable_bytes"] / survivors - go_heap
        available = ram * policy.memory_target - policy.reserved_ram_gib * GIB - block - non_go
        heap_limit = min(available, max(heap_limit, go_heap + metadata * gc_factor))
    cores = int(shape["cores"])
    logical = int(shape.get("logical_cpus", cores))
    # The worst cache density is one point per metric: the exported cache
    # estimate charges 16 bytes/point plus about 160 bytes/metric, not 16 alone.
    point_budget = int(ram * .12 / 176)
    delay = max(1, min(60, int(point_budget / max(1, points_s * 1.5))))
    cache_points = max(10000, math.ceil(points_s * delay * 1.5))
    mib = lambda value: int(value // (2 ** 20)) * 2 ** 20
    return {
        "env": {"GOMEMLIMIT": str(int(heap_limit)), "GOMAXPROCS": str(logical), "GOGC": str(round((gc_factor - 1) * 100))},
        "toml": {
            "common": {"max-cpu": logical},
            "whisper": {
                "storage-backend": "pebble-chunk", "store-encoding": "exact",
                "workers": min(64, max(2, cores // 2)),
                "store-cache-size": mib(block + ram * .04),
                "store-metadata-cache-size": mib(metadata),
                "store-memtable-size": mib(min(256 * 2 ** 20, ram * .01)),
                "store-memtable-max-unflushed-bytes": mib(ram * .04),
                "store-compaction-workers": min(16, max(2, cores // 4)),
            },
            "cache": {"max-size": cache_points, "writeout-min-points": 8,
                      "writeout-max-delay": f"{delay}s", "writeout-max-delay-spread": .25},
        },
        "cache_budget_ok": cache_points <= point_budget,
        "requires": ["Merge these budgets into the current config; retain paths, exact store and input log.",
                     "Do not change WAL/input-log durability or ring settings as part of sizing.",
                     "Validate with go-carbon -check-config and replay the measured peak workload.",
                     "Canary persister/compaction workers against debt, stalls and read p99 before rollout."],
        "memory_budget": {"go_heap_limit_gib": heap_limit / GIB,
                          "store_cache_gib": block / GIB + shape["ram_gib"] * .04,
                          "metadata_cache_gib": metadata / GIB,
                          "metadata_is_within_go_heap": True,
                          "memtables_included_in_store_cache_gib": shape["ram_gib"] * .04},
    }


def read_ring_plan(spec_path, summary_path, shape):
    """Adapt ds-ring's native spec and summary; never run it or fetch inventories."""
    spec, summary = read_json(spec_path), read_json(summary_path)
    if spec.get("version") != 1 or summary.get("unowned") != 0:
        raise ValueError("need a ds-ring v1 spec and a summary with unowned=0")
    total = positive(summary["metrics"], "ring summary metrics")
    moved = positive(summary["moved_metrics"], "ring summary moved_metrics", zero=True)
    if moved > total or not math.isclose(summary["moved_fraction"], moved / total, abs_tol=1e-9):
        raise ValueError("inconsistent ds-ring movement summary")

    def members(ring):
        # Hash identity includes port/index; the capacity inventory uses FQDNs.
        return [node["node"].split("=", 1)[0].split(":", 1)[0] for node in ring["nodes"]]

    return {"cluster": spec["cluster"], "site": spec["site"], "shape": shape,
            "nodes": len(spec["new"]["nodes"]), "old_hosts": members(spec["old"]),
            "new_hosts": members(spec["new"]), "total_metrics": total,
            "moved_metrics": moved, "moved_bytes": summary.get("estimated_bytes"),
            "moved_bytes_estimated": True,
            "new_physical_ids": [node.get("id") for node in spec["new"]["nodes"]],
            "new_endpoints": [node.get("endpoint") for node in spec["new"]["nodes"]],
            "native_spec_sha256": digest(spec), "native_summary_sha256": digest(summary)}


def movement(plans, cluster, site, shape, hosts, nodes, total_metrics):
    old = sorted(h["host"] for h in hosts)
    matches = [p for p in plans if (p["cluster"], p["site"], p["shape"], p["nodes"]) ==
               (cluster, site, shape["name"], nodes)]
    if len(matches) > 1:
        raise ValueError("ambiguous ring plans")
    if matches:
        plan = matches[0]
        if (sorted(plan["old_hosts"]) != old or len(plan["new_hosts"]) != nodes or
                len(set(plan["new_hosts"])) != nodes):
            raise ValueError("ring plan membership does not match snapshot/candidate")
        total = positive(plan["total_metrics"], "plan.total_metrics")
        moved = positive(plan["moved_metrics"], "plan.moved_metrics", zero=True)
        if moved > total:
            raise ValueError("ring plan moved_metrics exceeds total_metrics")
        replacement = not all(h.get("shape") == shape["name"] for h in hosts)
        # Tiny strategy samples must not turn a TB-scale copy estimate into KB.
        # Allow ordinary cardinality drift between a 7d peak and the inventory.
        sampled = not math.isclose(total, total_metrics, rel_tol=.10)
        return {"method": "ds-ring-sample" if sampled else "ds-ring", "fraction": moved / total,
                "copy_fraction": 1.0 if replacement else moved / total,
                "moved_metrics": moved, "total_metrics": total,
                "new_hosts": plan["new_hosts"], "plan_sha256": digest(plan),
                "new_physical_ids": plan.get("new_physical_ids"),
                "new_endpoints": plan.get("new_endpoints"),
                "native_spec_sha256": plan.get("native_spec_sha256"),
                "native_summary_sha256": plan.get("native_summary_sha256"),
                "moved_bytes": None if sampled else plan.get("moved_bytes"),
                "moved_bytes_estimated": plan.get("moved_bytes_estimated", False),
                "note": "Inventory count differs from snapshot by >10%; fraction is a sample/drift estimate." if sampled else
                        "Planner evidence applies only to these memberships and this snapshot's workload."}
    # Ideal movement for retaining a subset of equal, balanced owners.
    # Replacement VMs have new identities: every byte must be copied even if
    # the ingestion ring reuses the old owner names and changes zero hashes.
    replacement = not all(h.get("shape") == shape["name"] for h in hosts)
    fraction = 1.0 if replacement else abs(len(hosts) - nodes) / max(len(hosts), nodes)
    return {"method": "estimate", "fraction": fraction, "copy_fraction": fraction,
            "moved_metrics": math.ceil(total_metrics * fraction),
            "note": "Full-copy VM replacement estimate." if replacement else
                    "Balanced consistent-hash estimate; reindexing can move more. Obtain the ds-ring plan."}


def calculate(snapshot, shapes, policy=None, plans=(), estimate_whisper=False, inventory=None):
    policy = policy or Policy()
    policy.validate()
    validate_shapes(shapes)
    if snapshot.get("schema_version") != 1 or not snapshot.get("hosts"):
        raise ValueError("need a schema_version=1 snapshot with hosts")
    hosts_by_name = {h["host"]: h for h in snapshot["hosts"]}
    if len(hosts_by_name) != len(snapshot["hosts"]):
        raise ValueError("duplicate snapshot host")
    if inventory:
        sites = {h["site"] for h in snapshot["hosts"]}
        for facts in inventory["hosts"]:
            if facts["site"] in sites:
                old = hosts_by_name.get(facts["host"], {"metrics": {}})
                hosts_by_name[facts["host"]] = {**old, **facts}
    included = [h for h in hosts_by_name.values() if has_go_carbon(h)]
    if included:
        validate_snapshot({**snapshot, "hosts": included}, policy)
    excluded = [{"host": h["host"], "cluster": h["cluster"], "site": h["site"],
                 "reason": "no go_carbon metrics", "retain": True}
                for h in hosts_by_name.values() if not has_go_carbon(h)]
    groups = defaultdict(list)
    for host in hosts_by_name.values():
        groups[(host["cluster"], host["site"])].append(host)
    shape_map = {s["name"]: s for s in shapes}
    window_days = snapshot.get("sustained_seconds", snapshot["window"]["end"] - snapshot["window"]["start"]) / 86400
    growth_applied = window_days >= policy.growth_min_days
    percentile = policy.sustained_percentile
    report = {"schema_version": 1, "snapshot_sha256": digest(snapshot), "mode": "capacity-only",
              "inventory_sha256": digest(inventory) if inventory else None,
              "policy": asdict(policy), "excluded_hosts": excluded, "clusters": [],
              "exclusions": snapshot.get("exclusions", []),
              "graphite_sustained_consolidation": snapshot.get("graphite_sustained_consolidation", "legacy-max-upper-bound"),
              "semantics": f"sum of per-host {percentile} drives sustained capacity; peaks are separate headroom checks"}
    for (cluster, site), all_hosts in sorted(groups.items()):
        hosts = [h for h in all_hosts if has_go_carbon(h)]
        omitted = [h for h in excluded if (h["cluster"], h["site"]) == (cluster, site)]
        group = {"cluster": cluster, "site": site, "current_nodes": len(all_hosts),
                 "included_nodes": len(hosts), "excluded_nodes": len(omitted), "excluded_hosts": omitted,
                 "candidates": [], "recommended_shape": None}
        report["clusters"].append(group)
        if not hosts:
            group.update(status="excluded", reason="no go_carbon metrics; retain these hosts")
            continue
        forecasting = any(h["backend"] == "whisper" for h in hosts)
        if forecasting and not estimate_whisper:
            group.update(status="blocked", reason="Whisper: use --estimate-whisper for a conditional forecast")
            continue
        demand, peaks = defaultdict(float), defaultdict(float)
        source_data = defaultdict(list)
        execution_blockers, warnings, unverified = [], [], []
        if snapshot.get("graphite_sustained_consolidation") != "average":
            warnings.append("Legacy Graphite max-consolidated points: sustained percentiles remain upper bounds; averages cannot be recovered. Recollect with average consolidation.")
        for host in hosts:
            m = host["metrics"]
            source_shape = shape_map.get(host.get("shape"))
            if source_shape and m["write_mbps"][percentile] > source_shape["disk_write_mbps"]:
                warnings.append(f'{host["host"]}: write {percentile}={m["write_mbps"][percentile]:.1f} MB/s exceeds '
                                f'source ceiling={source_shape["disk_write_mbps"]} MB/s while util '
                                f'{percentile}={m["device_util_percent"][percentile]:.1f}%; verify ceiling/accounting and a stable window')
            if source_shape is None:
                warnings.append(f'{host["host"]}: source disk ceiling unknown; utilization scaling assumes 1100 MB/s')
            whisper = host["backend"] == "whisper"
            for field in FIELDS:
                factor = policy.conversion_cpu_factor if whisper and field == "cpu_cores" else (
                    policy.conversion_memory_factor if whisper and field == "working_set_bytes" else (
                    policy.conversion_disk_factor if whisper and field == "store_bytes" else 1))
                burst_peak = host.get("burst_metrics", {}).get(field, {}).get("peak")
                observed, peak = m[field][percentile] * factor, (m[field]["peak"] if burst_peak is None else burst_peak) * factor
                if field == "read_p99_seconds":
                    demand[field], peaks[field] = max(demand[field], observed), max(peaks[field], peak)
                else:
                    demand[field] += observed
                    peaks[field] += peak
                source_data[field].append({"host": host["host"], "percentile": percentile,
                                           "value": m[field][percentile], "peak": m[field]["peak"],
                                           "last": m[field]["last"], "conversion_factor": factor})
            nic = {key: (m["input_points_per_second"][key] * policy.input_bytes_per_point +
                         m["read_requests_per_second"][key] * policy.read_bytes_per_request) / 1e6
                   for key in (percentile, "peak", "last")}
            source_data["nic_mbps"].append({"host": host["host"], "percentile": percentile,
                                            "value": nic[percentile], "peak": nic["peak"], "last": nic["last"]})
            if m["cpu_cores"]["peak"] == 0 and m["input_points_per_second"][percentile] > 0:
                unverified.append(f'{host["host"]}: CPU is zero despite ingest; replay raw data with cpu_count/logical_cpus')
            for field in GUARDS:
                stats = m.get(field, {})
                if stats.get("peak") is None or stats.get("coverage", 0) < policy.coverage_min:
                    unverified.append(f'{host["host"]}: missing {field}')
                    continue
                positive(stats["peak"], f'{host["host"]}.{field}.peak', zero=True)
                if stats.get("last", 0) > 0 or stats.get(percentile, 0) > 0:
                    execution_blockers.append(f'{host["host"]}: {field} {percentile}={stats.get(percentile)}, last={stats.get("last")}')
                elif stats["peak"] > 0:
                    warnings.append(f'{host["host"]}: historical {field} peak={stats["peak"]}; sustained/last are zero')
            latency = m["read_p99_seconds"]
            if max(latency[percentile], latency["last"]) > policy.read_p99_limit_seconds:
                execution_blockers.append(f'{host["host"]}: sustained/current read p99 exceeds policy')
            elif latency["peak"] > policy.read_p99_limit_seconds:
                warnings.append(f'{host["host"]}: historical read p99 burst={latency["peak"]}s')
        growth = sum(max(0, h["metrics"]["store_bytes"]["growth_per_day"]) * (
            policy.conversion_disk_factor if h["backend"] == "whisper" else 1) for h in hosts)
        skew = policy.imbalance * (policy.conversion_headroom if forecasting else 1)
        # Observed p95 I/O already includes compaction. Do not compound a CPU
        # target and conversion/compaction margins onto the device ceiling.
        required = {
            "cpu_cores": demand["cpu_cores"] * policy.cpu_headroom * skew,
            "ram_bytes": demand["working_set_bytes"] * policy.memory_headroom * skew,
            "disk_bytes": (demand["store_bytes"] + (growth * policy.growth_days if growth_applied else 0)) *
                          policy.compaction_headroom * skew * (policy.conversion_disk_headroom if forecasting else 1),
            "write_mbps": demand["write_mbps"] * skew,
            "device_busy_mbps": sum(h["metrics"]["device_util_percent"][percentile] / 100 *
                                    shape_map.get(h.get("shape"), {}).get("disk_write_mbps", 1100)
                                    for h in hosts) * skew,
            "nic_mbps": (demand["input_points_per_second"] * policy.input_bytes_per_point +
                         demand["read_requests_per_second"] * policy.read_bytes_per_request) / 1e6 * skew,
            "iops": demand["iops"] * skew,
            "input_points_s": demand["input_points_per_second"] * skew,
            "read_qps": demand["read_requests_per_second"] * skew,
            "metrics": demand["metric_count"] * skew,
        }
        raw_peaks = {"cpu_cores": peaks["cpu_cores"], "ram_bytes": peaks["working_set_bytes"],
                     "disk_bytes": peaks["store_bytes"], "write_mbps": peaks["write_mbps"],
                     "iops": peaks["iops"], "nic_mbps": (peaks["input_points_per_second"] * policy.input_bytes_per_point +
                                                         peaks["read_requests_per_second"] * policy.read_bytes_per_request) / 1e6}
        fields = {"cpu_cores": "cpu_cores", "ram_bytes": "working_set_bytes", "disk_bytes": "store_bytes",
                  "write_mbps": "write_mbps", "device_busy_mbps": "device_util_percent", "iops": "iops",
                  "input_points_s": "input_points_per_second", "read_qps": "read_requests_per_second", "metrics": "metric_count",
                  "nic_mbps": "nic_mbps"}
        demand["nic_mbps"] = sum(d["value"] for d in source_data["nic_mbps"])
        if not growth_applied and growth:
            warnings.append(f"{window_days:g}-day window is shorter than growth_min_days={policy.growth_min_days:g}; growth projection is a headroom check only")
        if forecasting:
            warnings.append("Whisper conversion factors are assumptions; recollect after conversion")
        if omitted:
            warnings.append("Capacity covers included workload only; excluded hosts must remain and require a separate complete ring plan")
        await_values = [h["metrics"].get("device_await_ms", {}).get(percentile) for h in hosts]
        if any(v is None for v in await_values):
            warnings.append("Device await is unavailable; validate latency before a move")
        candidates = []
        for shape in shapes:
            memory = ram_model(hosts, shape, policy, percentile, skew)
            candidate_required = dict(required)
            hard = {"cpu_cores": shape["cores"] * shape.get("cpu_per_core_factor", 1),
                    "ram_bytes": (shape["ram_gib"] - policy.reserved_ram_gib) * GIB,
                    "disk_bytes": shape["disk_tb"] * 1e12 - policy.reserved_disk_gb * 1e9,
                    "write_mbps": shape["disk_write_mbps"], "nic_mbps": shape["nic_gbps"] * 125}
            capacity = {"cpu_cores": hard["cpu_cores"] * policy.cpu_target,
                        "ram_bytes": shape["ram_gib"] * GIB * policy.memory_target - policy.reserved_ram_gib * GIB,
                        "disk_bytes": shape["disk_tb"] * 1e12 * policy.disk_target - policy.reserved_disk_gb * 1e9,
                        "write_mbps": hard["write_mbps"] * policy.write_target,
                        "device_busy_mbps": hard["write_mbps"] * policy.device_util_target,
                        "nic_mbps": hard["nic_mbps"] * policy.nic_target}
            legacy_ram_capacity = capacity["ram_bytes"]
            if memory["mode"] == "components":
                candidate_required["ram_bytes"] = memory["component_variable_bytes"]
                capacity["ram_bytes"] -= memory["fixed_cache_bytes_per_node"]
            for key, source, target in (("iops", "disk_iops", policy.iops_target),
                                       ("input_points_s", "max_input_points_s", 1),
                                       ("read_qps", "max_read_qps", 1), ("metrics", "max_metrics", 1)):
                if source in shape:
                    capacity[key] = shape[source] * target
                    hard[key] = shape[source]
            if any(v <= 0 for v in capacity.values()):
                raise ValueError(f'{shape["name"]}: shape is smaller than per-node reserves')
            counts = {key: max(1, math.ceil(candidate_required[key] / value)) + 1 for key, value in capacity.items()}
            nodes = max(counts.values())
            memory = ram_model(hosts, shape, policy, percentile, skew, nodes - 1)
            memory["legacy_nodes_for_ram"] = max(1, math.ceil(required["ram_bytes"] / legacy_ram_capacity)) + 1
            memory["component_nodes_for_ram"] = max(1, math.ceil(memory["component_variable_bytes"] /
                (legacy_ram_capacity - memory["fixed_cache_bytes_per_node"]))) + 1 if memory["components_available"] else None
            binding = [key for key, count in counts.items() if count == nodes]
            drivers = [{"resource": key, "percentile": percentile, "source_metric": fields[key],
                        "sustained_total": demand[fields[key]],
                        "adjusted_total": candidate_required[key], "per_survivor_budget": capacity[key],
                        "datapoints": source_data.get(fields.get(key), [])} for key in binding]
            if memory["mode"] == "components":
                for driver in drivers:
                    if driver["resource"] == "ram_bytes":
                        driver.update(source_metric="component_ram", sustained_total=memory["component_variable_bytes"] / skew,
                                      datapoints=memory["datapoints"])
            checks = [{"resource": key, "observed_peak_sum": value,
                       "after_loss_fraction_of_target": value * skew / ((nodes - 1) * capacity[key]),
                       "after_loss_fraction_of_ceiling": value * skew / ((nodes - 1) * hard[key]),
                       "exceeds_ceiling": value * skew > (nodes - 1) * hard[key]}
                      for key, value in raw_peaks.items() if key in hard]
            if memory["mode"] == "components":
                checks = [check for check in checks if check["resource"] != "ram_bytes"]
                peak_ram = memory["component_peak_variable_bytes"] / (nodes - 1) + memory["fixed_cache_bytes_per_node"]
                checks.append({"resource": "ram_bytes", "basis": "components", "after_loss_peak_bytes": peak_ram,
                               "after_loss_fraction_of_target": peak_ram / legacy_ram_capacity,
                               "after_loss_fraction_of_ceiling": peak_ram / hard["ram_bytes"],
                               "exceeds_ceiling": peak_ram > hard["ram_bytes"]})
            for host, value in zip(hosts, await_values):
                if value is not None:
                    checks.append({"resource": "device_await_ms", "host": host["host"], "percentile": percentile,
                                   "value": value, "limit": policy.await_limit_ms, "exceeds_ceiling": value > policy.await_limit_ms})
            checks.append({"resource": "growth_projection", "applied_to_sizing": growth_applied,
                           "days": policy.growth_days, "positive_bytes_per_day": growth,
                           "projected_fraction_of_disk_target": (demand["store_bytes"] + growth * policy.growth_days) *
                               policy.compaction_headroom * skew / ((nodes - 1) * capacity["disk_bytes"])})
            move = movement(plans, cluster, site, shape, hosts, nodes, peaks["metric_count"])
            config = go_config(shape, required["input_points_s"] / (nodes - 1), policy, memory, nodes - 1)
            notes = list(warnings)
            if memory["mode"] != "components" and policy.ram_mode == "components":
                notes.append("RAM components missing: legacy AnonPages+Active_file+Slab fallback; no component demand was inferred")
            if memory["mode"] == "components":
                notes.append(memory["hit_rate_implications"])
            notes.extend(f"{key}: {value}" for key, value in shape.get("assumptions", {}).items())
            if shape.get("disk_write_mbps_assumed"):
                notes.append(f'disk_write_mbps={shape["disk_write_mbps"]} is an unmeasured assumption')
            if "disk_iops" not in shape:
                notes.append("IOPS is unconstrained until a measured disk_iops limit is supplied")
            if move["method"] != "ds-ring":
                notes.append(move["note"])
            if not config["cache_budget_ok"]:
                notes.append("Cache budget cannot fit one second of ingest")
            steady_write, steady_network = required["write_mbps"] / (nodes - 1), required["nic_mbps"] / (nodes - 1)
            spare = max(0, min(capacity["write_mbps"] - steady_write, capacity["nic_mbps"] - steady_network))
            copy_bytes = peaks["store_bytes"] * move["copy_fraction"]
            replacement = not all(h.get("shape") == shape["name"] for h in hosts)
            if move.get("moved_bytes") is not None and not replacement:
                plan_bytes = positive(move["moved_bytes"], "plan.moved_bytes", zero=True)
                copy_bytes = max(copy_bytes, plan_bytes) if move.get("moved_bytes_estimated") else plan_bytes
            transfer = {"bytes_estimate": copy_bytes, "per_target_spare_mbps": spare,
                        "io_lower_bound_seconds": copy_bytes / (spare * nodes * 1e6) if spare > 0 else None,
                        "note": "Optimistic balanced I/O bound, excludes source/CPU/verification/compaction limits",
                        "curve": [{"per_target_rate_limit_mbps": limit, "per_target_mbps": min(limit, spare),
                                   "aggregate_mbps": min(limit, spare) * nodes,
                                   "projected_write_fraction": (steady_write + min(limit, spare)) / hard["write_mbps"],
                                   "projected_nic_fraction": (steady_network + min(limit, spare)) / hard["nic_mbps"],
                                   "io_lower_bound_seconds": copy_bytes / (min(limit, spare) * nodes * 1e6) if spare > 0 else None}
                                  for limit in (50, 100, 250, 500)]}
            candidates.append({"shape": shape["name"], "nodes": nodes, "surviving_nodes": nodes - 1,
                               "total_nodes_if_excluded_retained": nodes + len(omitted),
                               "nodes_by_constraint": counts, "binding": binding, "drivers": drivers,
                               "ram_model": memory,
                               "suspicious": nodes > len(hosts), "current_nodes": len(all_hosts),
                               "after_loss_fraction_of_target": {key: candidate_required[key] / ((nodes - 1) * value) for key, value in capacity.items()},
                               "burst_checks": checks, "provisioned_cores": shape["cores"] * nodes,
                               "provisioned_ram_gib": shape["ram_gib"] * nodes, "provisioned_disk_tb": shape["disk_tb"] * nodes,
                               "monthly_cost": shape["monthly_cost"] * nodes if "monthly_cost" in shape else None,
                               "movement": move, "transfer": transfer, "config": config,
                               "status": "conditional" if config["cache_budget_ok"] else "blocked",
                               "execution_status": "blocked" if execution_blockers else "unverified",
                               "blockers": list(execution_blockers), "unverified": list(unverified), "warnings": notes})
        costed = all(c["monthly_cost"] is not None for c in candidates)
        complete = not any("CPU is zero" in text for text in unverified)
        # A downsize report must not prefer hundreds of tiny VMs merely because
        # their total core count is slightly lower. Keep increases diagnostic.
        eligible = [c for c in candidates if c["status"] != "blocked" and not c["suspicious"] and complete]
        preferred = min(eligible, key=lambda c: (c["monthly_cost"] if costed else c["provisioned_cores"], c["nodes"], c["provisioned_ram_gib"])) if eligible else None
        group.update(status="conditional" if preferred else "review", forecast_whisper=forecasting,
                     capacity_complete=complete,
                     demand_peak_sum=dict(peaks), demand_sustained_sum=dict(demand), required_after_headroom=required,
                     resource_percentiles={key: percentile for key in required},
                     growth={"observation_days": window_days, "applied_to_sizing": growth_applied, "positive_bytes_per_day": growth},
                     recommended_shape=preferred["shape"] if preferred else None,
                     recommendation_basis=("lowest monthly cost" if costed else "fewest provisioned cores (no prices supplied)") +
                                          " among candidates no larger than the current included count; increases require review",
                     candidates=candidates)
    return report


def print_report(report):
    percentile = report["policy"]["sustained_percentile"]
    print(f"Capacity plan: {percentile} sustained demand, N+1 per site; peaks are headroom checks. " +
          ", ".join(f'{key}={report["policy"][key]:.0%}' for key in ("cpu_target", "memory_target", "write_target", "iops_target", "disk_target")))
    if report.get("exclusions"):
        print("Excluded from sustained statistics: " + ", ".join(f'{x["start_utc"]} — {x["end_utc"]}' for x in report["exclusions"]))
    print("Graphite sustained source: " + report["graphite_sustained_consolidation"])
    print(f'{"cluster/site":24} {"shape":25} {"current":>7} {"sized":>7} {"binding":22} {"move":>7} status')
    for cluster in report["clusters"]:
        key = f'{cluster["cluster"]}/{cluster["site"]}'
        if not cluster["candidates"]:
            print(f'{key:24} {cluster["status"].upper()}: {cluster["reason"]}')
        if cluster.get("excluded_nodes"):
            print(f'{key}: {cluster["included_nodes"]} included, {cluster["excluded_nodes"]} excluded and retained; sizing covers included workload')
        if cluster.get("capacity_complete") is False:
            print(f"{key}: CPU UNVERIFIED (zero during ingest); capacity counts are resource floors")
        if cluster["candidates"]:
            reference = next((c for c in cluster["candidates"] if c["shape"] == "bare-metal-96"), cluster["candidates"][0])
            ram = reference["ram_model"]
            component = f'{ram["component_per_survivor_bytes"]/GIB:.1f} GiB' if ram["components_available"] else "unavailable (missing components; legacy fallback)"
            print(f'{key}: RAM per surviving {reference["shape"]}: old anon-based {ram["legacy_per_survivor_bytes"]/GIB:.1f} GiB; component {component}')
        for candidate in cluster["candidates"]:
            mark = " *" if candidate["shape"] == cluster["recommended_shape"] else ""
            move = candidate["movement"]
            movement_text = f'{move["fraction"]:.0%}' + ("~" if move["method"] != "ds-ring" else "")
            binding = candidate["binding"][0]
            if len(candidate["binding"]) > 1:
                binding += f' (+{len(candidate["binding"]) - 1})'
            count = str(candidate["nodes"]) + ("!" if candidate["suspicious"] else "")
            state = candidate["status"] + (" exec-blocked" if candidate["execution_status"] == "blocked" else "")
            print(f'{key:24} {candidate["shape"]:25} {cluster["current_nodes"]:7d} {count:>7} '
                  f'{binding:22} {movement_text:>7} {state}{mark}')
            if candidate["suspicious"]:
                if cluster["included_nodes"] == 1:
                    print("  N+1 requires at least two included nodes; current included count is one")
                for driver in candidate["drivers"]:
                    print(f'  SUSPICIOUS increase: {driver["resource"]} {percentile} '
                          f'total={driver["sustained_total"]}, adjusted={driver["adjusted_total"]:.4g}, '
                          f'per-survivor budget={driver["per_survivor_budget"]:.4g}; '
                          'host datapoints=' + ", ".join(f'{d["host"].split(".")[0]}:{d["value"]:.4g}' for d in driver["datapoints"]))
    for host in report["excluded_hosts"]:
        print(f'EXCLUDED {host["host"]}: {host["reason"]}; retain')
    print("* preferred under the stated assumptions; ~ estimated movement. Full evidence/config in JSON.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot")
    parser.add_argument("--shapes", default=str(Path(__file__).with_name("shapes.json")))
    parser.add_argument("--policy", help="JSON object overriding Policy defaults")
    parser.add_argument("--inventory", help="authoritative facts/expected members; absent metrics are reported as exclusions")
    parser.add_argument("--exclude", action="append", default=[], metavar="START-END", help="omit known one-off windows from sustained statistics; requires query cache")
    parser.add_argument("--state-dir", help="raw query cache for offline exclusion replay (default: snapshot query_cache)")
    parser.add_argument("--from", dest="start", type=timestamp, help="start of the sustained analysis range; raw cache required")
    parser.add_argument("--until", dest="end", type=timestamp, help="exclusive end of the sustained analysis range; raw cache required")
    parser.add_argument("--ring-plans", help="JSON list of normalized ds-ring plans; see README")
    parser.add_argument("--ds-ring", nargs=3, action="append", metavar=("SPEC", "SUMMARY", "SHAPE"),
                        help="read native ds-ring spec + summary for a shape; repeat per candidate")
    parser.add_argument("--estimate-whisper", action="store_true")
    parser.add_argument("--output", default="sizing.json")
    args = parser.parse_args(argv)
    try:
        snapshot = read_json(args.snapshot)
        inventory = read_json(args.inventory) if args.inventory else None
        exclusions = parse_exclusions(args.exclude)
        if args.start is not None or args.end is not None:
            low = max(snapshot["window"]["start"], args.start if args.start is not None else snapshot["window"]["start"])
            high = min(snapshot["window"]["end"], args.end if args.end is not None else snapshot["window"]["end"])
            if high <= low:
                raise ValueError("analysis range does not overlap the snapshot")
            exclusions.extend([(snapshot["window"]["start"], low), (high, snapshot["window"]["end"] + 1)])
        if exclusions:
            from collect import replay_snapshot
            state_dir = args.state_dir or snapshot.get("query_cache")
            if not state_dir:
                raise ValueError("--exclude needs the raw query cache; provide --state-dir for an older snapshot")
            facts = {h["host"]: h for h in inventory["hosts"]} if inventory else {}
            replay_inventory = {"hosts": [{**host, **facts.get(host["host"], {})} for host in snapshot["hosts"]]}
            snapshot = replay_snapshot(snapshot, state_dir, exclusions, replay_inventory)
        plans = read_json(args.ring_plans) if args.ring_plans else []
        plans.extend(read_ring_plan(*record) for record in args.ds_ring or [])
        report = calculate(snapshot, read_json(args.shapes),
                           Policy(**read_json(args.policy)) if args.policy else Policy(),
                           plans, args.estimate_whisper, inventory)
        write_json(args.output, report)
        print_report(report)
        return 2 if any(c["status"] == "blocked" for c in report["clusters"]) else 0
    except (ValueError, OSError, KeyError, TypeError) as error:
        print(f"sizing: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
