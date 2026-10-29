"""Fast offline tests, including the real collector/calculator command lines."""
import copy
import json
import math
import subprocess
import sys
import tempfile
import unittest
import urllib.parse
from pathlib import Path

import collect
import size
from common import FIELDS, GUARDS, combine, read_json, summarize, write_json

HERE = Path(__file__).parent
FIXTURE = HERE / "fixtures/snapshot.json"


class FakeMetrics:
    """A tiny recorded-shape adapter: no HTTP, authentication or production access."""
    calls = 0
    times = [0, 300, 600]
    hosts = [f"store-metrics-{n}" for n in (1201, 1202)]
    missing_slab = False

    def read_token(self):
        return "offline-test"

    def range_query(self, uid, expression, token, **kwargs):
        self.calls += 1
        assert uid == collect.MIMIR and kwargs["retries"] == 0
        value = 0
        if "metrics_received_tcp_total" in expression:
            value = 1e6
        elif "http_requests_total" in expression:
            value = 50
        elif "histogram_quantile" in expression:
            value = .2
        elif "disk_bytes" in expression:
            value = 500e9
        else:
            value = next((v for key, v in (("go_gc_heap_live_bytes", 3e9), ("pebble_block_cache_bytes", 1e9),
                ("store_metadata_cache_arena_bytes", 1e9), ("pebble_memtable_bytes", .5e9),
                ("process_resident_memory_bytes", 12e9), ("go_memory_classes_heap_objects_bytes", 4e9),
                ("go_memory_classes_heap_free_bytes", 1e9), ("go_memory_classes_heap_unused_bytes", 1e9),
                ("go_memory_classes_heap_stacks_bytes", 1e9), ("process_start_time_seconds", 1),
                ("pebble_block_cache_hits_total", .95), ("store_metadata_cache_hits_total", .8))
                if key in expression), 0)
        return {"status": "success", "data": {"result": [
            {"metric": {"host_name": host + ".region-a.example.net"},
             "values": [[t, str(value + (t * 1e5 if value == 500e9 else 0))] for t in self.times]}
            for host in self.hosts]}}

    def get_json(self, path, token, **kwargs):
        self.calls += 1
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
        expression = query["target"][0]
        assert kwargs["retries"] == 0
        value = next((v for key, v in (("cpu.total.idle", 17280), ("cpu.total.iowait", 384), ("cpu.cpu_count", 192),
                     ("AnonPages", 20e9), ("Active_file", 10e9), ("Slab", 5e9),
                     ("write_byte_per_second", 40e6), ("util_percentage", 20),
                     ("reads_per_second", 1000), ("writes_per_second", 2000),
                     ("read_await", 2), ("write_await", 3),
                     ("metrics_known", 20e6)) if key in expression), None)
        if value is None:
            raise AssertionError(expression)
        device = ".nvme1n1" if ".iostat." in expression else ""
        return [{"target": host + device, "datapoints": [
            [None if self.missing_slab and "Slab" in expression and t == 300 else value, t]
            for t in self.times]} for host in self.hosts]


def inventory():
    result = read_json(HERE / "fixtures/inventory.json")
    result["hosts"] = result["hosts"][:2]
    return result


def snapshot():
    return read_json(FIXTURE)


def converted_snapshot():
    value = snapshot()
    value["hosts"] = [h for h in value["hosts"] if h["backend"] == "pebble-chunk"]
    return value


def candidate(report, cluster="metrics", site="region-a", shape="bare-metal-96"):
    group = next(g for g in report["clusters"] if (g["cluster"], g["site"]) == (cluster, site))
    return next(c for c in group["candidates"] if c["shape"] == shape)


class StatisticsTests(unittest.TestCase):
    def test_percentile_growth_and_coverage(self):
        points = {n * 86400: n * 10 + 100 for n in range(20)}
        result = summarize(points, 0, 20 * 86400, 86400)
        self.assertEqual(result["p95"], 280)
        self.assertEqual(result["peak"], 290)
        self.assertEqual(result["growth_per_day"], 10)
        self.assertAlmostEqual(result["coverage"], 20 / 21)
        self.assertEqual(result["last_at"], 19 * 86400)

    def test_bad_values_and_missing_parts_do_not_become_zero(self):
        result = summarize({0: 4, 300: None, 600: float("nan"), 900: -1}, 0, 900, 300)
        self.assertEqual(result["samples"], 1)
        self.assertEqual(result["coverage"], .25)
        self.assertEqual(combine([{0: 2, 300: 3}, {0: 7}]), {0: 9})
        self.assertEqual(combine([{0: 2}, {}]), {})
        self.assertIsNone(summarize({}, 0, 300, 300)["peak"])


class CollectorTests(unittest.TestCase):
    def test_inventory_validation_and_scoped_queries(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inventory.json"
            value = inventory()
            write_json(path, value)
            loaded = collect.load_inventory(path)
            jobs = collect.query_plan(loaded)
            self.assertEqual(len(jobs), 48)
            self.assertTrue(all(j["site"] == "region-a" for j in jobs))
            self.assertTrue(all("1201" in j["expression"] and "1202" in j["expression"] for j in jobs))
            value["hosts"].append(value["hosts"][0])
            write_json(path, value)
            with self.assertRaisesRegex(ValueError, "duplicate"):
                collect.load_inventory(path)

    def test_device_and_whisper_inventory_are_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inventory.json"
            value = inventory()
            value["hosts"][0]["store_devices"] = []
            write_json(path, value)
            with self.assertRaisesRegex(ValueError, "store_devices"):
                collect.load_inventory(path)
            value = inventory()
            value["hosts"][0]["backend"] = "whisper"
            write_json(path, value)
            with self.assertRaisesRegex(ValueError, "store_bytes_target"):
                collect.load_inventory(path)

    def test_partial_responses_and_ambiguity_are_rejected(self):
        job = {"source": "mimir"}
        for response in ({"status": "error"}, {"status": "success", "warnings": ["partial"]}):
            with self.assertRaises(ValueError):
                collect.normalize(job, response)
        job = {"source": "graphite"}
        with self.assertRaisesRegex(ValueError, "duplicate"):
            collect.normalize(job, [{"target": "host", "datapoints": []}] * 2)
        self.assertEqual(collect.normalize(job, [{"target": "host", "datapoints":
                                                [[None, 0], ["NaN", 1], [1, 2]]}]), {"host": {2: 1}})
        job["field"] = "metric_count"
        self.assertEqual(collect.normalize(job, [{"target": "store-metrics-1201_region-a_example_net",
                                                 "datapoints": [[100, 0]]}]),
                         {"store-metrics-1201": {0: 100}})

    def test_collection_resume_and_derived_fields(self):
        value = inventory()
        jobs = collect.query_plan(value)
        window = {"start": 0, "end": 600, "step_seconds": 300}
        with tempfile.TemporaryDirectory() as directory:
            fake = FakeMetrics()
            result = list(collect.collect(value, jobs, window, directory, fake, "fake", rps=100000))
            hosts, cluster, queries = result[0]
            self.assertEqual(fake.calls, len(jobs))
            metrics = hosts[0]["metrics"]
            self.assertAlmostEqual(metrics["cpu_cores"]["peak"], 7.68)
            self.assertEqual(metrics["working_set_bytes"]["peak"], 35e9)
            self.assertEqual(metrics["write_mbps"]["peak"], 40)
            self.assertEqual(metrics["iops"]["peak"], 3000)
            self.assertEqual(cluster["metrics"]["read_p99_seconds"]["peak"], .2)
            self.assertEqual(cluster["metrics"]["input_points_per_second"]["peak"], 2e6)
            self.assertEqual(len(queries), len(jobs))
            again = list(collect.collect(value, jobs, window, directory, fake, "fake", rps=100000))
            self.assertEqual(fake.calls, len(jobs))
            self.assertEqual(again, result)

    def test_missing_memory_component_and_device_stays_missing(self):
        value = inventory()
        value["hosts"][0]["store_devices"].append("nvme2n1")
        fake = FakeMetrics()
        fake.missing_slab = True
        with tempfile.TemporaryDirectory() as directory:
            result = list(collect.collect(value, collect.query_plan(value),
                          {"start": 0, "end": 600, "step_seconds": 300}, directory,
                          fake, "fake", rps=100000))
        hosts = result[0][0]
        self.assertEqual(hosts[0]["metrics"]["write_mbps"]["coverage"], 0)
        self.assertAlmostEqual(hosts[1]["metrics"]["working_set_bytes"]["coverage"], 2 / 3)

    def test_failure_resumes_only_missing_queries(self):
        value = inventory()
        class Failure(FakeMetrics):
            fail = True
            def range_query(self, uid, expression, token, **kwargs):
                response = super().range_query(uid, expression, token, **kwargs)
                if self.fail and "histogram_quantile" in expression:
                    raise OSError("simulated interruption")
                return response
        fake = Failure()
        args = (value, collect.query_plan(value), {"start": 0, "end": 600, "step_seconds": 300})
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(OSError, "simulated interruption"):
                list(collect.collect(*args, directory, fake, "fake", rps=100000))
            cached = len(list(Path(directory).glob("*.json.gz")))
            self.assertGreater(cached, 0)
            self.assertLessEqual(cached, len(args[1]) - 1)
            fake.fail = False
            list(collect.collect(*args, directory, fake, "fake", rps=100000))
            self.assertEqual(fake.calls, len(args[1]) + 1)

    def test_overrides_and_cpu_consolidation(self):
        value = inventory()
        value["query_overrides"] = {"metrics/region-a/device_read_iops": "custom_read_iops"}
        jobs = collect.query_plan(value)
        job = next(j for j in jobs if j["field"] == "device_read_iops")
        self.assertEqual(job["expression"], "custom_read_iops")
        class HTTP:
            def get_json(self, path, token, **kwargs):
                self.path = path
                return []
        http = HTTP()
        collect.fetch(http, "", jobs[0], {"start": 0, "end": 600, "step_seconds": 300}, 10)
        self.assertIn(',"average")', urllib.parse.unquote(http.path))
        burst = next(j for j in jobs if j["field"] == "cpu_idle_percent" and j.get("role") == "burst")
        collect.fetch(http, "", burst, {"start": 0, "end": 600, "step_seconds": 300}, 10)
        self.assertIn(',"min")', urllib.parse.unquote(http.path))

    def test_system_leaf_names_and_cpu_count(self):
        jobs = {j["field"]: j for j in collect.query_plan(inventory())}
        self.assertIn("reads_per_second", jobs["device_read_iops"]["expression"])
        self.assertIn("writes_per_second", jobs["device_write_iops"]["expression"])
        self.assertIn(".cpu.cpu_count", jobs["cpu_count"]["expression"])
        self.assertIn("read_await", jobs["device_read_await_ms"]["expression"])

    def test_missing_cpu_count_is_missing_instead_of_zero(self):
        value = inventory()
        jobs = [j for j in collect.query_plan(value) if j["field"] != "cpu_count"]
        with tempfile.TemporaryDirectory() as directory:
            result = list(collect.collect(value, jobs, {"start": 0, "end": 600, "step_seconds": 300},
                                         directory, FakeMetrics(), "fake", rps=100000))
            self.assertIsNone(result[0][0][0]["metrics"]["cpu_cores"]["peak"])
            for h in value["hosts"]:
                h["logical_cpus"] = 192
            replayed = list(collect.collect(value, jobs, {"start": 0, "end": 600, "step_seconds": 300},
                                           directory, None, None, offline=True))
            self.assertAlmostEqual(replayed[0][0][0]["metrics"]["cpu_cores"]["p95"], 7.68)

    def test_old_build_does_not_poison_cluster_summary(self):
        class OldBuild(FakeMetrics):
            def range_query(self, *args, **kwargs):
                response = super().range_query(*args, **kwargs)
                response["data"]["result"] = response["data"]["result"][:1]
                return response
        value = inventory()
        with tempfile.TemporaryDirectory() as directory:
            result = list(collect.collect(value, collect.query_plan(value),
                          {"start": 0, "end": 600, "step_seconds": 300}, directory, OldBuild(), "fake", rps=100000))
        hosts, group, _ = result[0]
        self.assertTrue(hosts[1]["excluded"])
        self.assertEqual(group["excluded_host_count"], 1)
        self.assertEqual(group["metrics"]["input_points_per_second"]["p95"], 1e6)


class CalculatorTests(unittest.TestCase):
    def setUp(self):
        self.shapes = read_json(HERE / "shapes.json")

    def test_fixture_and_minimum_n_plus_one_for_every_resource(self):
        result = size.calculate(converted_snapshot(), self.shapes)
        self.assertEqual(candidate(result)["nodes"], 3)
        for group in result["clusters"]:
            for item in group["candidates"]:
                self.assertEqual(item["nodes"], max(item["nodes_by_constraint"].values()))
                self.assertGreaterEqual(item["nodes"], 2)
                ratios = item["after_loss_fraction_of_target"]
                self.assertTrue(all(value <= 1 + 1e-12 for value in ratios.values()))
                # Removing one more node violates at least one modeled capacity.
                if item["nodes"] > 2:
                    self.assertTrue(any(v * (item["nodes"] - 1) / (item["nodes"] - 2) > 1
                                        for v in ratios.values()))
                self.assertTrue(item["config"]["cache_budget_ok"])

    def test_site_isolation_and_no_conversion_discount_on_pebble(self):
        value = converted_snapshot()
        before = size.calculate(value, self.shapes)
        for host in value["hosts"]:
            if host["site"] == "region-a":
                # Large enough to bind even at the 70% CPU target.
                host["metrics"]["cpu_cores"]["peak"] *= 8
                host["metrics"]["cpu_cores"]["p95"] *= 8
        after = size.calculate(value, self.shapes)
        self.assertGreater(candidate(after)["nodes"], candidate(before)["nodes"])
        self.assertEqual(candidate(after, site="region-b"), candidate(before, site="region-b"))
        altered = size.calculate(converted_snapshot(), self.shapes,
                                 size.Policy(conversion_cpu_factor=.5, conversion_memory_factor=.5))
        self.assertEqual(candidate(altered), candidate(before))

    def test_whisper_requires_explicit_forecast(self):
        value = snapshot()
        blocked = size.calculate(value, self.shapes)
        fra = next(g for g in blocked["clusters"] if g["site"] == "region-c")
        self.assertEqual(fra["status"], "blocked")
        result = size.calculate(value, self.shapes, estimate_whisper=True)
        fra = next(g for g in result["clusters"] if g["site"] == "region-c")
        original = sum(h["metrics"]["cpu_cores"]["peak"] for h in value["hosts"] if h["site"] == "region-c")
        self.assertEqual(fra["demand_peak_sum"]["cpu_cores"], original * .75)
        self.assertTrue(fra["forecast_whisper"])

    def test_missing_and_partial_telemetry_fail_closed(self):
        for change in (lambda h: h["metrics"].pop("cpu_cores"),
                       lambda h: h["metrics"]["iops"].update(coverage=.5),
                       lambda h: h["metrics"]["store_bytes"].pop("growth_per_day"),
                       lambda h: h["metrics"]["cpu_cores"].update(peak=float("nan")),
                       lambda h: h["metrics"]["cpu_cores"].update(last_at=0)):
            value = converted_snapshot()
            change(value["hosts"][0])
            with self.assertRaises(ValueError):
                size.calculate(value, self.shapes)

    def test_health_guards_block_recommendation(self):
        for guard in GUARDS + ("read_p99_seconds",):
            value = converted_snapshot()
            value["hosts"][0]["metrics"][guard]["peak"] = 2
            value["hosts"][0]["metrics"][guard]["last"] = 2
            result = size.calculate(value, self.shapes)
            self.assertEqual(candidate(result)["execution_status"], "blocked")
            group = next(g for g in result["clusters"] if g["site"] == "region-a" and g["cluster"] == "metrics")
            self.assertNotEqual(group["status"], "blocked")
        value = converted_snapshot()
        value["hosts"][0]["metrics"].pop(GUARDS[0])
        self.assertTrue(candidate(size.calculate(value, self.shapes))["unverified"])

    def test_growth_never_credits_shrinkage_and_prices_choose_shape(self):
        value = converted_snapshot()
        for host in value["hosts"]:
            host["metrics"]["store_bytes"]["growth_per_day"] = -1e12
        negative = size.calculate(value, self.shapes)
        zero = size.calculate(value, self.shapes, size.Policy(growth_days=0))
        self.assertEqual(candidate(negative)["nodes"], candidate(zero)["nodes"])
        for shape in self.shapes:
            shape["monthly_cost"] = 100000
        self.shapes[1]["monthly_cost"] = 1000
        result = size.calculate(value, self.shapes)
        self.assertTrue(all(g["recommended_shape"] == "bare-metal-96" for g in result["clusters"]))

    def test_disk_write_nic_and_optional_ingest_constraints(self):
        value = converted_snapshot()
        reference = candidate(size.calculate(value, self.shapes), shape="example-base")["nodes"]
        for field, limit, resource in (("disk_tb", .1, "disk_bytes"),
                                       ("disk_write_mbps", 10, "device_busy_mbps"),
                                       ("nic_gbps", .1, "nic_mbps"),
                                       ("disk_iops", 100, "iops"),
                                       ("max_input_points_s", 1000, "input_points_s"),
                                       ("max_read_qps", 1, "read_qps"),
                                       ("max_metrics", 1e5, "metrics")):
            shapes = copy.deepcopy(self.shapes)
            shapes[0][field] = limit
            item = candidate(size.calculate(value, shapes), shape="example-base")
            self.assertGreater(item["nodes"], reference)
            self.assertIn(resource, item["binding"])
            self.assertTrue(next(d for d in item["drivers"] if d["resource"] == resource)["datapoints"])

    def test_ring_plan_evidence_and_vm_copy_are_distinct(self):
        value = converted_snapshot()
        hosts = [h["host"] for h in value["hosts"] if h["cluster"] == "metrics" and h["site"] == "region-a"]
        plan = {"cluster": "metrics", "site": "region-a", "shape": "bare-metal-96", "nodes": 3,
                "old_hosts": hosts, "new_hosts": hosts[:3], "total_metrics": 600e6,
                "moved_metrics": 315e6, "moved_bytes": 2e12}
        item = candidate(size.calculate(value, self.shapes, plans=[plan]))
        self.assertEqual(item["movement"]["fraction"], .525)
        self.assertEqual(item["movement"]["method"], "ds-ring")
        self.assertEqual(item["transfer"]["bytes_estimate"], 2e12)
        plan["old_hosts"] = hosts[:-1]
        with self.assertRaisesRegex(ValueError, "membership"):
            size.calculate(value, self.shapes, plans=[plan])
        item = candidate(size.calculate(value, self.shapes), shape="example-base")
        self.assertEqual(item["movement"]["copy_fraction"], 1)
        self.assertEqual(item["transfer"]["bytes_estimate"], 3.9e12)

    def test_copy_curve_stays_within_io_targets(self):
        result = size.calculate(converted_snapshot(), self.shapes)
        for group in result["clusters"]:
            for item in group["candidates"]:
                previous = 0
                for point in item["transfer"]["curve"]:
                    self.assertGreaterEqual(point["aggregate_mbps"], previous)
                    previous = point["aggregate_mbps"]
                    self.assertLessEqual(point["projected_write_fraction"], size.Policy().write_target + 1e-12)
                    self.assertLessEqual(point["projected_nic_fraction"], size.Policy().nic_target + 1e-12)

    def test_native_ds_ring_adapter_and_invalid_summary(self):
        value = converted_snapshot()
        hosts = [h["host"] for h in value["hosts"] if h["cluster"] == "metrics" and h["site"] == "region-a"]
        nodes = [{"id": str(i), "node": f"{host}:2003={i:03d}"} for i, host in enumerate(hosts)]
        spec = {"version": 1, "cluster": "metrics", "site": "region-a", "old": {"nodes": nodes},
                "new": {"nodes": nodes[:3]}}
        summary = {"metrics": 263, "moved_metrics": 119, "moved_fraction": 119 / 263,
                   "unowned": 0, "estimated_bytes": 71400}
        with tempfile.TemporaryDirectory() as directory:
            spec_path, summary_path = Path(directory) / "spec.json", Path(directory) / "summary.json"
            write_json(spec_path, spec)
            write_json(summary_path, summary)
            plan = size.read_ring_plan(spec_path, summary_path, "bare-metal-96")
            item = candidate(size.calculate(value, self.shapes, plans=[plan]))
            self.assertAlmostEqual(item["movement"]["fraction"], 119 / 263)
            self.assertEqual(item["movement"]["method"], "ds-ring-sample")
            self.assertGreater(item["transfer"]["bytes_estimate"], 1e12)
            self.assertEqual(item["movement"]["native_spec_sha256"], plan["native_spec_sha256"])
            summary["unowned"] = 1
            write_json(summary_path, summary)
            with self.assertRaises(ValueError):
                size.read_ring_plan(spec_path, summary_path, "bare-metal-96")

    def test_invalid_policy_and_shape(self):
        for policy in (size.Policy(cpu_target=.9), size.Policy(imbalance=.5), size.Policy(growth_days=-1)):
            with self.assertRaises(ValueError):
                size.calculate(converted_snapshot(), self.shapes, policy)
        self.shapes[0]["nic_gbps"] = 0
        with self.assertRaises(ValueError):
            size.calculate(converted_snapshot(), self.shapes)

    def test_example_shapes_and_explicit_write_assumptions(self):
        shapes = {s["name"]: s for s in self.shapes}
        self.assertEqual(shapes["bare-metal-96"]["ram_gib"], 1536)
        self.assertEqual(shapes["bare-metal-96"]["nic_gbps"], 25)
        for name, cores, ram, disk, iops in (
                ("example-vm-2", 16, 256, 5, 30000), ("example-vm-3", 32, 512, 10, 120000),
                ("example-vm-4", 32, 512, 4, 50000), ("example-vm-5", 8, 128, 4, 30000),
                ("example-vm-6", 16, 256, 5, 120000), ("example-vm-7", 24, 192, 3, 120000),
                ("example-vm-8", 4, 64, 1, None)):
            shape = shapes[name]
            self.assertEqual((shape["cores"], shape["ram_gib"], shape["disk_tb"], shape.get("disk_iops")),
                             (cores, ram, disk, iops))
            self.assertTrue(shape["disk_write_mbps_assumed"])

    def test_peaks_are_headroom_checks_not_sustained_drivers(self):
        value = converted_snapshot()
        before = size.calculate(value, self.shapes)
        for host in value["hosts"]:
            for field in ("cpu_cores", "working_set_bytes", "write_mbps", "store_bytes", "iops"):
                host["metrics"][field]["peak"] *= 100
        after = size.calculate(value, self.shapes)
        self.assertEqual(candidate(before)["nodes"], candidate(after)["nodes"])
        self.assertTrue(any(c["exceeds_ceiling"] for c in candidate(after)["burst_checks"] if "exceeds_ceiling" in c))
        self.assertTrue(all(d["percentile"] == "p95" for d in candidate(after)["drivers"]))

    def test_short_window_growth_does_not_turn_catchup_into_90_day_capacity(self):
        value = converted_snapshot()
        value["window"]["end"] = value["window"]["start"] + 86400
        for host in value["hosts"]:
            for stats in host["metrics"].values():
                stats["last_at"] = value["window"]["end"]
            host["metrics"]["store_bytes"]["growth_per_day"] = 1e14
        result = size.calculate(value, self.shapes)
        no_growth = size.calculate(value, self.shapes, size.Policy(growth_days=0))
        self.assertEqual(candidate(result)["nodes"], candidate(no_growth)["nodes"])
        verified = size.calculate(value, self.shapes, size.Policy(growth_min_days=0))
        self.assertGreater(candidate(verified)["nodes"], candidate(result)["nodes"])

    def test_no_go_carbon_hosts_are_excluded_and_retained(self):
        value = converted_snapshot()
        host = value["hosts"][0]
        for field in ("input_points_per_second", "read_requests_per_second", "read_p99_seconds", "store_bytes"):
            host["metrics"][field] = {"peak": None, "p95": None, "coverage": 0}
        result = size.calculate(value, self.shapes)
        self.assertEqual(len(result["excluded_hosts"]), 1)
        group = next(g for g in result["clusters"] if g["cluster"] == "metrics" and g["site"] == "region-a")
        self.assertEqual((group["current_nodes"], group["included_nodes"], group["excluded_nodes"]), (6, 5, 1))
        self.assertTrue(group["candidates"])
        self.assertTrue(all(c["total_nodes_if_excluded_retained"] == c["nodes"] + 1 for c in group["candidates"]))

    def test_inventory_reports_absent_old_builds_without_sizing_other_sites(self):
        value = converted_snapshot()
        facts = {"hosts": [{"host": "store-infra-1305.region-a.example.net", "cluster": "infra", "site": "region-a"},
                            {"host": "store-infra-6105.region-c.example.net", "cluster": "infra", "site": "region-c"}]}
        result = size.calculate(value, self.shapes, inventory=facts)
        self.assertEqual([h["host"] for h in result["excluded_hosts"]], [facts["hosts"][0]["host"]])
        self.assertFalse(any(g["site"] == "region-c" for g in result["clusters"]))

    def test_historical_guard_bursts_do_not_block_capacity(self):
        value = converted_snapshot()
        value["hosts"][0]["metrics"]["stall_seconds_per_second"]["peak"] = .25
        result = size.calculate(value, self.shapes)
        self.assertEqual(candidate(result)["status"], "conditional")
        self.assertFalse(candidate(result)["blockers"])
        self.assertTrue(any("historical" in text for text in candidate(result)["warnings"]))

    def test_suspicious_increases_explain_percentile_and_datapoints(self):
        value = converted_snapshot()
        value["hosts"] = value["hosts"][:1]
        result = size.calculate(value, self.shapes)
        item = candidate(result)
        self.assertEqual(item["current_nodes"], 1)
        self.assertTrue(item["suspicious"])
        self.assertTrue(item["drivers"][0]["datapoints"])
        self.assertIsNone(result["clusters"][0]["recommended_shape"])
        self.assertEqual(result["clusters"][0]["status"], "review")

    def test_old_zero_cpu_snapshot_is_explicitly_incomplete(self):
        value = converted_snapshot()
        value["hosts"][0]["metrics"]["cpu_cores"].update(peak=0, p95=0, last=0)
        result = size.calculate(value, self.shapes)
        group = next(g for g in result["clusters"] if g["cluster"] == "metrics" and g["site"] == "region-a")
        self.assertFalse(group["capacity_complete"])
        self.assertIsNone(group["recommended_shape"])
        self.assertTrue(any("CPU is zero" in t for t in candidate(result)["unverified"]))

    def test_config_uses_existing_settings_and_bounds_memory(self):
        import re
        example = {}
        section = None
        for line in (HERE.parents[2] / "go-carbon.conf.example").read_text().splitlines():
            match = re.match(r"\[([\w-]+)\]", line)
            if match:
                section = match[1]
                example.setdefault(section, set())
            key = re.match(r"\s*#?\s*([\w-]+)\s*=", line)
            if section and key:
                example[section].add(key[1])
        for shape in self.shapes:
            config = size.go_config(shape, 2e6)
            for section, values in config["toml"].items():
                for key in values:
                    self.assertIn(key, example[section], (section, key))
            budgets = config["memory_budget"]
            self.assertLessEqual(config["toml"]["cache"]["max-size"] * 176,
                                 shape["ram_gib"] * size.GIB * .12)
            self.assertLess(sum(budgets[k] for k in ("go_heap_limit_gib", "store_cache_gib", "metadata_cache_gib")),
                            shape["ram_gib"] * .75)


class CommandTests(unittest.TestCase):
    def run_cli(self, script, *args, env=None):
        return subprocess.run([sys.executable, str(getattr(self, "collector", HERE / script) if script == "collect.py" else HERE / script), *map(str, args)],
                              capture_output=True, text=True, env=env, timeout=15)

    def test_dry_run_needs_no_credentials_or_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state"
            result = self.run_cli("collect.py", "--inventory", HERE / "fixtures/inventory.json", "--state-dir", state)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(state.exists())
            self.assertEqual(json.loads(result.stderr)["mode"], "dry-run")

    def test_complete_cli_pipeline_resume_and_window_guard(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.collector = root / "collector.py"
            self.collector.write_text(
                f"import sys\nsys.path.insert(0, {str(HERE)!r})\n"
                "import collect\nfrom test_sizing import FakeMetrics\n"
                "collect.MetricsHTTP=lambda *args: FakeMetrics()\n"
                "collect.RateLimit.wait=lambda self: None\n"
                "sys.exit(collect.main())\n")
            inv = root / "inventory.json"
            write_json(inv, inventory())
            snapshot_path, state, output = root / "snapshot.json", root / "state", root / "sizing.json"
            args = ["--inventory", inv, "--state-dir", state, "--output", snapshot_path,
                    "--step", "300", "--days", str(600 / 86400),
                    "--requests-per-second", "10", "--end", "600"]
            result = self.run_cli("collect.py", *args, "--collect")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(len(read_json(snapshot_path)["hosts"]), 2)
            result = self.run_cli("size.py", snapshot_path, "--output", output)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(read_json(output)["mode"], "capacity-only")
            replay = root / "replay.json"
            result = self.run_cli("collect.py", "--inventory", inv, "--state-dir", state,
                                  "--replay-snapshot", snapshot_path, "--output", replay)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(read_json(replay)["hosts"], read_json(snapshot_path)["hosts"])
            result = self.run_cli("size.py", snapshot_path, "--exclude", "0-300", "--output", output)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(read_json(output)["exclusions"][0]["start"], 0)
            result = self.run_cli("size.py", snapshot_path, "--from", "0", "--until", "600", "--output", output)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(read_json(output)["exclusions"])
            corrected_inventory = inventory()
            for h in corrected_inventory["hosts"]:
                h["cores"] = 48
            write_json(inv, corrected_inventory)
            result = self.run_cli("size.py", snapshot_path, "--inventory", inv, "--exclude", "0-300", "--output", output)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertAlmostEqual(read_json(output)["clusters"][0]["demand_sustained_sum"]["cpu_cores"], 7.68)
            write_json(inv, inventory())
            quiet_output = root / "quiet.json"
            result = self.run_cli("quiet.py", replay, "--exclude", "100-200", "--output", quiet_output)
            self.assertEqual(result.returncode, 0, result.stderr)
            # A completed cache resumes even when the transport is unavailable.
            self.collector.write_text(self.collector.read_text().replace("collect.MetricsHTTP=lambda *args: FakeMetrics()", "collect.MetricsHTTP=lambda *args: None"))
            result = self.run_cli("collect.py", *args[:-2], "--collect")
            self.assertEqual(result.returncode, 0, result.stderr)
            import fcntl
            with (state / "collector.lock").open("w") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                result = self.run_cli("collect.py", *args, "--collect")
                self.assertEqual(result.returncode, 2)
                self.assertIn("another collector", result.stderr)
            result = self.run_cli("collect.py", *args[:-1], "900", "--collect")
            self.assertEqual(result.returncode, 2)
            self.assertIn("another inventory/query/window", result.stderr)

    def test_fixture_cli_and_blocked_exit_code(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "sizing.json"
            result = self.run_cli("size.py", FIXTURE, "--output", output)
            self.assertEqual(result.returncode, 2)
            self.assertIn("Whisper", result.stdout)
            result = self.run_cli("size.py", FIXTURE, "--estimate-whisper", "--output", output)
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
