"""Quiet-window, exclusion and target-shape memory regressions."""
import copy
import tempfile
import unittest
from unittest import mock

import collect
import quiet
import size
from common import read_json, summarize
from test_sizing import HERE, FakeMetrics, candidate, converted_snapshot, inventory
from windows import EVENT_FIELDS, activity, interval, parse_exclusions


class ExclusionTests(unittest.TestCase):
    def test_iso_epoch_short_end_and_overlap(self):
        expected = interval("2026-10-28T02:00Z-2026-10-28T10:30Z")
        self.assertEqual(interval("2026-10-28T02:00Z-10:30Z"), expected)
        self.assertEqual(interval(f"{expected[0]}-{expected[1]}"), expected)
        self.assertEqual(parse_exclusions(["100-200", "150-300", "300-400"]), [(100, 400)])
        for value in ("garbage", "300-100", "2026-10-28T02:00-2026-10-28T10:30"):
            with self.assertRaises(ValueError):
                interval(value)

    def test_third_of_day_burst_and_masked_coverage(self):
        points = {h * 3600: 1200 if 2 <= h <= 10 else 65 for h in range(25)}
        self.assertEqual(summarize(points, 0, 86400, 3600)["p95"], 1200)
        masked = summarize(points, 0, 86400, 3600, [(7200, 37800)])
        self.assertEqual(masked["p95"], 65)
        self.assertEqual(masked["coverage"], 1)
        self.assertEqual(masked["expected_samples"], 16)
        edge = summarize(points, 0, 86400, 3600, [(0, 37800)])
        self.assertEqual(edge["eligible_start"], 39600)
        self.assertIsNone(summarize(points, 0, 86400, 3600, [(0, 86401)])["peak"])

    def test_average_and_max_are_distinct_cache_entries_and_statistics(self):
        class Spiky(FakeMetrics):
            def get_json(self, path, token, **kwargs):
                rows = super().get_json(path, token, **kwargs)
                if "write_byte_per_second" in path:
                    high = "%22max%22" in path
                    for row in rows:
                        row["datapoints"] = [[2000e6 if high else 80e6, t] for t in self.times]
                return rows
        value = inventory()
        with tempfile.TemporaryDirectory() as directory:
            hosts, _, _ = list(collect.collect(value, collect.query_plan(value),
                {"start": 0, "end": 600, "step_seconds": 300}, directory, Spiky(), "fake", rps=100000))[0]
        self.assertEqual(hosts[0]["metrics"]["write_mbps"]["p95"], 80)
        self.assertEqual(hosts[0]["burst_metrics"]["write_mbps"]["peak"], 2000)
        self.assertEqual(hosts[0]["metrics"]["application_heap_live_bytes"]["p95"], 2e9)
        self.assertEqual(hosts[0]["metrics"]["non_heap_anon_bytes"]["p95"], 3.5e9)

    def test_rate_limit_uses_sleep_without_real_waiting(self):
        with mock.patch("collect.time.monotonic", return_value=100), mock.patch("collect.time.sleep") as sleep:
            limiter = collect.RateLimit(2)
            limiter.wait()
            limiter.wait()
        sleep.assert_called_once_with(.5)


class QuietTests(unittest.TestCase):
    def snapshot(self, unknown=False):
        series = {field: {} for field in EVENT_FIELDS}
        for t in range(10000, 20001, 100):
            series["process_start_time_seconds"][t] = 1000 if t < 12000 else 12000
            series["migration_in_flight"][t] = int(14000 <= t < 15000)
            series["migration_converted_rate"][t] = int(14000 <= t < 15000)
        if unknown:
            series["migration_converted_rate"].pop(18000)
        markers = activity(series, 10000, 20000, 100)
        host = {"host": "a", "cluster": "metrics", "site": "region-a", "metrics": {"store_bytes": {"peak": 1}}, "activity": markers}
        return {"window": {"start": 10000, "end": 20000}, "hosts": [host]}

    def test_longest_windows_exclude_restarts_conversion_and_prunes(self):
        value = self.snapshot()
        self.assertEqual(len(value["hosts"][0]["activity"]["unsafe"]), 2)
        result = quiet.find_windows(value, exclusions=[(16000, 17000)], minimum=500)
        self.assertEqual((result["fleet_windows"][0]["start"], result["fleet_windows"][0]["end"]), (17000, 20000))
        for window in result["fleet_windows"]:
            for a, b in ((11900, 12900), (13900, 15000), (16000, 17000)):
                self.assertFalse(window["start"] < b and window["end"] > a)

    def test_unknown_marker_or_host_never_proves_quiet(self):
        result = quiet.find_windows(self.snapshot(True), exclusions=[(16000, 17000)], minimum=500)
        self.assertFalse(any(w["start"] <= 18000 < w["end"] for w in result["fleet_windows"]))
        value = self.snapshot()
        unknown = copy.deepcopy(value["hosts"][0])
        unknown["host"] = "b"
        unknown.pop("activity")
        value["hosts"].append(unknown)
        self.assertEqual(quiet.find_windows(value)["fleet_windows"], [])

    def test_range_cannot_extend_beyond_marker_coverage(self):
        value = self.snapshot()
        value["window"]["end"] = 25000
        result = quiet.find_windows(value, start=17000, minimum=500)
        self.assertTrue(all(w["end"] <= 20000 for w in result["fleet_windows"]))


class ComponentMemoryTests(unittest.TestCase):
    def snapshot(self):
        value = converted_snapshot()
        value["hosts"] = [h for h in value["hosts"] if h["cluster"] == "metrics" and h["site"] == "region-a"]
        for host in value["hosts"]:
            host["metrics"]["working_set_bytes"].update(p95=800*size.GIB, peak=800*size.GIB, last=800*size.GIB)
            for field, gib in (("heap_live_bytes", 120), ("application_heap_live_bytes", 100),
                               ("non_heap_anon_bytes", 20), ("memtable_bytes", 4), ("active_file_bytes", 40),
                               ("slab_bytes", 10), ("block_cache_bytes", 64), ("metadata_cache_bytes", 20)):
                stats = copy.deepcopy(host["metrics"]["working_set_bytes"])
                stats.update(p95=gib*size.GIB, peak=gib*size.GIB, last=gib*size.GIB)
                host["metrics"][field] = stats
        return value

    def test_policy_commit_retained(self):
        p = size.Policy()
        self.assertEqual((p.cpu_target, p.memory_target, p.write_target, p.disk_target, p.iops_target, p.nic_target), (.7,)*6)
        self.assertEqual((p.cpu_headroom, p.memory_headroom, p.compaction_headroom), (1,)*3)
        size.Policy(cpu_target=.85).validate()

    def test_heap_slack_and_old_cache_size_are_not_target_demand(self):
        value = self.snapshot()
        shapes = read_json(HERE / "shapes.json")
        before = candidate(size.calculate(value, shapes, size.Policy(ram_mode="legacy")))
        after = candidate(size.calculate(value, shapes))
        model = after["ram_model"]
        self.assertLess(after["nodes"], before["nodes"])
        self.assertEqual(model["mode"], "components")
        self.assertEqual(model["gc_factor"], 1.5)
        self.assertEqual(after["config"]["env"]["GOGC"], "50")
        self.assertEqual(after["config"]["env"]["GOMAXPROCS"], "192")
        self.assertEqual(after["config"]["toml"]["common"]["max-cpu"], 192)
        self.assertLess(model["component_per_survivor_bytes"], model["legacy_per_survivor_bytes"])
        self.assertAlmostEqual(model["datapoints"][0]["modeled_variable_bytes"] / size.GIB, 224)
        self.assertTrue(all(v <= 1 for v in after["after_loss_fraction_of_target"].values()))
        self.assertTrue(after["config"]["memory_budget"]["metadata_is_within_go_heap"])
        higher_gc = candidate(size.calculate(value, shapes, size.Policy(go_gc_factor=3)))
        self.assertGreater(higher_gc["nodes"], after["nodes"])

    def test_per_shape_cache_and_gc_overrides_and_hit_implications(self):
        shapes = read_json(HERE / "shapes.json")
        shapes[1].update(block_cache_gib=16, metadata_cache_gib=8, go_gc_factor=1.25)
        item = candidate(size.calculate(self.snapshot(), shapes))
        self.assertEqual(item["ram_model"]["block_cache_budget_bytes"], 16*size.GIB)
        self.assertEqual(item["config"]["env"]["GOGC"], "25")
        self.assertTrue(any("IOPS/CPU/p99" in w for w in item["warnings"]))

    def test_missing_components_fall_back_without_inventing_live_heap(self):
        value = self.snapshot()
        value["hosts"][0]["metrics"].pop("heap_live_bytes")
        item = candidate(size.calculate(value, read_json(HERE / "shapes.json")))
        self.assertEqual(item["ram_model"]["mode"], "legacy-working-set")
        self.assertIsNone(item["ram_model"]["component_variable_bytes"])
        self.assertTrue(item["ram_model"]["missing"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
