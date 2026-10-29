import copy
import datetime
import importlib.util
import json
import math
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('migration_control', Path(__file__).with_name('main.py'))
control = importlib.util.module_from_spec(spec)
spec.loader.exec_module(control)


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        host = {'cache': 50_000_000, 'cache_growth_per_second': 0, 'cpu_cores': 18,
                'rss_bytes': 12 << 30}
        for name in control.HISTOGRAMS:
            host[name] = {'count': 1000, 'p99_seconds': .002, 'mean_seconds': .001}
        self.baseline = {'candidate': copy.deepcopy(host), 'control': copy.deepcopy(host)}
        self.current = copy.deepcopy(self.baseline)

    def test_accelerates_only_after_two_healthy_observations(self):
        workers, healthy, _ = control.decide(self.current, self.baseline, 8, 0)
        self.assertEqual((workers, healthy), (8, 1))
        workers, healthy, _ = control.decide(self.current, self.baseline, workers, healthy)
        self.assertEqual((workers, healthy), (16, 0))

    def test_real_request_regression_halves_admission(self):
        self.current['candidate'][control.HISTOGRAMS[0]]['p99_seconds'] = .004
        workers, healthy, reasons = control.decide(self.current, self.baseline, 32, 1)
        self.assertEqual((workers, healthy), (16, 0))
        self.assertTrue(any('p99' in reason for reason in reasons))

    def test_probes_small_conversion_budgets_after_healthy_windows(self):
        workers, healthy = 0, 0
        for expected in (1, 2, 4):
            held, healthy, _ = control.decide(self.current, self.baseline, workers, healthy)
            self.assertEqual((held, healthy), (workers, 1))
            workers, healthy, _ = control.decide(self.current, self.baseline, held, healthy)
            self.assertEqual((workers, healthy), (expected, 0))

    def test_small_probes_have_no_floor_during_real_request_regression(self):
        histogram = control.HISTOGRAMS[0]
        # Captured post-fix 1202 traffic, including a genuine end-to-end gap.
        self.current['candidate'][histogram]['p99_seconds'] = .018426666666666886
        self.current['control'][histogram]['p99_seconds'] = .003982
        for workers in (0, 1, 2):
            after, healthy, reasons = control.decide(self.current, self.baseline, workers, 1)
            self.assertEqual((after, healthy), (workers // 2, 0))
            self.assertIn(histogram + ' regressed relative to unchanged replica', reasons)

    def test_small_probes_still_stop_on_storage_stalls(self):
        self.current['write_stall_active'] = True
        for workers in (0, 1):
            after, healthy, reasons = control.decide(self.current, self.baseline, workers, 1)
            self.assertEqual((after, healthy), (0, 0))
            self.assertIn('foreground storage write stalled', reasons)

    def test_backend_phase_timers_do_not_override_equal_request_latency(self):
        # Captured 1202 traffic: disk-phase p99 differs while request p99 agrees.
        request, phase = control.HISTOGRAMS
        self.current['candidate'][request]['p99_seconds'] = .00550857
        self.current['control'][request]['p99_seconds'] = .00552
        self.current['candidate'][phase]['p99_seconds'] = .02918757
        self.current['control'][phase]['p99_seconds'] = .00267010
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (16, 0))
        # The same phase figures must still reject an end-to-end regression.
        self.current['candidate'][request]['p99_seconds'] = .009
        workers, healthy, reasons = control.decide(self.current, self.baseline, 8, 1)
        self.assertEqual((workers, healthy), (4, 0))
        self.assertIn(request + ' regressed relative to unchanged replica', reasons)

    def test_shared_burst_preserves_relative_limit(self):
        for name in ['candidate', 'control']:
            self.current[name][control.HISTOGRAMS[0]]['p99_seconds'] = .008
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (16, 0))
        self.current['candidate'][control.HISTOGRAMS[0]]['p99_seconds'] = .016
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (4, 0))

    def test_missing_real_requests_does_not_accelerate(self):
        self.current['candidate'][control.HISTOGRAMS[0]]['count'] = 0
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (8, 0))

    def test_equal_current_tails_do_not_require_baseline_advantage(self):
        histogram = control.HISTOGRAMS[0]
        self.baseline['control'][histogram]['p99_seconds'] = .004
        for host in ['candidate', 'control']:
            self.current[host][histogram]['p99_seconds'] = .008
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (16, 0))

        # Discarding the initial advantage must still reject a real regression.
        self.current['candidate'][histogram]['p99_seconds'] = .016
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (4, 0))

    def test_inherited_tail_regression_does_not_grant_conversion_budget(self):
        histogram = control.HISTOGRAMS[0]
        self.baseline['candidate'][histogram]['p99_seconds'] = .012
        self.baseline['control'][histogram]['p99_seconds'] = .004
        self.current['candidate'][histogram]['p99_seconds'] = .01154
        self.current['control'][histogram]['p99_seconds'] = .00443
        workers, healthy, reasons = control.decide(self.current, self.baseline, 64, 1)
        self.assertEqual((workers, healthy), (32, 0))
        self.assertIn(histogram + ' regressed relative to unchanged replica', reasons)
        for host in ['candidate', 'control']:
            self.current[host][histogram]['p99_seconds'] = .008
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (16, 0))

    def test_backlog_uses_long_window_instead_of_sawtooth_derivative(self):
        self.current['candidate']['cache_growth_per_second'] = 1_000_000
        self.current['backlog_window'] = {
            'candidate': {'mean_cache': 150_000_000, 'cache_growth_per_second': 0},
            'control': {'mean_cache': 100_000_000, 'cache_growth_per_second': 0}}
        self.current['candidate']['cache'] = 150_000_000
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (16, 0))
        self.current['backlog_window']['candidate']['cache_growth_per_second'] = 200_000
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (4, 0))

    def test_stalls_cpu_rss_and_physical_io_reduce_admission(self):
        cases = [('write_stall_active', True), ('io_full_avg10', 6),
                 ('physical_io', {'busy_percent': 95, 'await_ms': 11})]
        for key, value in cases:
            current = copy.deepcopy(self.current); current[key] = value
            self.assertEqual(control.decide(current, self.baseline, 8, 1)[:2], (4, 0))
        for key, value in [('cpu_cores', 131), ('rss_bytes', 65 << 30)]:
            current = copy.deepcopy(self.current); current['candidate'][key] = value
            self.assertEqual(control.decide(current, self.baseline, 8, 1)[:2], (4, 0))

    def test_source_root_survives_empty_directory_reaper_and_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / 'source'; root.mkdir()
            nested = root / 'empty' / 'metric-parent'; nested.mkdir(parents=True)
            identity = control.protect_source_root(root)
            for path, _, _ in os.walk(root, topdown=False):
                try: Path(path).rmdir()
                except OSError: pass
            self.assertTrue(root.is_dir())
            self.assertFalse((root / 'empty').exists())
            self.assertEqual(control.protect_source_root(root), identity)
            marker = root / '.go-carbon-incremental-source-root.keep'
            saved = marker.read_bytes()
            replacement = Path(temporary) / 'replacement'; replacement.mkdir()
            shutil.copyfile(marker, replacement / marker.name)
            with self.assertRaisesRegex(ValueError, 'identity'):
                control.protect_source_root(replacement)
            self.assertEqual(marker.read_bytes(), saved)

    def test_source_root_guard_rejects_foreign_files_and_symlinks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); marker = root / '.go-carbon-incremental-source-root.keep'
            marker.write_text('{"foreign":true}')
            with self.assertRaisesRegex(ValueError, 'identity'):
                control.protect_source_root(root)
            self.assertEqual(marker.read_text(), '{"foreign":true}')
            marker.unlink(); target = root / 'unrelated'; target.write_text('keep')
            marker.symlink_to(target)
            with self.assertRaises(OSError): control.protect_source_root(root)
            self.assertEqual(target.read_text(), 'keep')
            marker.unlink(); os.mkfifo(marker)
            with self.assertRaisesRegex(ValueError, 'invalid'):
                control.protect_source_root(root)
            marker.unlink(); marker.mkdir()
            with self.assertRaises((OSError, ValueError)):
                control.protect_source_root(root)

    def test_operator_cap_pause_bad_limits_and_local_cron_window(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); limits = root / 'limits.json'
            cfg = {'limits_file': str(limits), 'source_directory': str(root),
                   'min_free_bytes': 0, 'quiet_local_minutes': [[175, 330]]}
            limits.write_text(json.dumps({'max_workers': 24}))
            outside = datetime.datetime(2026, 10, 22, 12)
            with patch.object(control.Path, 'iterdir', return_value=iter([])):
                self.assertEqual(control.admission_limit(cfg, outside), (24, []))
                limits.write_text(json.dumps({'max_workers': 24, 'paused': True}))
                self.assertEqual(control.admission_limit(cfg, outside)[0], 0)
                limits.write_text(json.dumps({'max_workers': 65}))
                with self.assertRaises(ValueError): control.admission_limit(cfg, outside)
            inside = datetime.datetime(2026, 10, 22, 3)
            self.assertEqual(control.admission_limit(cfg, inside)[0], 0)

    def test_counter_reset_and_restart_invalidate_window(self):
        def sample(start, cpu):
            return {'time': 1, 'cache': {'size': 0}, 'values': {
                'process_start_time_seconds': start, 'process_cpu_seconds_total': cpu}}
        first, last = sample(10, 20), sample(11, 30)
        last['time'] = 61
        with self.assertRaisesRegex(ValueError, 'generation'):
            control.summarize(first, last)
        last['values']['process_start_time_seconds'] = 10
        last['values']['process_cpu_seconds_total'] = 19
        with self.assertRaisesRegex(ValueError, 'counter reset'):
            control.summarize(first, last)

    def test_interpolation_avoids_adjacent_bucket_false_regression(self):
        candidate = control.histogram_p99({.001: 9000, .002: 9899, .004: 9999, math.inf: 10000}, 10000)
        peer = control.histogram_p99({.001: 9800, .002: 10000, math.inf: 10000}, 10000)
        self.assertAlmostEqual(candidate[0], .00202)
        self.assertEqual(candidate[1:], (.002, .004))
        self.assertAlmostEqual(peer[0], .0015)
        histogram = control.HISTOGRAMS[0]
        self.current['candidate'][histogram]['p99_seconds'] = candidate[0]
        self.current['control'][histogram]['p99_seconds'] = peer[0]
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (16, 0))
        self.current['candidate'][histogram]['p99_seconds'] = .004
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (4, 0))

    def test_interpolation_still_rejects_observed_sustained_regression(self):
        # The earlier measured 6.106 ms vs 3.790 ms remains outside 1.5x.
        histogram = control.HISTOGRAMS[0]
        self.current['candidate'][histogram]['p99_seconds'] = .006105555555556
        self.current['control'][histogram]['p99_seconds'] = .00379
        self.assertEqual(control.decide(self.current, self.baseline, 8, 1)[:2], (4, 0))

    def test_unbounded_empty_and_invalid_histograms(self):
        self.assertEqual(control.histogram_p99({}, 0), (None, None, None))
        self.assertEqual(control.histogram_p99({.001: 2}, 100), (None, None, None))
        self.assertTrue(math.isinf(control.histogram_p99({.001: 90, math.inf: 100}, 100)[0]))
        with self.assertRaisesRegex(ValueError, 'nonmonotone'):
            control.histogram_p99({.001: 80, .002: 70, math.inf: 100}, 100)


if __name__ == '__main__': unittest.main()
