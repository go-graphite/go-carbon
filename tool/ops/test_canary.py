import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from canary import guard, read_state, ready, rollback, save_state, swap_dropin


class Clock:
    def __init__(self):
        self.now = 0
    def time(self):
        return self.now
    def sleep(self, seconds):
        self.now += seconds


class CanaryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.old = self.root / 'old'
        self.new = self.root / 'new'
        self.old.write_bytes(b'previous')
        self.new.write_bytes(b'candidate')
        self.drop = self.root / 'drop.conf'
        self.original = '[Service]\nExecStart=\nExecStart=%s -config /etc/carbon.conf\n' % self.old
        self.candidate = swap_dropin(self.original, str(self.old), str(self.new))
        self.drop.write_text(self.original)
        (self.root / 'previous.conf').write_text(self.original)
        (self.root / 'candidate.conf').write_text(self.candidate)
        self.state = {'binary': str(self.new), 'old_binary': str(self.old),
                      'old_sha256': hashlib.sha256(b'previous').hexdigest(),
                      'sha256': hashlib.sha256(b'candidate').hexdigest(),
                      'drop_in': str(self.drop), 'drop_in_mode': 0o644, 'service': 'go-carbon.service',
                      'duration': 2, 'interval': 1, 'max_errors': 20, 'startup_timeout': 2,
                      'restart_timeout': 10, 'status': 'prepared'}
        save_state(self.root, self.state)
        self.calls = []

    def run_command(self, args, **kwargs):
        self.calls.append(args)
        return 'active'

    def execute_guard(self, values, health=None):
        clock = Clock()
        values = iter(values)
        return guard(self.root, run=self.run_command, sleep=clock.sleep, clock=clock.time,
                     get_metrics=lambda _: {('errors', ()): next(values)},
                     check_ready=health or (lambda state, run: None))

    def test_guard_completes_and_keeps_new_binary(self):
        self.execute_guard([0, 1, 2])
        self.assertEqual(self.drop.read_text(), self.candidate)
        self.assertEqual(read_state(self.root)['status'], 'complete')
        self.assertEqual(sum(c[:2] == ['systemctl', 'restart'] for c in self.calls), 1)

    def test_guard_error_burst_and_first_sample_restore_binary(self):
        for values in ([0, 21], [21]):
            with self.subTest(values=values):
                self.drop.write_text(self.original)
                save_state(self.root, self.state)
                with self.assertRaises(ValueError):
                    self.execute_guard(values)
                self.assertEqual(self.drop.read_text(), self.original)
                self.assertEqual(read_state(self.root)['status'], 'rolled-back')

    def test_missing_health_and_startup_failure_roll_back(self):
        def unhealthy(state, run):
            raise ValueError('missing readiness')
        with self.assertRaises(ValueError):
            self.execute_guard([], health=unhealthy)
        self.assertEqual(self.drop.read_text(), self.original)
        self.assertIn('startup timeout', read_state(self.root)['failure'])

    def test_metrics_loss_after_ready_rolls_back(self):
        clock = Clock()
        with patch('canary.metrics', side_effect=ValueError('missing metrics')):
            def metric(_):
                if clock.now:
                    raise ValueError('metrics missing')
                return {('errors', ()): 0}
            with self.assertRaises(ValueError):
                guard(self.root, run=self.run_command, sleep=clock.sleep, clock=clock.time,
                      get_metrics=metric, check_ready=lambda state, run: None)
        self.assertEqual(read_state(self.root)['status'], 'rolled-back')

    def test_rollback_preserves_external_changes(self):
        self.drop.write_text('external edit')
        with self.assertRaises(ValueError):
            rollback(self.root, self.state, self.run_command)
        self.assertEqual(self.drop.read_text(), 'external edit')
        self.assertFalse(self.calls)

    def test_refuse_changed_previous_binary(self):
        self.old.write_bytes(b'changed')
        self.drop.write_text(self.candidate)
        with self.assertRaises(ValueError):
            rollback(self.root, self.state, self.run_command)
        self.assertEqual(self.drop.read_text(), self.candidate)

    def test_dropin_rejects_wrong_binary_multiple_commands_and_expansion(self):
        for text, new in [('ExecStart=/other -config /x', '/new'),
                          ('ExecStart=/old -config /x\nExecStart=/old -config /y', '/new'),
                          ('ExecStart=/old -config /x', '/new%h')]:
            with self.assertRaises(ValueError):
                swap_dropin(text, '/old', new)

    def test_ready_requires_actual_candidate_process(self):
        def run(args):
            return 'active' if args[1] == 'is-active' else '12'
        ready(self.state, run=run, readlink=lambda _: str(self.new))
        with self.assertRaises(ValueError):
            ready(self.state, run=run, readlink=lambda _: str(self.old))


if __name__ == '__main__':
    unittest.main()
