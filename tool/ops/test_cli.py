import argparse
import contextlib
import io
import json
import os
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import canary
import capacity
import cleanup
import expiration
import nvme_smart
import prometheus
import status


class CLITest(unittest.TestCase):
    def invoke(self, module, arguments):
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch('sys.argv', [module.__name__] + arguments), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr), patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')):
            try:
                module.main()
                code = 0
            except SystemExit as exc:
                code = exc.code
        return code, stdout.getvalue(), stderr.getvalue()

    def test_all_command_help_is_offline(self):
        for module in (prometheus, capacity, status, canary, cleanup, expiration, nvme_smart):
            with self.subTest(command=module.__name__):
                code, stdout, _ = self.invoke(module, ['--help'])
                self.assertEqual(code, 0)
                self.assertIn('usage:', stdout)

    def test_query_plans_and_invalid_range_are_offline(self):
        for report in ('health', 'migration', 'cost', 'gc', 'input-log', 'io', 'capacity'):
            with self.subTest(report=report):
                code, stdout, _ = self.invoke(prometheus, ['--report', report, '--print-queries'])
                self.assertEqual(code, 0)
                self.assertTrue(json.loads(stdout))
        code, _, _ = self.invoke(prometheus, ['--start', '2', '--end', '1', '--print-queries'])
        self.assertEqual(code, 2)
        code, stdout, _ = self.invoke(prometheus, ['--report', 'cost', '--backend', 'whisper', '--print-queries'])
        self.assertEqual(code, 0)
        self.assertIn('persister_whisper_points_total', stdout)

    def test_missing_target_visible_without_network(self):
        class API:
            def json(self, path, params):
                return {'status': 'success', 'data': {'resultType': 'vector', 'result': []}}
        with patch('prometheus.client', return_value=API()):
            code, stdout, _ = self.invoke(prometheus, ['--endpoint', 'http://metrics.example', '--target', 'absent', '--at', '100'])
        self.assertEqual(code, 0)
        report = json.loads(stdout)
        self.assertEqual(report['requested_targets'], ['absent'])
        self.assertIsNone(report['rows'][0]['cpu_cores'])

    def test_capacity_cli_rejects_missing_target_and_range_holes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'samples.json'
            report = {'requested_targets': ['a', 'b'], 'rows': [{'target': 'a', 'timestamp': 100, 'cpu_cores': 1}]}
            path.write_text(json.dumps(report))
            self.assertEqual(self.invoke(capacity, [str(path), '--cpu-cores', '8'])[0], 2)
            report['requested_targets'] = ['a']
            report['range'] = {'start': 100, 'end': 110, 'step': 10}
            path.write_text(json.dumps(report))
            self.assertEqual(self.invoke(capacity, [str(path), '--cpu-cores', '8'])[0], 2)

    def test_canary_dryrun_and_apply_only_schedule_fake_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            old, new, drop, config = [root / name for name in ('old', 'new', 'binary.conf', 'server.conf')]
            old.write_bytes(b'old')
            new.write_bytes(b'new')
            old.chmod(0o755)
            new.chmod(0o755)
            config.write_text('[common]\n')
            drop.write_text('[Service]\nExecStart=\nExecStart=%s -config %s\n' % (old, config))
            original = drop.read_bytes()
            arguments = ['run', '--binary', str(new), '--old-binary', str(old), '--sha256', canary.digest(new),
                         '--drop-in', str(drop), '--config', str(config), '--state-dir', str(root / 'state')]
            code, stdout, _ = self.invoke(canary, arguments)
            self.assertEqual(code, 0)
            self.assertFalse(json.loads(stdout)['apply'])
            self.assertEqual(drop.read_bytes(), original)
            self.assertFalse((root / 'state').exists())
            # Call start directly with an injected command runner, never systemd.
            args = argparse.Namespace(binary=str(new), old_binary=str(old), sha256=canary.digest(new),
                                      drop_in=str(drop), config=str(config), state_dir=str(root / 'state'),
                                      service='go-carbon.service', error_counter=None,
                                      metrics_endpoint='http://127.0.0.1:7007', token_file=None,
                                      duration=3, interval=1, max_errors=20, startup_timeout=2,
                                      restart_timeout=10, http_timeout=1, apply=True)
            calls = []
            with patch('canary.metrics', return_value={}), contextlib.redirect_stdout(io.StringIO()):
                canary.start(args, run=lambda argv, **kwargs: calls.append(argv))
            self.assertTrue(any(argv[0] == 'systemd-run' for argv in calls))
            self.assertEqual(drop.read_bytes(), original)
            self.assertEqual(canary.read_state(root / 'state')['status'], 'prepared')


if __name__ == '__main__':
    unittest.main()
